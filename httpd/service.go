// Package httpd is the client-facing HTTP API. Any node accepts any request:
// linearizable reads are served locally, and writes are forwarded to the leader.
package httpd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/soumil-kumar/raft-kv/raft"
	"github.com/soumil-kumar/raft-kv/store"
)

const (
	forwardedHeader = "X-Raft-Forwarded-By"
	maxKeyBytes     = 4 << 10
	maxScan         = 1000
)

// Options configures a Service.
type Options struct {
	Timeout      time.Duration // per operation; default 5s
	MaxBodyBytes int64         // default 1 MiB
	Storage      *raft.FileStorage
	Logger       *slog.Logger
}

// Service serves the API for one store.
type Service struct {
	store  *store.Store
	opt    Options
	client *http.Client
	server *http.Server
}

// New builds the API for s.
func New(s *store.Store, opt Options) *Service {
	if opt.Timeout == 0 {
		opt.Timeout = 5 * time.Second
	}
	if opt.MaxBodyBytes == 0 {
		opt.MaxBodyBytes = 1 << 20
	}
	if opt.Logger == nil {
		opt.Logger = slog.New(slog.DiscardHandler)
	}
	svc := &Service{
		store: s, opt: opt,
		// One pooled client: the default pool keeps only two idle connections
		// per host, which exhausts ephemeral ports under load.
		client: &http.Client{Transport: &http.Transport{
			DialContext:         (&net.Dialer{Timeout: time.Second}).DialContext,
			MaxIdleConnsPerHost: 512,
			IdleConnTimeout:     time.Minute,
		}},
	}
	svc.server = &http.Server{
		Handler:           svc.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		// Longer than Timeout, so slow operations get an error response
		// instead of a dropped connection.
		WriteTimeout: 30*time.Second + opt.Timeout,
		IdleTimeout:  2 * time.Minute,
	}
	return svc
}

// Handler returns the routes.
func (s *Service) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /store/{key...}", s.get)
	mux.HandleFunc("PUT /store/{key...}", s.put)
	mux.HandleFunc("DELETE /store/{key...}", s.delete)
	mux.HandleFunc("GET /store", s.scan)
	mux.HandleFunc("GET /status", s.status)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /readyz", s.ready)
	mux.HandleFunc("GET /metrics", s.metrics)
	mux.HandleFunc("GET /cluster/members", s.members)
	mux.HandleFunc("POST /cluster/members", s.addMember)
	mux.HandleFunc("POST /cluster/members/{id}/promote", s.promote)
	mux.HandleFunc("DELETE /cluster/members/{id}", s.removeMember)
	mux.HandleFunc("POST /cluster/transfer", s.transfer)
	return mux
}

// Serve blocks until the server is shut down.
func (s *Service) Serve(ln net.Listener) error {
	if err := s.server.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Shutdown drains in-flight requests.
func (s *Service) Shutdown(ctx context.Context) error { return s.server.Shutdown(ctx) }

// --- keys ---

type valueResponse struct {
	Key     string `json:"key"`
	Value   string `json:"value"`
	Version uint64 `json:"version"`
}

func (s *Service) key(w http.ResponseWriter, r *http.Request) (string, bool) {
	k := r.PathValue("key")
	if k == "" || len(k) > maxKeyBytes {
		fail(w, http.StatusBadRequest, "bad_request", "key must be 1 to 4096 bytes: use /store/{key}")
		return "", false
	}
	return k, true
}

// stale reports whether the client opted out of linearizability.
func stale(r *http.Request) bool { return r.URL.Query().Get("stale") == "true" }

func (s *Service) get(w http.ResponseWriter, r *http.Request) {
	key, ok := s.key(w, r)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.opt.Timeout)
	defer cancel()
	it, found, err := s.store.Get(ctx, key, stale(r))
	if err != nil {
		readError(w, err)
		return
	}
	if !found {
		fail(w, http.StatusNotFound, "not_found", fmt.Sprintf("key %q not found", key))
		return
	}
	w.Header().Set("ETag", strconv.Quote(strconv.FormatUint(it.Version, 10)))
	reply(w, http.StatusOK, valueResponse{key, it.Value, it.Version})
}

func (s *Service) scan(w http.ResponseWriter, r *http.Request) {
	limit := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > maxScan {
			fail(w, http.StatusBadRequest, "bad_request", "limit must be 1 to 1000")
			return
		}
		limit = n
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.opt.Timeout)
	defer cancel()
	kvs, err := s.store.Scan(ctx, r.URL.Query().Get("prefix"), limit, stale(r))
	if err != nil {
		readError(w, err)
		return
	}
	out := make([]valueResponse, 0, len(kvs))
	for _, kv := range kvs {
		out = append(out, valueResponse{kv.Key, kv.Value, kv.Version})
	}
	reply(w, http.StatusOK, map[string]any{"items": out})
}

// body reads the whole request body so that it can also be forwarded.
func (s *Service) body(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	b, err := io.ReadAll(http.MaxBytesReader(w, r.Body, s.opt.MaxBodyBytes))
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			fail(w, http.StatusRequestEntityTooLarge, "too_large", fmt.Sprintf("body exceeds %d bytes", tooBig.Limit))
		} else {
			fail(w, http.StatusBadRequest, "bad_request", "could not read body")
		}
		return nil, false
	}
	return b, true
}

func strictJSON(b []byte, v any) error {
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return err
	}
	if d.More() {
		return errors.New("trailing data")
	}
	return nil
}

func writeOptions(r *http.Request) (o store.WriteOptions, err error) {
	o.ClientID = r.Header.Get("X-Client-Id")
	seq := r.Header.Get("X-Sequence-Num")
	if (o.ClientID == "") != (seq == "") {
		return o, errors.New("X-Client-Id and X-Sequence-Num must be sent together")
	}
	if seq != "" {
		if o.Seq, err = strconv.ParseUint(seq, 10, 64); err != nil || o.Seq == 0 {
			return o, errors.New("X-Sequence-Num must be a positive integer")
		}
	}
	if v := r.URL.Query().Get("if_version"); v != "" {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return o, errors.New("if_version must be a non-negative integer")
		}
		o.IfVersion = &n
	}
	return o, nil
}

func (s *Service) put(w http.ResponseWriter, r *http.Request) {
	key, ok := s.key(w, r)
	if !ok {
		return
	}
	raw, ok := s.body(w, r)
	if !ok {
		return
	}
	var in struct {
		Value *string `json:"value"`
	}
	if err := strictJSON(raw, &in); err != nil || in.Value == nil {
		fail(w, http.StatusBadRequest, "bad_request", `body must be {"value": "..."}`)
		return
	}
	s.write(w, r, raw, key, func(ctx context.Context, o store.WriteOptions) (store.Result, error) {
		return s.store.Set(ctx, key, *in.Value, o)
	})
}

func (s *Service) delete(w http.ResponseWriter, r *http.Request) {
	if key, ok := s.key(w, r); ok {
		s.write(w, r, nil, key, func(ctx context.Context, o store.WriteOptions) (store.Result, error) {
			return s.store.Delete(ctx, key, o)
		})
	}
}

func (s *Service) write(w http.ResponseWriter, r *http.Request, raw []byte, key string,
	do func(context.Context, store.WriteOptions) (store.Result, error)) {
	o, err := writeOptions(r)
	if err != nil {
		fail(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.opt.Timeout)
	defer cancel()
	res, err := do(ctx, o)
	switch {
	case err == nil:
		reply(w, http.StatusOK, map[string]any{
			"status": "ok", "key": key, "version": res.Version, "existed": res.Existed,
		})
	case errors.Is(err, store.ErrVersionMismatch):
		reply(w, http.StatusPreconditionFailed, map[string]any{
			"code": "version_mismatch", "error": err.Error(), "version": res.Version,
		})
	case errors.Is(err, store.ErrStaleSequence):
		fail(w, http.StatusConflict, "stale_sequence", err.Error())
	case errors.Is(err, raft.ErrNotLeader):
		s.forward(ctx, w, r, raw)
	default:
		writeError(w, err)
	}
}

// --- forwarding ---

func (s *Service) forward(ctx context.Context, w http.ResponseWriter, r *http.Request, body []byte) {
	if r.Header.Get(forwardedHeader) != "" {
		unavailable(w, "not_leader", "leadership moved while forwarding; retry")
		return
	}
	leader, ok := s.store.Raft().Leader()
	if !ok || leader.HTTPAddr == "" {
		unavailable(w, "no_leader", "no leader is known; retry")
		return
	}
	req, err := http.NewRequestWithContext(ctx, r.Method, "http://"+leader.HTTPAddr+r.URL.RequestURI(), bytes.NewReader(body))
	if err != nil {
		fail(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	for _, h := range []string{"Content-Type", "X-Client-Id", "X-Sequence-Num"} {
		if v := r.Header.Get(h); v != "" {
			req.Header.Set(h, v)
		}
	}
	req.Header.Set(forwardedHeader, s.store.Raft().Status().ID)

	resp, err := s.client.Do(req)
	if err != nil {
		var op *net.OpError
		if errors.As(err, &op) && op.Op == "dial" {
			unavailable(w, "no_leader", "leader is unreachable; retry") // nothing was sent
			return
		}
		unknown(w, "lost contact with the leader")
		return
	}
	defer resp.Body.Close()
	for _, h := range []string{"Content-Type", "Retry-After"} {
		if v := resp.Header.Get(h); v != "" {
			w.Header().Set(h, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

// --- cluster ---

func (s *Service) members(w http.ResponseWriter, _ *http.Request) {
	st := s.store.Raft().Status()
	reply(w, http.StatusOK, map[string]any{"leader": st.LeaderID, "members": st.Members})
}

func (s *Service) admin(w http.ResponseWriter, r *http.Request, body []byte, do func(context.Context) error) {
	ctx, cancel := context.WithTimeout(r.Context(), s.opt.Timeout)
	defer cancel()
	err := do(ctx)
	switch {
	case err == nil:
		reply(w, http.StatusOK, map[string]string{"status": "ok"})
	case errors.Is(err, raft.ErrNotLeader):
		s.forward(ctx, w, r, body)
	case errors.Is(err, raft.ErrConfigPending):
		unavailable(w, "config_pending", err.Error())
	case errors.Is(err, raft.ErrLeadershipLost), errors.Is(err, raft.ErrOverloaded), errors.Is(err, raft.ErrTransferring),
		errors.Is(err, raft.ErrStopped), errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		writeError(w, err)
	default:
		fail(w, http.StatusConflict, "rejected", err.Error())
	}
}

func (s *Service) addMember(w http.ResponseWriter, r *http.Request) {
	raw, ok := s.body(w, r)
	if !ok {
		return
	}
	var m raft.Member
	if err := strictJSON(raw, &m); err != nil || m.ID == "" || m.RaftAddr == "" || m.HTTPAddr == "" {
		fail(w, http.StatusBadRequest, "bad_request", `body must be {"id","raft_addr","http_addr"}`)
		return
	}
	s.admin(w, r, raw, func(ctx context.Context) error { return s.store.Raft().AddLearner(ctx, m) })
}

func (s *Service) promote(w http.ResponseWriter, r *http.Request) {
	s.admin(w, r, nil, func(ctx context.Context) error { return s.store.Raft().Promote(ctx, r.PathValue("id")) })
}

func (s *Service) removeMember(w http.ResponseWriter, r *http.Request) {
	s.admin(w, r, nil, func(ctx context.Context) error { return s.store.Raft().RemoveMember(ctx, r.PathValue("id")) })
}

func (s *Service) transfer(w http.ResponseWriter, r *http.Request) {
	s.admin(w, r, nil, func(ctx context.Context) error {
		return s.store.Raft().TransferLeadership(ctx, r.URL.Query().Get("target"))
	})
}

// --- observability ---

func (s *Service) status(w http.ResponseWriter, _ *http.Request) {
	st := s.store.Raft().Status()
	reply(w, http.StatusOK, struct {
		raft.Status
		IsLeader bool `json:"is_leader"`
		Keys     int  `json:"keys"`
	}{st, st.Role == raft.Leader.String(), s.store.Len()})
}

// ready succeeds only if this node can serve a linearizable read right now.
func (s *Service) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	if err := s.store.Raft().LinearizableRead(ctx); err != nil {
		readError(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Service) metrics(w http.ResponseWriter, _ *http.Request) {
	st := s.store.Raft().Status()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	metric := func(kind, name, help string, v uint64) {
		fmt.Fprintf(w, "# HELP raftkv_%s %s\n# TYPE raftkv_%s %s\nraftkv_%s %d\n", name, help, name, kind, name, v)
	}
	var leader uint64
	if st.Role == raft.Leader.String() {
		leader = 1
	}
	metric("gauge", "is_leader", "1 if this node is the leader", leader)
	metric("gauge", "term", "current Raft term", st.Term)
	metric("gauge", "commit_index", "highest committed log index", st.CommitIndex)
	metric("gauge", "last_applied", "highest applied log index", st.LastApplied)
	metric("gauge", "last_index", "highest log index", st.LastIndex)
	metric("gauge", "durable_index", "highest fsynced log index", st.DurableIndex)
	metric("gauge", "snapshot_index", "log index covered by the snapshot", st.SnapshotIndex)
	metric("gauge", "keys", "number of keys", uint64(s.store.Len()))
	metric("counter", "elections_started_total", "elections started by this node", st.Elections)
	metric("counter", "snapshots_taken_total", "snapshots taken", st.Snapshots)
	metric("counter", "snapshots_installed_total", "snapshots received from a leader", st.Installed)
	if fs := s.opt.Storage; fs != nil {
		metric("counter", "wal_fsyncs_total", "WAL fsync calls", fs.SyncCount.Load())
		metric("counter", "wal_entries_total", "log entries written to the WAL", fs.SavedEntries.Load())
	}
}

// --- responses ---

func reply(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func fail(w http.ResponseWriter, status int, code, msg string) {
	reply(w, status, map[string]string{"code": code, "error": msg})
}

// unavailable means the request had no effect and may be retried as is.
func unavailable(w http.ResponseWriter, code, msg string) {
	w.Header().Set("Retry-After", "1")
	fail(w, http.StatusServiceUnavailable, code, msg)
}

// unknown means a write may or may not have taken effect. Clients that need
// exactly-once must retry with the same X-Client-Id and X-Sequence-Num.
func unknown(w http.ResponseWriter, msg string) {
	fail(w, http.StatusGatewayTimeout, "unknown_outcome", msg)
}

func readError(w http.ResponseWriter, err error) {
	unavailable(w, "unavailable", err.Error())
}

func writeError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, raft.ErrOverloaded), errors.Is(err, raft.ErrTransferring), errors.Is(err, raft.ErrStopped):
		unavailable(w, "unavailable", err.Error())
	case errors.Is(err, raft.ErrLeadershipLost), errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		unknown(w, err.Error())
	default:
		fail(w, http.StatusInternalServerError, "internal", err.Error())
	}
}
