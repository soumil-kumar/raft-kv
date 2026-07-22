package httpd

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/raft-kv/store"
)

// Service provides an HTTP API for interacting with the distributed KV store.
// It handles routing, request parsing, and leader forwarding.
type Service struct {
	addr  string
	store *store.Store
	ln    net.Listener
}

// New creates a new HTTP Service.
// addr is the HTTP bind address (e.g., ":8001").
// s is the underlying distributed store.
func New(addr string, s *store.Store) *Service {
	return &Service{
		addr:  addr,
		store: s,
	}
}

// Start begins listening for HTTP requests.
// This method blocks until the server is shut down.
func (s *Service) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.addr, err)
	}
	s.ln = ln

	mux := http.NewServeMux()
	mux.HandleFunc("/store/", s.handleStore)
	mux.HandleFunc("/join", s.handleJoin)
	mux.HandleFunc("/status", s.handleStatus)

	server := &http.Server{
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	log.Printf("[httpd] listening on %s", s.addr)
	return server.Serve(s.ln)
}

// Addr returns the bound address of the HTTP server.
// Useful when binding to port 0 for tests.
func (s *Service) Addr() net.Addr {
	return s.ln.Addr()
}

// handleStore routes /store/{key} requests to the appropriate handler
// based on the HTTP method (GET, PUT, DELETE).
func (s *Service) handleStore(w http.ResponseWriter, r *http.Request) {
	// Extract the key from the URL path
	key := strings.TrimPrefix(r.URL.Path, "/store/")
	if key == "" || key == "/" {
		httpError(w, http.StatusBadRequest, "key is required: use /store/{key}")
		return
	}

	switch r.Method {
	case http.MethodGet:
		s.handleGet(w, r, key)
	case http.MethodPut:
		s.handleSet(w, r, key)
	case http.MethodDelete:
		s.handleDelete(w, r, key)
	default:
		httpError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// handleGet retrieves the value for a key.
// Supports ?consistent=true for linearizable reads.
func (s *Service) handleGet(w http.ResponseWriter, r *http.Request, key string) {
	consistent := r.URL.Query().Get("consistent") == "true"

	val, ok, err := s.store.Get(key, consistent)
	if err != nil {
		if err == store.ErrNotLeader {
			// For consistent reads, we need the leader.
			// Redirect to the leader or return an error with leader address.
			leaderAddr := s.store.LeaderAddr()
			httpError(w, http.StatusServiceUnavailable,
				fmt.Sprintf("not the leader, leader is at %s", leaderAddr))
			return
		}
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if !ok {
		httpError(w, http.StatusNotFound, fmt.Sprintf("key %q not found", key))
		return
	}

	jsonResponse(w, http.StatusOK, map[string]string{
		"key":   key,
		"value": val,
	})
}

// handleSet stores a key-value pair.
// If this node is not the leader, the request is transparently forwarded
// to the current leader — the client doesn't need to know who the leader is.
func (s *Service) handleSet(w http.ResponseWriter, r *http.Request, key string) {
	// Parse the request body
	var body struct {
		Value string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		httpError(w, http.StatusBadRequest, "invalid JSON body, expected {\"value\": \"...\"}")
		return
	}

	err := s.store.Set(key, body.Value)
	if err != nil {
		if err == store.ErrNotLeader {
			// Forward the request to the leader
			s.forwardToLeader(w, r)
			return
		}
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}

	jsonResponse(w, http.StatusOK, map[string]string{
		"status": "ok",
		"key":    key,
		"value":  body.Value,
	})
}

// handleDelete removes a key from the store.
// Like handleSet, forwards to the leader if this node is not the leader.
func (s *Service) handleDelete(w http.ResponseWriter, r *http.Request, key string) {
	err := s.store.Delete(key)
	if err != nil {
		if err == store.ErrNotLeader {
			s.forwardToLeader(w, r)
			return
		}
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}

	jsonResponse(w, http.StatusOK, map[string]string{
		"status": "ok",
		"key":    key,
	})
}

// handleJoin is unsupported in this custom static Raft implementation.
func (s *Service) handleJoin(w http.ResponseWriter, r *http.Request) {
	httpError(w, http.StatusNotImplemented, "Dynamic membership changes not supported in from-scratch Raft. Use static --peers flag.")
}

// handleStatus returns diagnostic information about this node.
func (s *Service) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		httpError(w, http.StatusMethodNotAllowed, "use GET")
		return
	}

	stats := s.store.Stats()
	status := map[string]interface{}{
		"node_id":   s.store.NodeID(),
		"is_leader": s.store.IsLeader(),
		"leader":    s.store.LeaderAddr(),
		"raft":      stats,
		"store":     s.store.GetAll(),
	}

	jsonResponse(w, http.StatusOK, status)
}

// forwardToLeader transparently proxies the request to the current Raft leader.
// This is a critical feature — it means clients can send writes to ANY node
// in the cluster and the system "just works." This is the same pattern used
// by production systems like HashiCorp Consul and etcd.
func (s *Service) forwardToLeader(w http.ResponseWriter, r *http.Request) {
	leaderAddr := s.store.LeaderAddr()
	if leaderAddr == "" {
		httpError(w, http.StatusServiceUnavailable, "no leader elected yet, try again shortly")
		return
	}

	// The leader's Raft address is in the format "host:raftPort".
	// We need to map it to the HTTP address. Convention: HTTP port = Raft port - 1000.
	// e.g., Raft :9001 → HTTP :8001
	leaderHost, leaderPort, err := net.SplitHostPort(leaderAddr)
	if err != nil {
		httpError(w, http.StatusInternalServerError, "failed to parse leader address")
		return
	}

	// Convert raft port to HTTP port (raft port - 1000)
	var raftPort int
	fmt.Sscanf(leaderPort, "%d", &raftPort)
	httpPort := raftPort - 1000

	if leaderHost == "" {
		leaderHost = "127.0.0.1"
	}

	targetURL := fmt.Sprintf("http://%s:%d%s", leaderHost, httpPort, r.URL.Path)

	log.Printf("[httpd] forwarding %s %s to leader at %s", r.Method, r.URL.Path, targetURL)

	// Create a new request to the leader
	proxyReq, err := http.NewRequest(r.Method, targetURL, r.Body)
	if err != nil {
		httpError(w, http.StatusInternalServerError, "failed to create proxy request")
		return
	}
	proxyReq.Header = r.Header

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(proxyReq)
	if err != nil {
		httpError(w, http.StatusBadGateway, fmt.Sprintf("failed to forward to leader: %v", err))
		return
	}
	defer resp.Body.Close()

	// Copy the leader's response back to the client
	for key, values := range resp.Header {
		for _, v := range values {
			w.Header().Add(key, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

// httpError sends a JSON error response.
func httpError(w http.ResponseWriter, code int, message string) {
	jsonResponse(w, code, map[string]string{"error": message})
}

// jsonResponse sends a JSON response with the given status code.
func jsonResponse(w http.ResponseWriter, code int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(data)
}
