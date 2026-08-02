# The Ultimate Testing Playbook

To verify everything is working, open 4 terminals. In Terminals 1, 2, and 3, start your cluster:

```bash
# Terminal 1
go run . --node-id=node1 --http-addr=:8001 --raft-addr=:9001 --peers=:9001,:9002,:9003

# Terminal 2
go run . --node-id=node2 --http-addr=:8002 --raft-addr=:9002 --peers=:9001,:9002,:9003

# Terminal 3
go run . --node-id=node3 --http-addr=:8003 --raft-addr=:9003 --peers=:9001,:9002,:9003
```

Use Terminal 4 to run these test cases:

## Test Case 1: The Happy Path (Basic Replication)
* **Goal**: Verify that writing to the cluster replicates data to all nodes.
* **Action**: Check the logs to see who the Leader is (e.g., Node 1). Send a write request to it:
  ```bash
  curl -X PUT http://localhost:8001/store/company -d '{"value": "Amazon"}'
  ```
* **Verify**: Read the data from a *different* node (e.g., Node 2).
  ```bash
  curl http://localhost:8002/store/company
  ```
  *Result:* It should return `Amazon`. The data was successfully replicated.

## Test Case 2: Transparent Leader Forwarding
* **Goal**: Prove that your API abstracts cluster topology away from the client.
* **Action**: Send a write request to a node that you *know* is a Follower (e.g., Node 3).
  ```bash
  curl -X PUT http://localhost:8003/store/role -d '{"value": "SDE"}'
  ```
* **Verify**: Look at Node 3's terminal. You won't see a commit log. Look at the Leader's terminal—you will see the commit happen there. The client still gets a `200 OK` response because Node 3 proxied the HTTP request perfectly.

## Test Case 3: Fault Tolerance (Leader Crash & Re-election)
* **Goal**: Prove that the cluster survives the death of its Leader (High Availability).
* **Action**: 
  1. Go to the Leader's terminal and press `Ctrl+C` to kill it.
  2. Watch the logs of the two surviving nodes. Within ~300ms, their heartbeat timeouts will expire. One will start an election and become the new Leader.
* **Verify**: Send a write request to the new Leader (or any surviving node).
  ```bash
  curl -X PUT http://localhost:8002/store/status -d '{"value": "survived"}'
  ```
  *Result:* The cluster continues to accept writes because 2 out of 3 nodes are alive (a quorum exists).

## Test Case 4: Minority Partition (No Quorum)
* **Goal**: Prove that the system prioritizes **Consistency** over **Availability** (CAP Theorem).
* **Action**: 
  1. Kill another node, leaving only **one** node alive (e.g., Node 2).
  2. The remaining node only has 1 vote (itself). A majority of 3 is 2. 
  3. Try to write to it:
  ```bash
  curl -X PUT http://localhost:8002/store/split -d '{"value": "brain"}'
  ```
* **Verify**: The HTTP request will hang or fail. Why? Because the Leader appends the entry to its log but cannot get an acknowledgment from a majority. It refuses to commit the data, preventing a "split-brain" scenario. 

## Test Case 5: Crash Recovery (Disk Persistence)
* **Goal**: Prove that Raft state is durable.
* **Action**: 
  1. Kill all nodes (Ctrl+C on everything). 
  2. Verify that `raft-data-node1.json`, `raft-data-node2.json`, etc., exist in your folder.
  3. Restart all 3 nodes.
* **Verify**: Read the data you wrote before the crash:
  ```bash
  curl http://localhost:8001/store/company
  ```
  *Result:* It should immediately return `Amazon`. The nodes read their JSON files on startup, rebuilt the in-memory log, and restored the KV store.

## Test Case 6: Stale vs. Consistent Reads
* **Goal**: Demonstrate read safety.
* **Action**: Read from a Follower using `?consistent=true`.
  ```bash
  curl "http://localhost:8003/store/company?consistent=true"
  ```
* **Verify**: The request will fail with an error `not the leader, leader is at...`. This proves that your system forces clients to read from the Leader if they demand strict consistency (guaranteeing they don't read stale data from a disconnected follower).
