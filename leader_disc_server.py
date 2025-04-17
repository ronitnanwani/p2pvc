
#!/usr/bin/env python3
import socket
import threading
import json
import uuid
import time
import random
import logging
from collections import defaultdict
import requests

IP1 =  '192.168.137.59'     # oms ip
IP2 = '192.168.137.9'       # ronits ip
IP3 = "10.117.166.40"       # chinmays ip
SERVER_HOST = IP2
SERVER_PORT = 6004  # Client communication port
RAFT_PORT = 6005    # RAFT RPC port

# Configuration for 3 servers (modify with your actual IPs)
SERVERS = {
    1: (IP1, RAFT_PORT),
    2: (IP2, RAFT_PORT),
    3: (IP3,RAFT_PORT)
}

meetings = {}
meetings_lock = threading.Lock()

peer_connections = {}
peer_connections_lock = threading.Lock()





class RaftNode:
    def __init__(self, node_id):
        self.node_id = node_id
        self.currentTerm = 0
        self.votedFor = None
        self.log = []
        self.commitIndex = 0
        self.lastApplied = 0
        self.state = 'follower'
        self.election_timeout = random.uniform(10.0, 15.0)
        self.heartbeat_interval = 0.5
        self.election_timer = None
        self.lock = threading.RLock()
        self.nextIndex = defaultdict(int)
        self.matchIndex = defaultdict(int)
        self.results = {}
        self.results_lock = threading.RLock()
        self.peers = [s for sid, s in SERVERS.items() if sid != node_id]
        
        print(f"[Init] Node {self.node_id} initialized as follower.")
        self.start_election_timer()
        # threading.Thread(target=self.apply_log_entries, daemon=True).start()

    def start_election_timer(self):
        if self.election_timer:
            self.election_timer.cancel()
        print("[Timer] Starting election timer...")
        self.election_timer = threading.Timer(self.election_timeout, self.start_election)
        self.election_timer.start()

    def start_election(self):
        with self.lock:
            self.currentTerm += 1
            self.state = 'candidate'
            self.votedFor = self.node_id
            votes_received = 1
            last_log_index = len(self.log)
            last_log_term = self.log[-1]['term'] if self.log else 0

            print(f"[Election] Node {self.node_id} starting election for term {self.currentTerm}")

            for peer in self.peers:
                try:
                    response = self.send_rpc(peer, {
                        "type": "RequestVote",
                        "term": self.currentTerm,
                        "candidateId": self.node_id,
                        "lastLogIndex": last_log_index,
                        "lastLogTerm": last_log_term
                    })
                    print(f"[Election] Vote response from {peer}: {response}")
                    if response and response['voteGranted']:
                        votes_received += 1
                except Exception as e:
                    print(f"[Error] RequestVote RPC failed to {peer}: {e}")

            if votes_received > len(self.peers) // 2:
                print(f"[Election] Node {self.node_id} becomes leader for term {self.currentTerm}")
                self.become_leader()
            else:
                print(f"[Election] Node {self.node_id} failed to win election.")
                self.state = 'follower'
                self.start_election_timer()
                
    def notify_load_balancers(self):
        leader_ip = SERVERS[self.node_id][0]
        print("Notifying load balancers#############################")
        lb_list = [f"http://{IP1}:8090", f"http://{IP2}:8090"]  # Replace with actual LB IPs or hostnames
        for lb in lb_list:
            try:
                requests.post(f"{lb}/update_leader", json={"leader": leader_ip}, timeout=2)
            except requests.exceptions.RequestException as e:
                print(f"[LeaderNotify] Failed to update {lb}: {e}")

    def become_leader(self):
        
        self.state = 'leader'
        self.notify_load_balancers()
        print(f"[Leader] Node {self.node_id} is now {self.state} leader.")
        for peer in self.peers:
            self.nextIndex[peer] = len(self.log) + 1
            self.matchIndex[peer] = 0
        threading.Thread(target=self.send_heartbeats, daemon=True).start()

    def send_heartbeats(self):
        while True:
            with self.lock:
                if self.state != 'leader':
                    print(f"In heartbeats {self.state}")
                    break

                for peer in self.peers:
                    while True:
                        prev_log_index = self.nextIndex[peer] - 1
                        prev_log_term = self.log[prev_log_index - 1]['term'] if prev_log_index > 0 else 0
                        entries = self.log[prev_log_index:]

                        print(f"[Heartbeat] Sending AppendEntries to {peer}")
                        response = self.send_rpc(peer, {
                            "type": "AppendEntries",
                            "term": self.currentTerm,
                            "leaderId": self.node_id,
                            "prevLogIndex": prev_log_index,
                            "prevLogTerm": prev_log_term,
                            "entries": entries,
                            "leaderCommit": self.commitIndex
                        })

                        if response is None:
                            print(f"[Heartbeat] No response from {peer}")
                            break

                        if response.get("term", 0) > self.currentTerm:
                            print(f"[Heartbeat] Higher term from {peer}, stepping down.")
                            self.currentTerm = response["term"]
                            self.state = "follower"
                            print(f"In heartbeat the node {self.node_id} is changed to follower")
                            self.votedFor = None
                            self.start_election_timer()
                            return

                        if response.get("success"):
                            self.matchIndex[peer] = prev_log_index + len(entries)
                            self.nextIndex[peer] = self.matchIndex[peer] + 1
                            break
                        else:
                            self.nextIndex[peer] = max(1, self.nextIndex[peer] - 1)

                if len(self.peers) == 0:
                    # Single-node cluster — commit all uncommitted entries
                    if self.commitIndex < len(self.log):
                        self.commitIndex = len(self.log)
                        self.apply_log_entries()
                        print("apply log entries completed")
                    time.sleep(self.heartbeat_interval)
                    print("heartbeats completed")
                    continue

               
                match_indexes = list(self.matchIndex.values()) + [len(self.log)]  # include self
                for N in range(len(self.log), self.commitIndex, -1):
                    count = sum(1 for idx in match_indexes if idx >= N)
                    if count > len(self.peers) // 2 and self.log[N - 1]["term"] == self.currentTerm:
                        self.commitIndex = N
                        print(f"[Leader] Commit index updated to {self.commitIndex}")
                        self.apply_log_entries()
                        break

            time.sleep(self.heartbeat_interval)


    def send_rpc(self, peer, message):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(1.0)
                s.connect(peer)
                s.sendall(json.dumps(message).encode('utf-8'))
                response = s.recv(4096)
                return json.loads(response.decode('utf-8'))
        except Exception as e:
            print(f"[RPC] RPC to {peer} failed: {e}")
            return None

    def handle_append_entries(self, data):
        print("waiting for lock")
        with self.lock:
            print(f"[RPC] Received AppendEntries: {data}")
            if data['term'] < self.currentTerm:
                self.start_election_timer()
                return {"term": self.currentTerm, "success": False}
            print("5")
            if data['term'] >= self.currentTerm:
                self.currentTerm = data['term']
                self.state = 'follower'
                print(f"In Handle append entries node id {self.node_id} is changed to follower")
                self.start_election_timer()
            print("4")
            if data['prevLogIndex'] > len(self.log):
                return {"term": self.currentTerm, "success": False}
            print("3")
            if data['prevLogIndex'] > 0 and self.log[data['prevLogIndex']-1]['term'] != data['prevLogTerm']:
                return {"term": self.currentTerm, "success": False}
            print("2")
            if data['entries']:
                self.log = self.log[:data['prevLogIndex']]
                self.log.extend(data['entries'])
            print('1')
            if data['leaderCommit'] > self.commitIndex:
                self.commitIndex = min(data['leaderCommit'], len(self.log))
            print("-----------------------success----------------")
            self.apply_log_entries()
            return {"term": self.currentTerm, "success": True}

    def handle_request_vote(self, data):
        with self.lock:
            print(f"[RPC] Received RequestVote: {data}")
            if data['term'] < self.currentTerm:
                return {"term": self.currentTerm, "voteGranted": False}

            if data['term'] > self.currentTerm:
                self.currentTerm = data['term']
                self.state = 'follower'

                self.votedFor = None

            last_log_index = len(self.log)
            last_log_term = self.log[-1]['term'] if self.log else 0
            log_ok = (data['lastLogTerm'] > last_log_term) or \
                    (data['lastLogTerm'] == last_log_term and 
                     data['lastLogIndex'] >= last_log_index)

            if not self.votedFor and log_ok:
                self.votedFor = data['candidateId']
                self.start_election_timer()
                return {"term": self.currentTerm, "voteGranted": True}
            
            return {"term": self.currentTerm, "voteGranted": False}

    def apply_log_entries(self):
        while True:
            # print("waiting for lock in apply_log entries")
            with self.lock:
                # print("entered")
                if self.lastApplied < self.commitIndex:
                    print(f"[Apply] Applying entries from index {self.lastApplied} to {self.commitIndex}")
                    for i in range(self.lastApplied, self.commitIndex):
                        entry = self.log[i]
                        result = self.apply_entry(entry)
                        print("apply entry completed ", result)
                        request_id = entry.get('request_id')
                        if request_id and self.state == 'leader':
                            with self.results_lock:
                                self.results[request_id] = result
                                print("got results_lock")
                    self.lastApplied = self.commitIndex
                else: break
            time.sleep(0.1)

    def apply_entry(self, entry):
        print(f"[Apply] Entry: {entry}")
        action = entry['action']
        client_ip = entry['client_ip']
        meeting_id = entry.get('meeting_id')

        if action == 'create':
            with meetings_lock:
                meetings[meeting_id] = {client_ip}
                print(f"Apply entry from index {self.lastApplied} to {self.commitIndex}")
            return {'meeting_id': meeting_id, 'peers': [client_ip]}
        elif action == 'join':
            
            with meetings_lock:
                if meeting_id in meetings:
                    meetings[meeting_id].add(client_ip)
                    peers = list(meetings[meeting_id])
                    with peer_connections_lock:
                        other_peers = [ip for ip in peers if ip != client_ip and ip in peer_connections]
                    notify_peers(other_peers, {
                        "status": "success",
                        "type": "new_peer",
                        "meeting_id": meeting_id,
                        "new_peer": client_ip
                    })
                    return {'peers': peers}
                else:
                    return {'error': 'Meeting does not exist'}
        elif action == 'leave':
            
            with meetings_lock:
                if meeting_id in meetings:
                    meetings[meeting_id].discard(client_ip)
                    other_peers = list(meetings[meeting_id])
                    # with peer_connections_lock:
                    #     other_peers = [ip for ip in peers if ip != client_ip and ip in peer_connections]
                    print("Before notifying peers functions")
                    notify_peers(other_peers, {
                        "status": "success",
                        "type": "leave_peer",
                        "meeting_id": meeting_id,
                        "leaving_peer": client_ip
                    })
                    if not meetings[meeting_id]:
                        del meetings[meeting_id]
                        return {'message': 'Meeting removed'}
                    else:
                        return {'message': 'Peer left'}
                else:
                    return {'error': 'Meeting does not exist'}
        
        return {'error': 'Unknown action'}

def notify_peers(peer_ips, message):
    print("In notifying peers")
    if(message["type"] == "new_peer"):
        with peer_connections_lock:
            for ip in peer_ips:
                file = peer_connections.get(ip)
                if file:
                    try:
                        print(f"[Notify] Notifying peer {ip}: {message}")
                        file.write((json.dumps(message) + '\n').encode('utf-8'))
                        file.flush()
                    except Exception as e:
                        print(f"[Notify Error] Failed to notify peer {ip}: {e}")
    elif(message["type"] == "leave_peer"):
        print("Notifying peers about meeting leave by ",message["leaving_peer"])
        with peer_connections_lock:
            for ip in peer_ips:
                file = peer_connections.get(ip)
                if file:
                    try:
                        print(f"[Notify] Notifying peer {ip}: {message}")
                        file.write((json.dumps(message) + '\n').encode('utf-8'))
                        file.flush()
                    except Exception as e:
                        print(f"[Notify Error] Failed to notify peer {ip}: {e}")



def handle_client(conn, addr, raft_node):
    client_ip = addr[0]
    print(f"[Client] Connected by {client_ip}")
    file = conn.makefile(mode='rwb')

    with peer_connections_lock:
        peer_connections[client_ip] = file

    try:
        while True:
            raw = file.readline()
            if not raw:
                continue
            try:
                request = json.loads(raw.decode('utf-8').strip())
            except json.JSONDecodeError:
                print("[Client] Invalid JSON received")
                continue

            print(f"[Client] Request: {request}")
            action = request.get("action")
            client_ip = request.get("client_ip", client_ip)
            response = {"status": "error"}
            # print(raft_node.votedFor)
            # print(f"server state : {raft_node.state}")
            if action in ["create", "join", "leave"]:
                if raft_node.state != 'leader':
                    response = {"status": "error", "message": "Not the leader"}
                else:
                    request_id = str(uuid.uuid4())
                    command = {
                        "action": action,
                        "client_ip": client_ip,
                        "request_id": request_id,
                        "meeting_id": request.get("meeting_id")
                    }
                    if action == "create":
                        command["meeting_id"] = str(uuid.uuid4())
                    
                    command["term"] = raft_node.currentTerm
                    raft_node.log.append(command)
                    print("appended")
                    # print(raft_node.log)
                    start_time = time.time()
                    result = None
                    while time.time() - start_time < 5:
                        with raft_node.results_lock:
                            if request_id in raft_node.results:
                                result = raft_node.results.pop(request_id)
                                break
                        time.sleep(0.1)
                    if result:
                        response = {"status": "success", "result": result}
                    else:
                        response = {"status": "error", "message": "Timeout"}
            file.write((json.dumps(response) + '\n').encode('utf-8'))
            file.flush()
    except Exception as e:
        print(f"[Client] Exception: {e}")
    finally:
        with peer_connections_lock:
            if client_ip in peer_connections:
                del peer_connections[client_ip]
        conn.close()
        print(f"[Client] Disconnected {client_ip}")


def run_server(raft_node):
    print("In run server")
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        print("before bind")
        s.bind((SERVER_HOST, SERVER_PORT))
        print("after bind")
        s.listen()
        print(f"Server started on {SERVER_HOST}:{SERVER_PORT}")
        while True:
            conn, addr = s.accept()
            threading.Thread(target=handle_client, args=(conn, addr, raft_node), daemon=True).start()


def handle_raft_rpc(conn, raft_node):
    try:
        data = conn.recv(4096)
        if data:
            message = json.loads(data.decode('utf-8'))
            response = {}
            if message['type'] == 'AppendEntries':
                response = raft_node.handle_append_entries(message)
            elif message['type'] == 'RequestVote':
                response = raft_node.handle_request_vote(message)
            
            conn.send(json.dumps(response).encode('utf-8'))
    except Exception as e:
        logging.error(f"RAFT RPC error: {e}")
    finally:
        conn.close()

if __name__ == '__main__':
    # Get node ID from command line or environment
    node_id = int(input("Enter server node ID (1-3): "))
    raft_node = RaftNode(node_id)
    
    # Start client server
    threading.Thread(target=run_server, args=(raft_node,), daemon=True).start()
    
    # Start RAFT RPC server
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((SERVER_HOST, RAFT_PORT))
        s.listen()
        logging.info(f"RAFT RPC server started on {RAFT_PORT}")
        while True:
            conn, addr = s.accept()
            threading.Thread(target=handle_raft_rpc, args=(conn, raft_node), daemon=True).start()

# this is the server code with raft implemented, In this the nodes dont know who the leader is at any instant can you implement another parameter which tells everyone who is the current node and also when a client sends any request to a node who is not the leader, the node simply rejects the request saying that it is not the leader, instead can you send the request to the leader 
