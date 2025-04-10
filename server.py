#!/usr/bin/env python3
import socket
import threading
import json
import uuid
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Server configuration
SERVER_HOST = '0.0.0.0'
SERVER_PORT = 6000  # port used for signalling

# In-memory structure for meetings.
# meetings: key = meeting_id, value = set of peer IPs
meetings = {}
meetings_lock = threading.Lock()  # to protect concurrent access

peer_connections = {}  # key = client_ip, value = connection file-like object
peer_connections_lock = threading.Lock()


def create_meeting(client_ip):
    """Generate a new meeting ID and register the first peer."""
    meeting_id = str(uuid.uuid4())  # use UUID for unique meeting ID
    with meetings_lock:
        meetings[meeting_id] = set([client_ip])
    logging.info(f"Created meeting {meeting_id} with initial peer {client_ip}")
    return meeting_id

def notify_peers(peer_ips, message):
    with peer_connections_lock:
        for ip in peer_ips:
            file = peer_connections.get(ip)
            if file:
                try:
                    file.write((json.dumps(message) + '\n').encode('utf-8'))
                    file.flush()
                except Exception as e:
                    logging.error(f"Failed to notify peer {ip}: {e}")


def join_meeting(meeting_id, client_ip):
    """Add a client to an existing meeting."""
    with meetings_lock:
        if meeting_id in meetings:
            meetings[meeting_id].add(client_ip)
            logging.info(f"Peer {client_ip} joined meeting {meeting_id}")
                        # Notify other peers
            other_peers = meetings[meeting_id] - {client_ip}
            notify_peers(other_peers, {
                "status":"success",
                "type": "new_peer",
                "meeting_id": meeting_id,
                "new_peer": client_ip
            })

            return list(meetings[meeting_id])
        else:
            logging.warning(f"Join attempt for non-existent meeting {meeting_id} by {client_ip}")
            return None

def leave_meeting(meeting_id, client_ip):
    """Remove a client from a meeting. Remove the meeting if no one remains."""
    with meetings_lock:
        if meeting_id in meetings:
            meetings[meeting_id].discard(client_ip)
            if len(meetings[meeting_id]) == 0:
                del meetings[meeting_id]
                logging.info(f"Meeting {meeting_id} removed as it is empty")
            else:
                logging.info(f"Peer {client_ip} left meeting {meeting_id}")
            return True
        else:
            logging.warning(f"Leave attempt for non-existent meeting {meeting_id} by {client_ip}")
            return False

def list_meetings():
    """Return a list of active meeting IDs."""
    with meetings_lock:
        active = list(meetings.keys())
    logging.info("Listing active meetings")
    return active

def handle_client(conn, addr):
    client_ip = addr[0]
    print(client_ip)
    logging.info(f"Connected by {client_ip}")
    # Use file-like object for easier line based reading.
    file = conn.makefile(mode='rwb')
    
    with peer_connections_lock:
        peer_connections[addr[0]] = file

    try:
        # Continuously process client requests
        while True:
            raw = file.readline()
            if not raw:
                continue
            try:
                request = json.loads(raw.decode('utf-8').strip())
            except json.JSONDecodeError as e:
                logging.error("Invalid JSON from client: %s", raw)
                continue

            action = request.get("action")
            response = {"status": "error"}
            client_ip= request.get("client_ip")

            if action == "create":
                # Create a new meeting
                meeting_id = create_meeting(client_ip)
                response = {
                    "status": "success",
                    "meeting_id": meeting_id,
                    "peers": [client_ip]   # the creator is the first peer
                }
            elif action == "join":
                meeting_id = request.get("meeting_id")
                if meeting_id:
                    peers = join_meeting(meeting_id, client_ip)
                    if peers is not None:
                        response = {
                            "status": "success",
                            "meeting_id": meeting_id,
                            "peers": peers
                        }
                        
                    else:
                        response = {
                            "status": "error",
                            "message": f"Meeting ID {meeting_id} does not exist."
                        }
                else:
                    response = {
                        "status": "error",
                        "message": "Missing meeting_id for join action."
                    }
            elif action == "leave":
                meeting_id = request.get("meeting_id")
                if meeting_id:
                    success = leave_meeting(meeting_id, client_ip)
                    if success:
                        response = {
                            "status": "success",
                            "message": f"Left meeting {meeting_id}."
                        }
                    else:
                        response = {
                            "status": "error",
                            "message": f"Meeting ID {meeting_id} does not exist."
                        }
                else:
                    response = {
                        "status": "error",
                        "message": "Missing meeting_id for leave action."
                    }
            elif action == "list":
                active_meetings = list_meetings()
                response = {
                    "status": "success",
                    "meetings": active_meetings
                }
            else:
                response = {
                    "status": "error",
                    "message": "Unknown action."
                }

            # Send back the response as a JSON line.
            resp_str = json.dumps(response) + '\n'
            file.write(resp_str.encode('utf-8'))
            file.flush()
    except Exception as e:
        logging.error("Error handling client %s: %s", client_ip, e)
    finally:
        with peer_connections_lock:
            peer_connections.pop(client_ip, None)

        file.close()
        conn.close()
        logging.info("Connection closed for %s", client_ip)

def run_server():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((SERVER_HOST, SERVER_PORT))
        s.listen(5)
        logging.info(f"Signalling Server started on {SERVER_HOST}:{SERVER_PORT}")
        while True:
            conn, addr = s.accept()
            threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()

if __name__ == '__main__':
    run_server()
