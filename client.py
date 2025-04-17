#!/usr/bin/env python3
import socket
import threading
import cv2
import numpy as np
import json
import time
import sys
import argparse
import struct
import sounddevice as sd
import queue
import signal
from queue import Queue
import subprocess

# my_video_queue = Queue(maxsize=1)
# remote_video_queue = Queue(maxsize=1)


# ------------------ Global Stop Event ------------------ #
stop_event = threading.Event()

# ------------------ Signal Handler ------------------ #
def signal_handler(sig, frame):
    print("\nReceived Ctrl+C. Shutting down...")
    leave_meeting()
    stop_event.set()
    # Close any OpenCV windows
    cv2.destroyAllWindows()
    # Allow some time for threads to wrap up before exiting.
    time.sleep(1)
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

# ------------------ Configuration ------------------ #
# Signalling server configuration
SIGNAL_SERVER_PORT = 6004

# UDP ports for video, audio (and note that CBCAST is now applied to these channels)
VIDEO_PORT = 5000
AUDIO_PORT = 5001

# Audio configuration
AUDIO_SAMPLERATE = 44100
AUDIO_CHANNELS = 1
AUDIO_CHUNK = 1024  # samples per block

# ------------------ Command line arguments ------------------ #
parser = argparse.ArgumentParser(description="Distributed AV Client with CBCAST for AV data")
parser.add_argument("--meeting", type=str, default="", help="Meeting ID to join (if empty, create a new meeting)")
parser.add_argument("--name", type=str, required=True, help="Your name (used as identity)")
args = parser.parse_args()

client_name = args.name
requested_meeting = args.meeting  # if empty, we create a new meeting

# ------------------ Signalling Handshake ------------------ #
def get_local_ip():
    """
    Returns the actual local network IP address (e.g., 10.xxx.xxx.xxx or 192.168.xxx.xxx)
    rather than 127.0.0.1 by opening a temporary UDP socket.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # The IP doesn't need to be reachable; we just get the local IP used for this connection.
        s.connect(("8.8.8.8", 80))
        local_ip = s.getsockname()[0]
    except Exception as e:
        print("Error determining local IP:", e)
        local_ip = "127.0.0.1"
    finally:
        s.close()
    return local_ip

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)


def signalling_handshake():
    """
    Connects to the signalling server, sends a JSON request including the client's actual IP,
    and receives a JSON response. The response includes the meeting_id and the peer IP addresses.
    """
    # Include the client's actual IP in the request:
    client_ip = get_local_ip()
    print(client_ip)
    request = {}
    request["client_ip"] = client_ip  # Report client's real IP
    if requested_meeting == "":
        request["action"] = "create"
    else:
        request["action"] = "join"
        request["meeting_id"] = requested_meeting
    result = subprocess.run(['curl', '-s', 'http://10.42.0.100:8080/leader'], stdout=subprocess.PIPE)
    # Decode the result and parse it using jq style (assuming it's a JSON response)
    output = result.stdout.decode('utf-8')

    # Use json module to parse the JSON and extract the leader IP
    data = json.loads(output)
    leader_ip = data.get('leader')
    
    print(leader_ip)
    
    if(leader_ip == 'null'):
        print("Failed signalling handshake")
        sys.exit(1)

    
    
    
    
    try:
        sock.connect((leader_ip, SIGNAL_SERVER_PORT))
    except:
        print("failed contacting signalling server")
        sys.exit(1)
    sock.sendall((json.dumps(request) + "\n").encode('utf-8'))
    response_line = sock.makefile(mode='r').readline()
    response = json.loads(response_line.strip())
    if response.get("status") != "success":
        print("Error from signalling server:", response.get("message"))
        sys.exit(1)
    return response
    


print("Contacting signalling server ...")
response = signalling_handshake()
if(requested_meeting is None):
    meeting_id = response["result"]["meeting_id"]
else:
    meeting_id = requested_meeting
peer_ips = response["result"]["peers"]
# peer_ips.append('10.145.203.100')


# peer_ips.append('10.145.203.100')
# print(f"Joined meeting {meeting_id}")
print("Peer list:", peer_ips)

video_vc = {ip: 0 for ip in peer_ips}  # includes local


    
def listen_for_new_peers():
    def handler():
        try:
            # with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            #     s.connect((SIGNAL_SERVER_HOST, SIGNAL_SERVER_PORT))
                # Now keep listening for any "new_peer" announcements
            while True:
                data = sock.recv(4096)
                if not data:
                    break
                msg = json.loads(data.decode().strip())
                if 'new_peer' in msg:
                    print("New peer connection")
                    peer_ip = msg['new_peer']
                    
                    # print(f"[INFO] New peer joined: {peer_ip}")
                    peer_ips.append(peer_ip)
                    video_vc[peer_ip] = 0
                    print("New peer added")
                    print(peer_ips)
                elif "leaving_peer" in msg:
                    peer_ip = msg['leaving_peer']
                    # print(f"[INFO] New peer joined: {peer_ip}")
                    peer_ips.remove(peer_ip)
                    del video_vc[peer_ip]
                    print("peer removed")
                    print(peer_ips)
                    cv2.destroyWindow(peer_ip)

                     
        except Exception as e:
            print("[ERROR] Listener thread:", e)

    threading.Thread(target=handler, daemon=True).start()
    
listen_for_new_peers()

# Determine our own IP from the list (fallback if necessary)
# try:
#     local_ip = [ip for ip in peer_ips if ip in socket.gethostbyname_ex(socket.gethostname())[2]][0]
# except IndexError:
#     local_ip = socket.gethostbyname(socket.gethostname())

local_ip = get_local_ip()

# Build the peers dictionary for AV streams (exclude self)
# peers = {ip: None for ip in peer_ips }

print("Local IP:", local_ip)
# print("Other peers:", list(peers.keys()))

# ------------------ CBCAST Setup for Video and Audio ------------------ #
# Create separate vector clocks, locks, and pending buffers for video and audio.
vc_lock_video = threading.Lock()
pending_buffer_video = []  # list of tuples: (sender, vc, payload)

vc_lock_audio = threading.Lock()
audio_vc = {ip: 0 for ip in peer_ips}
pending_buffer_audio = []  # list of tuples: (sender, vc, payload)
print("hello")
def can_deliver(sender, msg_vc, local_vc):
    """
    Check causal delivery conditions:
      - For the sender: local_vc[sender] + 1 == msg_vc[sender]
      - For each other process: local_vc[p] >= msg_vc[p]
    """
    # print(msg_vc)
    # print(local_vc)
    if(msg_vc[sender] - local_vc[sender] > 15):
        local_vc[sender] = msg_vc[sender]
    if local_vc[sender] + 1 < msg_vc[sender]:
        return False
    for p in local_vc:
        if p != sender and local_vc[p] < msg_vc[p]:
            return False
    return True

def check_pending_buffer_video():
    delivered_any = True
    while delivered_any:
        delivered_any = False
        with vc_lock_video:
            remaining = []
            for (sender, msg_vc, payload) in pending_buffer_video:
                if can_deliver(sender, msg_vc, video_vc):
                    # Deliver the video frame
                    frame_arr = np.frombuffer(payload, dtype=np.uint8)
                    frame = cv2.imdecode(frame_arr, 1)
                    if frame is not None:
                        pass
                        cv2.imshow(f'{sender}', frame)
                        if cv2.waitKey(1) & 0xFF == 27:
                            break
                    video_vc[sender] = msg_vc[sender]
                    delivered_any = True
                else:
                    remaining.append((sender, msg_vc, payload))
            pending_buffer_video[:] = remaining

# For audio, we store delivered blocks into a queue that an output thread will consume.
audio_queue = queue.Queue()

def check_pending_buffer_audio():
    delivered_any = True
    while delivered_any:
        delivered_any = False
        with vc_lock_audio:
            remaining = []
            for (sender, msg_vc, payload) in pending_buffer_audio:
                if can_deliver(sender, msg_vc, audio_vc):
                    audio_queue.put(payload)
                    audio_vc[sender] = msg_vc[sender]
                    delivered_any = True
                else:
                    remaining.append((sender, msg_vc, payload))
            pending_buffer_audio[:] = remaining

# ------------------ UDP Sockets ------------------ #
# Video socket
video_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
video_sock.bind(('', VIDEO_PORT))
# Audio socket
audio_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
audio_sock.bind(('', AUDIO_PORT))

# ------------------ Video: Sending and Receiving with CBCAST ------------------ #
def send_video():
    cap = cv2.VideoCapture(0)
    # cap = cv2.VideoCapture('sample_video_client.mp4')
    global video_vc
    while True:
        ret, frame = cap.read()
        if not ret:
            break
        # Encode the frame as JPEG
        ret, buffer_img = cv2.imencode('.jpg', frame, [int(cv2.IMWRITE_JPEG_QUALITY), 50])
        if not ret:
            continue
        payload = buffer_img.tobytes()
        # Increment our video vector clock before sending
        with vc_lock_video:
            video_vc[local_ip] += 1
            vc_copy = video_vc.copy()
        header = {
            "sender": local_ip,
            "vc": vc_copy
        }
        # print("inside send_video")
        # print(peers)
        header_json = json.dumps(header).encode('utf-8')
        header_len = len(header_json)
        # Packet format: [4 bytes header length][header JSON][payload]
        packet = struct.pack("!I", header_len) + header_json + payload
        # Broadcast this video frame to every peer.
        for ip in peer_ips:
            video_sock.sendto(packet, (ip, VIDEO_PORT))
            # print("sent")
        time.sleep(0.033)  # Approximately 30 fps

def receive_video():
    while True:
        try:
            print("Before recv from")
            data, addr = video_sock.recvfrom(65536)
            print("Recved from: ", addr[0])
            if len(data) < 4:
                continue
            # Extract header length.
            print("Before unpacking")
            header_len = struct.unpack("!I", data[:4])[0]
            print("Unpacking")
            header_json = data[4:4+header_len]
            header = json.loads(header_json.decode('utf-8'))
            sender = header["sender"]
            print("sender = ",sender)
            msg_vc = header["vc"]
            payload = data[4+header_len:]
            
            with vc_lock_video:
                # video_vc[local_ip] += 1
                # print(msg_vc)
                # print(video_vc)
                if can_deliver(sender, msg_vc, video_vc):
                    print("Can delivar message")
                    frame_arr = np.frombuffer(payload, dtype=np.uint8)
                    print("np.frombuffer")
                    frame = cv2.imdecode(frame_arr, 1)
                    if frame is not None:
                        print("frame is not None")
                        cv2.imshow(f'{sender}', frame)
                        print("After cv2.imshow")
                        if cv2.waitKey(1) & 0xFF == 27:
                            break
                    video_vc[sender] = max(msg_vc[sender],video_vc[sender])
                else:
                    pending_buffer_video.append((sender, msg_vc, payload))
                    print("pending")
            check_pending_buffer_video()
        except Exception as e:
            print("Error receiving video:", e)
            time.sleep(0.01)
            
            


# ------------------ Audio: Sending and Receiving with CBCAST ------------------ #
def send_audio():
    global audio_vc
    def callback(indata, frames, time_info, status):
        if status:
            print("Audio input error:", status)
        data_bytes = indata.tobytes()
        with vc_lock_audio:
            audio_vc[local_ip] += 1
            vc_copy = audio_vc.copy()
        header = {"sender": local_ip, "vc": vc_copy}
        header_json = json.dumps(header).encode('utf-8')
        header_len = len(header_json)
        packet = struct.pack("!I", header_len) + header_json + data_bytes
        for ip in peer_ips:
            audio_sock.sendto(packet, (ip, AUDIO_PORT))
    with sd.InputStream(samplerate=AUDIO_SAMPLERATE, channels=AUDIO_CHANNELS,
                        blocksize=AUDIO_CHUNK, dtype='int16', callback=callback):
        while True:
            sd.sleep(100)

def receive_audio():
    while True:
        try:
            data, addr = audio_sock.recvfrom(AUDIO_CHUNK * 2 + 1024)
            if len(data) < 4:
                continue
            header_len = struct.unpack("!I", data[:4])[0]
            header_json = data[4:4+header_len]
            header = json.loads(header_json.decode('utf-8'))
            sender = header["sender"]
            msg_vc = header["vc"]
            payload = data[4+header_len:]
            with vc_lock_audio:
                if can_deliver(sender, msg_vc, audio_vc):
                    audio_queue.put(payload)
                    audio_vc[sender] = msg_vc[sender]
                else:
                    pending_buffer_audio.append((sender, msg_vc, payload))
            check_pending_buffer_audio()
        except Exception as e:
            print("Error receiving audio:", e)
            time.sleep(0.01)
            
def leave_meeting():
    """
    Notifies the signalling server that the client is leaving the meeting.
    This implementation sends a 'leave' action along with the meeting_id.
    """
    try:
        leave_request = {
            "client_ip": get_local_ip(),
            "action": "leave",
            "meeting_id": meeting_id,  # use the meeting_id obtained from the handshake
            "name": client_name  # optionally include your identity for clean up
        }
        # You can either reuse the existing socket or open a new connection.
        # In this example, we create a new temporary connection.
        result = subprocess.run(['curl', '-s', 'http://10.42.0.100:8080/leader'], stdout=subprocess.PIPE)
        # Decode the result and parse it using jq style (assuming it's a JSON response)
        output = result.stdout.decode('utf-8')

        # Use json module to parse the JSON and extract the leader IP
        data = json.loads(output)
        leader_ip = data.get('leader')
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as leave_sock:
            try:
                leave_sock.connect((leader_ip, SIGNAL_SERVER_PORT))
                leave_sock.sendall((json.dumps(leave_request) + "\n").encode('utf-8'))
                print("Notified server about leaving the meeting.")
            except:
                print("Error leaving meeting...closing client")
                stop_event.set()
                # Close any OpenCV windows
                cv2.destroyAllWindows()
                sys.exit(1)
    except Exception as e:
        print("Error while leaving meeting:", e)

def audio_output_thread():
    """
    Consumes delivered audio blocks from audio_queue and plays them using an OutputStream.
    """
    def callback(outdata, frames, time_info, status):
        try:
            block = audio_queue.get(timeout=0.01)
            arr = np.frombuffer(block, dtype='int16').reshape((AUDIO_CHUNK, AUDIO_CHANNELS))
            outdata[:] = arr
        except queue.Empty:
            outdata[:] = np.zeros((AUDIO_CHUNK, AUDIO_CHANNELS), dtype='int16')
    with sd.OutputStream(samplerate=AUDIO_SAMPLERATE, channels=AUDIO_CHANNELS,
                         blocksize=AUDIO_CHUNK, dtype='int16', callback=callback):
        while True:
            time.sleep(0.1)

# ------------------ Thread Startup ------------------ #
threads = [
    threading.Thread(target=send_video, daemon=True),
    threading.Thread(target=receive_video, daemon=True),
    # threading.Thread(target=send_audio, daemon=True),
    # threading.Thread(target=receive_audio, daemon=True),
    # threading.Thread(target=audio_output_thread, daemon=True)
]

for t in threads:
    t.start()

# Keep main thread alive.
while True:
    time.sleep(1)