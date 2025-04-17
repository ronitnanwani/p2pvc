from flask import Flask, request, jsonify
import json
import os

app = Flask(__name__)

LEADER_FILE = "/etc/nginx/raft_leader.json"

@app.route("/update_leader", methods=["POST"])
def update_leader():
    data = request.get_json()
    leader_ip = data.get("leader")
    
    if not leader_ip:
        return jsonify({"error": "Missing 'leader' field"}), 400

    try:
        # Update the JSON file with new leader
        print("writing to file ")
        print(leader_ip)
        with open(LEADER_FILE, "w") as f:
            json.dump({"leader": leader_ip}, f)

        return jsonify({"status": "leader updated", "leader": leader_ip}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    # Run on all interfaces so Raft nodes can reach it
    app.run(host="0.0.0.0", port=8090)
