import json
import time
from datetime import datetime
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query,Header
from fastapi.responses import HTMLResponse
from typing import Dict
from collections import defaultdict, deque

app = FastAPI()



class RateLimiter:
    def __init__(self, max_messages: int = 10, period_seconds: int = 10):
        self.max_messages = max_messages
        self.period_seconds = period_seconds
        # Dictionary: Key = username, Value = List of timestamps
        self.user_history = defaultdict(deque)

    def is_allowed(self, username: str) -> bool:
        current_time = time.time()
        user_timestamps = self.user_history[username]

        #  Remove timestamps that are too old (expired)
        while user_timestamps and user_timestamps[0] < current_time - self.period_seconds:
            user_timestamps.popleft()

        #  Check if they have space left
        if len(user_timestamps) < self.max_messages:
            user_timestamps.append(current_time)
            return True
        else:
            return False

limiter = RateLimiter(max_messages=10, period_seconds=10) # 10 messages per 10 seconds


# --- ROOM MANAGER ---
class Room:
    def __init__(self, name: str, limit: int = 2, password: str = ""):
        self.name = name
        self.limit = limit
        self.password = password
        self.connections: Dict[str, WebSocket] = {}

    def is_full(self):
        return len(self.connections) >= self.limit

    async def broadcast(self, message: dict, sender_name: str = "System"):
        message["sender"] = sender_name
        to_remove = []
        for name, ws in self.connections.items():
            try:
                await ws.send_text(json.dumps(message))
            except:
                to_remove.append(name)
        for name in to_remove:
            del self.connections[name]

    async def broadcast_signal(self, sender_name: str, signal_data: dict):
        # Broadcast signals (invites, webrtc) to others in the room
        to_remove = []
        for name, ws in self.connections.items():
            if name != sender_name:
                try:
                    await ws.send_text(json.dumps({
                        "type": "signal",
                        "sender": sender_name,
                        "data": signal_data
                    }))
                except:
                    to_remove.append(name)
        for name in to_remove:
            del self.connections[name]

class RoomManager:
    def __init__(self):
        self.rooms: Dict[str, Room] = {}
        self.create_room("global", limit=1000, password="")

    def create_room(self, name: str, limit: int, password: str = ""):
        if name not in self.rooms:
            self.rooms[name] = Room(name, limit, password)
            return True
        return False

    async def join_room(self, name: str, username: str, websocket: WebSocket, password: str = ""):
        if name not in self.rooms: 
            return False, "Room missing."
        
        room = self.rooms[name]
        
        if room.password and room.password != password:
            return False, "Incorrect Password."
        
        if room.is_full(): 
            return False, "Room is full."
        
        room.connections[username] = websocket
        
        # Notify others
        await room.broadcast({"type": "user_joined", "username": username, "content": f"{username} joined."}, sender_name="System")
        
        # Tell the user they joined
        await websocket.send_json({"type": "room_joined", "name": name})
        
        return True, "Joined"

    async def leave_room(self, name: str, username: str):
        if name in self.rooms and username in self.rooms[name].connections:
            del self.rooms[name].connections[username]
            await self.rooms[name].broadcast({"type": "system", "content": f"{username} left."}, sender_name="System")
            # Auto-delete empty rooms (except global)
            if name != "global" and len(self.rooms[name].connections) == 0:
                del self.rooms[name]

manager = RoomManager()

@app.get("/")
async def get():
    with open("index.html", "r") as f:
        return HTMLResponse(content=f.read())

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, username: str = Query(...),user_agent: str | None = Header(default=None)):
    await websocket.accept()

    # SECURITY LOGGING START
    client_host = websocket.client.host
    client_port = websocket.client.port
    login_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    print(f"\n🔒 [SECURITY LOG] New Connection Accepted")
    print(f"   👤 User: {username}")
    print(f"   🌍 IP Address: {client_host}")
    print(f"   📱 Device: {user_agent}")
    print(f"   🚪 Port: {client_port}")
    print(f"   ⏰ Time: {login_time}")
    print("-" * 40)
    # SECURITY LOGGING END 

    current_room = "global"
    await manager.join_room("global", username, websocket)

    try:
        while True:
            data = await websocket.receive_text()
            msg_data = json.loads(data)
            action = msg_data.get("action")

            if action == "message":

                # RATE LIMIT CHECK START ---
                if not limiter.is_allowed(username):
                    # If spamming, send error ONLY to this user
                    await websocket.send_json({
                        "type": "error", 
                        "content": "⚠️ Slow down! You are sending messages too fast."
                    })
                    print(f"🚫 [BLOCKED] {username} is spamming.")
                    continue

                msg_id = msg_data.get("id")
                time_str = datetime.now().strftime("%I:%M %p") 
                if current_room in manager.rooms:
                    await manager.rooms[current_room].broadcast({
                        "type": "chat", 
                        "content": msg_data["content"],
                        "id": msg_id,
                        "time": time_str  
                    }, sender_name=username)

            elif action in ["status_delivered", "status_read"]:
                status_type = "delivered" if action == "status_delivered" else "read"
                if current_room in manager.rooms:
                    await manager.rooms[current_room].broadcast({
                        "type": "status_update",
                        "msg_id": msg_data["id"],
                        "status": status_type,
                        "who": username
                    }, sender_name=username)

            elif action == "signal":
                if current_room in manager.rooms:
                    await manager.rooms[current_room].broadcast_signal(username, msg_data["data"])

            elif action == "create_room":
                room_name = msg_data["name"]
                limit = int(msg_data.get("limit", 2))
                password = msg_data.get("password", "")

                if manager.create_room(room_name, limit, password):
                    await manager.leave_room(current_room, username)
                    current_room = room_name
                    await manager.join_room(current_room, username, websocket, password)
                else:
                    await websocket.send_json({"type": "error", "content": f"Room '{room_name}' exists."})

            elif action == "join_room":
                room_name = msg_data["name"]
                password = msg_data.get("password", "")

                await manager.leave_room(current_room, username)
                success, response = await manager.join_room(room_name, username, websocket, password)
                
                if success:
                    current_room = room_name
                else:
                    await manager.join_room("global", username, websocket)
                    current_room = "global"
                    await websocket.send_json({"type": "error", "content": response})

    except WebSocketDisconnect:
        await manager.leave_room(current_room, username)