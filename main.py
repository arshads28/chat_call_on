import json
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.responses import HTMLResponse
from typing import Dict, List

app = FastAPI()

# --- ROOM MANAGER ---
class Room:
    def __init__(self, name: str, limit: int = 2):
        self.name = name
        self.limit = limit
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

    # New: Send signal ONLY to other peers (not self)
    async def broadcast_signal(self, sender_name: str, signal_data: dict):
        to_remove = []
        for name, ws in self.connections.items():
            if name != sender_name: # Don't send back to self
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
        self.create_room("global", limit=1000)

    def create_room(self, name: str, limit: int):
        if name not in self.rooms:
            self.rooms[name] = Room(name, limit)
            return True
        return False

    async def join_room(self, name: str, username: str, websocket: WebSocket):
        if name not in self.rooms: return False, "Room missing."
        room = self.rooms[name]
        if room.is_full(): return False, "Room is full."
        room.connections[username] = websocket
        await room.broadcast({"type": "system", "content": f"{username} joined."}, sender_name="System")
        return True, "Joined"

    async def leave_room(self, name: str, username: str):
        if name in self.rooms and username in self.rooms[name].connections:
            del self.rooms[name].connections[username]
            await self.rooms[name].broadcast({"type": "system", "content": f"{username} left."}, sender_name="System")
            if name != "global" and len(self.rooms[name].connections) == 0:
                del self.rooms[name]

manager = RoomManager()

@app.get("/")
async def get():
    with open("index.html", "r") as f:
        return HTMLResponse(content=f.read())

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, username: str = Query(...)):
    await websocket.accept()
    current_room = "global"
    await manager.join_room("global", username, websocket)

    try:
        while True:
            data = await websocket.receive_text()
            msg_data = json.loads(data)
            action = msg_data.get("action")

            if action == "message":
                if current_room in manager.rooms:
                    await manager.rooms[current_room].broadcast({
                        "type": "chat", "content": msg_data["content"]
                    }, sender_name=username)

            elif action == "signal":
                # Handle WebRTC Signaling (Offer/Answer/Candidate)
                if current_room in manager.rooms:
                    await manager.rooms[current_room].broadcast_signal(username, msg_data["data"])

            elif action == "create_room":
                room_name = msg_data["name"]
                limit = int(msg_data.get("limit", 2))
                if manager.create_room(room_name, limit):
                    await manager.leave_room(current_room, username)
                    current_room = room_name
                    await manager.join_room(current_room, username, websocket)
                    await websocket.send_json({"type": "system", "content": f"Room '{room_name}' created."})
                else:
                    await websocket.send_json({"type": "error", "content": f"Room '{room_name}' exists."})

            elif action == "join_room":
                room_name = msg_data["name"]
                await manager.leave_room(current_room, username)
                success, response = await manager.join_room(room_name, username, websocket)
                if success:
                    current_room = room_name
                    await websocket.send_json({"type": "system", "content": f"Joined {room_name}"})
                else:
                    await manager.join_room("global", username, websocket)
                    current_room = "global"
                    await websocket.send_json({"type": "error", "content": response})

    except WebSocketDisconnect:
        await manager.leave_room(current_room, username)







        