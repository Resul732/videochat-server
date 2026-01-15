import asyncio
import json
import logging
from typing import Dict, Set
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# FastAPI app with CORS
app = FastAPI(title="VideoChat Signaling Server")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================================
# Data structures
# ============================================================================

class Room:
    """Represents a video chat room with up to 4 participants"""
    MAX_USERS = 4
    
    def __init__(self, room_id: str):
        self.room_id = room_id
        self.users: Dict[str, Dict] = {}  # userId -> {ws, websocket}
    
    def add_user(self, user_id: str, websocket: WebSocket) -> bool:
        """Add user to room. Returns True if added, False if room is full."""
        if len(self.users) >= self.MAX_USERS:
            return False
        
        self.users[user_id] = {
            'ws': websocket,
            'id': user_id
        }
        logger.info(f"[Room {self.room_id}] User {user_id} joined. Count: {len(self.users)}")
        return True
    
    def remove_user(self, user_id: str) -> bool:
        """Remove user from room. Returns True if removed, False if not found."""
        if user_id in self.users:
            del self.users[user_id]
            logger.info(f"[Room {self.room_id}] User {user_id} left. Count: {len(self.users)}")
            return True
        return False
    
    def get_user(self, user_id: str):
        """Get user by ID"""
        return self.users.get(user_id)
    
    def get_other_users(self, user_id: str) -> Dict[str, Dict]:
        """Get all users except the specified one"""
        return {uid: user for uid, user in self.users.items() if uid != user_id}
    
    def is_empty(self) -> bool:
        """Check if room is empty"""
        return len(self.users) == 0
    
    async def broadcast(self, message: dict, exclude_user: str = None):
        """Send message to all users in room, optionally excluding one"""
        for user_id, user_data in self.users.items():
            if exclude_user and user_id == exclude_user:
                continue
            
            try:
                await user_data['ws'].send_json(message)
            except Exception as e:
                logger.warning(f"[Room {self.room_id}] Failed to send to {user_id}: {e}")

# ============================================================================
# Global state
# ============================================================================

rooms: Dict[str, Room] = {}

# ============================================================================
# Room management
# ============================================================================

def get_or_create_room(room_id: str) -> Room:
    """Get existing room or create new one"""
    if room_id not in rooms:
        rooms[room_id] = Room(room_id)
        logger.info(f"[Rooms] Created new room: {room_id}")
    return rooms[room_id]

def remove_empty_room(room_id: str):
    """Remove room if empty"""
    if room_id in rooms and rooms[room_id].is_empty():
        del rooms[room_id]
        logger.info(f"[Rooms] Deleted empty room: {room_id}")

# ============================================================================
# WebSocket handler
# ============================================================================

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """Main WebSocket endpoint for signaling"""
    await websocket.accept()
    
    room_id = None
    user_id = None
    room = None
    
    logger.info("[WebSocket] New connection")
    
    try:
        while True:
            data = await websocket.receive_json()
            message_type = data.get('type')
            
            # ==================== JOIN ====================
            if message_type == 'join':
                room_id = data.get('roomId')
                user_id = data.get('userId')
                
                if not room_id or not user_id:
                    await websocket.send_json({
                        'type': 'error',
                        'message': 'Missing roomId or userId'
                    })
                    continue
                
                room = get_or_create_room(room_id)
                
                # Check if room is full
                if len(room.users) >= Room.MAX_USERS:
                    await websocket.send_json({
                        'type': 'room-full',
                        'message': f'Room {room_id} is full'
                    })
                    logger.warning(f"[Room {room_id}] Join rejected - full. User: {user_id}")
                    continue
                
                # Add user to room
                if not room.add_user(user_id, websocket):
                    await websocket.send_json({
                        'type': 'error',
                        'message': 'Failed to add user to room'
                    })
                    continue
                
                # Notify all other users in room
                other_users = room.get_other_users(user_id)
                
                # Send all existing users to new user
                await websocket.send_json({
                    'type': 'room-users',
                    'users': [uid for uid in other_users.keys()]
                })
                
                # Notify others that new user joined
                await room.broadcast({
                    'type': 'user-joined',
                    'userId': user_id,
                    'roomId': room_id
                }, exclude_user=user_id)
                
                logger.info(f"[Room {room_id}] User {user_id} successfully joined")
            
            # ==================== OFFER ====================
            elif message_type == 'offer':
                to_user = data.get('to')
                offer = data.get('offer')
                
                if not room or not to_user or not offer:
                    continue
                
                target_user = room.get_user(to_user)
                if target_user:
                    try:
                        await target_user['ws'].send_json({
                            'type': 'offer',
                            'from': user_id,
                            'offer': offer
                        })
                        logger.debug(f"[Room {room_id}] Offer: {user_id} -> {to_user}")
                    except Exception as e:
                        logger.warning(f"[Room {room_id}] Failed to send offer: {e}")
            
            # ==================== ANSWER ====================
            elif message_type == 'answer':
                to_user = data.get('to')
                answer = data.get('answer')
                
                if not room or not to_user or not answer:
                    continue
                
                target_user = room.get_user(to_user)
                if target_user:
                    try:
                        await target_user['ws'].send_json({
                            'type': 'answer',
                            'from': user_id,
                            'answer': answer
                        })
                        logger.debug(f"[Room {room_id}] Answer: {user_id} -> {to_user}")
                    except Exception as e:
                        logger.warning(f"[Room {room_id}] Failed to send answer: {e}")
            
            # ==================== ICE CANDIDATE ====================
            elif message_type == 'ice-candidate':
                to_user = data.get('to')
                candidate = data.get('candidate')
                
                if not room or not to_user or not candidate:
                    continue
                
                target_user = room.get_user(to_user)
                if target_user:
                    try:
                        await target_user['ws'].send_json({
                            'type': 'ice-candidate',
                            'from': user_id,
                            'candidate': candidate
                        })
                    except Exception as e:
                        logger.warning(f"[Room {room_id}] Failed to send ICE: {e}")
            
            # ==================== LEAVE ====================
            elif message_type == 'leave':
                if room and user_id:
                    await room.broadcast({
                        'type': 'user-left',
                        'userId': user_id
                    })
                break
            
            else:
                logger.warning(f"[Room {room_id}] Unknown message type: {message_type}")
    
    except WebSocketDisconnect:
        logger.info(f"[WebSocket] Disconnected - Room: {room_id}, User: {user_id}")
    
    except Exception as e:
        logger.error(f"[WebSocket] Error: {e}")
    
    finally:
        # Cleanup on disconnect
        if room and user_id:
            room.remove_user(user_id)
            remove_empty_room(room_id)
            
            # Notify other users
            if not room.is_empty():
                try:
                    await room.broadcast({
                        'type': 'user-left',
                        'userId': user_id
                    })
                except Exception as e:
                    logger.warning(f"[Room {room_id}] Failed to notify users: {e}")
        
        logger.info(f"[WebSocket] Cleanup complete - Room: {room_id}, User: {user_id}")

# ============================================================================
# Health check
# ============================================================================

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        'status': 'ok',
        'rooms': len(rooms),
        'users': sum(len(r.users) for r in rooms.values())
    }

# ============================================================================
# Info endpoint
# ============================================================================

@app.get("/info")
async def info():
    """Get server info"""
    room_info = {}
    for room_id, room in rooms.items():
        room_info[room_id] = {
            'users': len(room.users),
            'user_ids': list(room.users.keys())
        }
    
    return {
        'server': 'VideoChat Signaling Server',
        'version': '1.0.0',
        'rooms': room_info,
        'max_users_per_room': Room.MAX_USERS
    }

# ============================================================================
# Startup/Shutdown
# ============================================================================

@app.on_event("startup")
async def startup():
    logger.info("[Server] Starting VideoChat Signaling Server")

@app.on_event("shutdown")
async def shutdown():
    logger.info("[Server] Shutting down")
    # Close all WebSocket connections
    for room in rooms.values():
        for user in room.users.values():
            try:
                await user['ws'].close()
            except:
                pass

# ============================================================================
# If running as script
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    
    # Run with: python server.py
    # Or: uvicorn server:app --host 0.0.0.0 --port 8000 --reload
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info"
    )
