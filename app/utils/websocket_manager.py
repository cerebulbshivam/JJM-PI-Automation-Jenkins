# WebSocket connections store
connections = set()

async def broadcast_message(message: str):
    """Send a message to all connected WebSocket clients"""
    if connections:  # Use the global connections set
        for connection in list(connections):
            try:
                await connection.send_text(message)
            except Exception:
                if connection in connections:
                    connections.remove(connection)

def add_connection(websocket):
    """Add a WebSocket connection to the set"""
    connections.add(websocket)

def remove_connection(websocket):
    """Remove a WebSocket connection from the set"""
    if websocket in connections:
        connections.remove(websocket)
