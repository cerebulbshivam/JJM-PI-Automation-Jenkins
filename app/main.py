from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from app.routers import upload

# FastAPI app
app = FastAPI()

# Mount static files
app.mount("/static", StaticFiles(directory="app/static"), name="static")

# Templates
templates = Jinja2Templates(directory="app/templates")

# -------------------------
# WebSocket Manager
# -------------------------
# class ConnectionManager:
#     def __init__(self):
#         self.active_connections: list[WebSocket] = []

#     async def connect(self, websocket: WebSocket):
#         await websocket.accept()
#         self.active_connections.append(websocket)

#     def disconnect(self, websocket: WebSocket):
#         if websocket in self.active_connections:
#             self.active_connections.remove(websocket)

#     async def broadcast(self, message: str):
#         for connection in self.active_connections:
#             try:
#                 await connection.send_text(message)
#             except:
#                 self.disconnect(connection)


# Manager instance
# manager = ConnectionManager()

# Helper function for sending logs
# async def send_log(message: str):
#     """Send a log message to all connected WebSocket clients"""
#     await manager.broadcast(message)


# -------------------------
# WebSocket Endpoint
# -------------------------
# @app.websocket("/ws/logs")
# async def websocket_logs(websocket: WebSocket):
#     await manager.connect(websocket)
#     try:
#         while True:
#             # Just keep the connection alive
#             await websocket.receive_text()
#     except WebSocketDisconnect:
#         manager.disconnect(websocket)
#     except Exception:
#         manager.disconnect(websocket)


# -------------------------
# Routes
# -------------------------
app.include_router(upload.router)

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("upload.html", {"request": request})


# Example endpoint that triggers a WebSocket log message
# @app.get("/do-task")
# async def do_task():
#     # await send_log("✅ Task started")
#     # await send_log("⚡ Processing step 1")
#     # await send_log("⚡ Processing step 2")
#     # await send_log("🎉 Task completed successfully")
#     return {"message": "done"}
