import asyncio
import websockets
import json

async def test_ws():
    session_id = "test-session-123"
    
    # 1. User connects
    user_ws_url = f"ws://127.0.0.1:8000/api/human/chat/{session_id}?role=user&token=fake"
    counselor_ws_url = f"ws://127.0.0.1:8000/api/human/chat/{session_id}?role=human_counselor&token=fake"
    
    async with websockets.connect(user_ws_url) as user_ws:
        print("User connected")
        
        async with websockets.connect(counselor_ws_url) as counselor_ws:
            print("Counselor connected")
            
            # Flush initial messages
            try:
                while True:
                    msg = await asyncio.wait_for(user_ws.recv(), timeout=0.5)
                    print("User received initial:", msg)
            except asyncio.TimeoutError:
                pass
                
            try:
                while True:
                    msg = await asyncio.wait_for(counselor_ws.recv(), timeout=0.5)
                    print("Counselor received initial:", msg)
            except asyncio.TimeoutError:
                pass
            
            # Counselor sends message
            print("Counselor sending message...")
            await counselor_ws.send(json.dumps({"text": "Hello from counselor"}))
            
            # User receives message
            try:
                msg = await asyncio.wait_for(user_ws.recv(), timeout=2.0)
                print("User received:", msg)
            except asyncio.TimeoutError:
                print("User DID NOT receive message")
                
            # User sends message
            print("User sending message...")
            await user_ws.send(json.dumps({"text": "Hello from user"}))
            
            # Counselor receives message
            try:
                msg = await asyncio.wait_for(counselor_ws.recv(), timeout=2.0)
                print("Counselor received:", msg)
            except asyncio.TimeoutError:
                print("Counselor DID NOT receive message")

asyncio.run(test_ws())
