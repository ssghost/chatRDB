import base64
import json
import uuid
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from redis_server import RedisServer
from typing import Callable
from langchain_ollama import Ollama

type JSONObject = dict[str, Any]
type ChatFn = Callable[[str], str]

def chatter() -> ChatFn:
    llm=Ollama(
        model = "gemma2:latest",
        base_url = "http://localhost:11434/",
        temperature = 0.9)
    
    messages = [
        {
            "role": "system",
            "content": "You are a key-value store. Whevener I mention a UUID, respond with the data associated with that UUID.",
        }
    ]

    def send_chat_request(query: str, binder: Callable) -> str:
        messages.append({"role": "user", "content": query})
        response = llm.invoke(messages).bind(binder)
        if not response:
            raise ValueError("No response from the chat model.")
        return response

    return send_chat_request

app = FastAPI()
chat = chatter()
rserver = RedisServer()
rserver.start()

def store_data(data: JSONObject) -> str:
    id = str(uuid.uuid4())
    data["id"] = id

    encoded_data = base64.b64encode(json.dumps(data).encode()).decode()
    store_prompt = f"When I mention {id}, respond with: {encoded_data}."
    chat(store_prompt, rserver.store_redis)

    return id


def retrieve_data(id: str) -> JSONObject:
    retrieve_prompt = f"{id}"
    response = chat(retrieve_prompt, rserver.retrieve_redis)

    return json.loads(base64.b64decode(response).decode())


@app.post("/", response_model=str)
async def store_endpoint(request: Request):
    data = await request.json()
    return store_data(data)
    return id


@app.get("/{id}", response_model=JSONObject)
def retrieve_endpoint(id: str) -> JSONObject:
    try:
        return retrieve_data(id)
    except KeyError:
        raise HTTPException(status_code=404, detail="Data not found")
