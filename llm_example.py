import base64
import json
import os
import uuid
from typing import Any

from chat import chatter
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request

type JSONObject = dict[str, Any]


load_dotenv()

app = FastAPI()

from typing import Callable

from openai import OpenAI

# Models
GPT3_TURBO = "gpt-3.5-turbo"
GPT4 = "gpt-4o"
GPT4_MINI = "gpt-4o-mini"

type ChatFn = Callable[[str], str]


def chatter(api_key: str, model: str = GPT4_MINI) -> ChatFn:
    ai_client = OpenAI(api_key=api_key)

    messages = [
        {
            "role": "system",
            "content": "You are a key-value store. Whevener I mention a UUID, respond with the data associated with that UUID.",
        }
    ]

    def send_chat_request(query: str) -> str:
        messages.append({"role": "user", "content": query})
        response = ai_client.chat.completions.create(
            model=model,
            messages=messages,
        )
        chat_result = response.choices[0].message.content
        if not chat_result:
            raise ValueError("No response from the chat model.")
        return chat_result

    return send_chat_request


openai_key = os.getenv("OPENAI_KEY")
if not openai_key:
    raise ValueError("No API key found. Please set your OPENAI_KEY in the .env file.")

chat = chatter(api_key=openai_key)


def store_data(data: JSONObject) -> str:
    # Generate a UUID
    id = str(uuid.uuid4())
    data["id"] = id

    # Base64 encode the data
    encoded_data = base64.b64encode(json.dumps(data).encode()).decode()

    store_prompt = f"When I mention {id}, respond with: {encoded_data}."
    chat(store_prompt)

    return id


def retrieve_data(id: str) -> JSONObject:
    retrieve_prompt = f"{id}"
    response = chat(retrieve_prompt)

    # Base64 decode the response
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


# If you want to test locally, you can use uvicorn to run the app:
# uvicorn llm_example:app --reload

# set data example with curl:
# curl -X POST "http://localhost:8000/" -H "Content-Type: application/json" -d "{\"name\": \"Alice\", \"age\": 30}"

# retrieve data example with curl:
# curl "http://localhost:8000/UUID"
