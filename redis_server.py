import subprocess
import threading
import time

import redis

from typing import Union

type RedisProcess = subprocess.Popen[bytes]
type RedisPort = redis.Redis

class RedisServer:
    def __init__(self):
        self.process
        self.port

    def start(self) -> Union[RedisProcess, RedisPort]:
        self.process = subprocess.Popen(
            ["redis-server"], stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        time.sleep(0.5)
        self.port = redis.Redis(host="localhost", port=6379, db=0)
        return self.process, self.port  

    def stop(self) -> None:
        self.process.terminate()
        self.process.wait()

    def real_time_data(self) -> None:
        self.port.set("page_views", 0)
        self.port.incr("page_views")
        self.port.incr("page_views")
        page_views = self.port.get("page_views").decode("utf-8")
        print(f"Page views: {page_views}")

    def task_queue(self) -> None:
        self.port.rpush("task_queue", "task1")
        self.port.rpush("task_queue", "task2")
        self.port.rpush("task_queue", "task3")

        task = self.port.lpop("task_queue").decode("utf-8")
        print(f"Processing task: {task}")

    def redis_set(self) -> None:
        self.port.sadd("unique_users", "user1")
        self.port.sadd("unique_users", "user2")

        unique_users = self.port.smembers("unique_users")
        print(f"Unique users: {[user.decode('utf-8') for user in unique_users]}")

    def expiring_data(self) -> None:
        self.port.set("session_token", "abc123", ex=3600) 
        session_token = self.port.get("session_token").decode("utf-8")
        print(f"Session token: {session_token} (will expire in 1 hour)")

    def pub_sub_messaging(self) -> None:
        def message_handler(message):
            print(f"Received message: {message['data'].decode('utf-8')}")

        def listen_for_messages():
            p = self.port.pubsub()
            p.subscribe("my-channel")
            while not threading.Event().is_set():
                message = p.get_message(timeout=1.0)  
                if message and message["type"] == "message":
                    message_handler(message)

        listener_thread = threading.Thread(target=listen_for_messages)
        listener_thread.start()

        time.sleep(0.5)
        self.port.publish("my-channel", "Hello, Redis!")
        time.sleep(0.5)
        threading.Event().set()
        listener_thread.join() 

    def store_redis(self):
        pass

    def retrieve_redis(self):
        pass 

def main() -> None:
    rserver = RedisServer()
    rprocess, rport = rserver.start()
    rserver.real_time_data()
    rserver.task_queue()
    rserver.redis_set()
    rserver.expiring_data()
    rserver.pub_sub_messaging()
    rserver.stop()

if __name__ == "__main__":
    main()
