import json
import os
import sys
import time

import google.auth
from dotenv import load_dotenv
from google.cloud import pubsub_v1

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from gcp_helper import create_cloudevent, publish_to_pubsub  # noqa: E402

load_dotenv()

res_topic_name = os.environ.get("RESPONSE_TOPIC", "")


_, project_id = google.auth.default()


received_message = None


def callback(message):
    global received_message
    received_message = message
    message.ack()


def wait_for_response(req_id: str, timeout_seconds: int = 90) -> None:
    global received_message
    received_message = None

    topic_name = os.environ.get("RESPONSE_TOPIC", "edgarai-response")
    if not topic_name:
        raise ValueError("RESPONSE_TOPIC environment variable is not set")

    subscription_name = (
        f"{topic_name}-sub"  # default subscription when creating the topic
    )

    # Subscribe to the topic
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_name)
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}...")

    start_time = time.time()
    while time.time() - start_time < timeout_seconds:
        time.sleep(0.2)
        if received_message:
            payload = json.loads(received_message.data.decode("utf-8"))
            print(payload)
            if payload.get("req-id") == req_id:
                print("good")
                streaming_pull_future.cancel()
                return
            else:
                received_message = None

    print("deadline exceeded")
    streaming_pull_future.cancel()


def publish_message(arg: str, wait: bool = False):
    topic_name = os.environ.get("REQUEST_TOPIC")
    if not topic_name:
        raise ValueError("REQUEST_TOPIC environment variable is not set")

    event = None
    if arg.startswith("idx"):
        parts = arg.split("|")
        year = parts[1]
        if year <= 2000 or year >= 2028:
            raise ValueError(f"Invalid year: {year}")

        quarters = [parts[2]] if len(parts[2]) == 3 else [1, 2, 3, 4]

        for qtr in quarters:
            event = create_cloudevent(
                attributes={},
                data={
                    "function": "load_master_idx",
                    "year": year,
                    "quarter": qtr,
                },
            )
            req_id = publish_to_pubsub(event, topic_name)
            print(f"published {req_id}")
            if wait:
                wait_for_response(req_id)

    elif arg.startswith("chunk"):
        parts = arg.split("|")
        if len(parts) == 2:
            event = create_cloudevent(
                attributes={},
                data={
                    "function": "chunk_one_filing",
                    "filename": parts[1],
                },
            )

            req_id = publish_to_pubsub(event, topic_name)
            print(f"published {req_id}")
            if wait:
                wait_for_response(req_id)
    else:
        print(f"Invalid argument: {arg}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <data json string>")
        sys.exit

    wait_flag = len(sys.argv) >= 3 and sys.argv[2] == "-w"

    publish_message(sys.argv[1], wait=wait_flag)
