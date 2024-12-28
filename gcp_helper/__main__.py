import json
import os

import google.auth
from dotenv import load_dotenv
from google.cloud import pubsub_v1

load_dotenv()

res_topic_name = os.environ.get("RESPONSE_TOPIC", "")


_, project_id = google.auth.default()


def callback(message):
    # we recieve bytes of a json string
    # we convert it to dict using json.loads then we print it with indentation
    print(json.dumps(json.loads(message.data.decode("utf-8")), indent=4))
    message.ack()


def wait_for_response() -> None:
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

    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        print("Stopping subscriber...")
        streaming_pull_future.cancel()
        streaming_pull_future.result()


if __name__ == "__main__":
    wait_for_response()
