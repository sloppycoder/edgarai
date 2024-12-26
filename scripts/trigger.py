import json
import os
import sys

from dotenv import load_dotenv

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from gcp_helper import create_cloudevent, publish_to_pubsub  # noqa: E402

load_dotenv()

topic_name = os.environ.get("REQUEST_TOPIC", "")


def publish_message(arg):
    event = None
    if arg.startswith("idx"):
        parts = arg.split("|")
        if len(parts) == 3:
            event = create_cloudevent(
                attributes={},
                data={
                    "function": "load_master_idx",
                    "year": parts[1],
                    "quarter": parts[2],
                },
            )
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
    else:
        event = create_cloudevent(
            attributes={},
            data=json.loads(arg),
        )

    if event:
        print(event)
        publish_to_pubsub(event, topic_name)
        print("published")
    else:
        print(f"Invalid argument: {arg}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <data json string>")
        sys.exit

    publish_message(sys.argv[1])
