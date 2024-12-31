"""
config

This module provides a configuration holder for the application.

Normal usage:

    import config
    config.setv("database_id", "dev")
    config.database_id

Use a key before initializing will raise a RuntimeError

    import config
    config.database_id

    RuntimeError: Config key database_id used before being set

Use an unkown key will raise a RuntimeError:

    import config
    config.setv("unknown_key", "value")

    RuntimeError: Config key unknown_key is not allowed

```

"""

import os
from dataclasses import dataclass, fields
from typing import Any


@dataclass
class ConfigHolder:
    dataset_id: str = ""
    bucket_id: str = ""
    cache_dir: str = "cache"
    log_level: str = "DEBUG"
    req_topic: str = ""
    res_topic: str = ""

    def __init__(self):
        # use env var to override default values
        for env_var, config_key in [
            ("GCS_BUCKET_ID", "bucket_id"),
            ("EDGAR_CACHE_DIR", "cache_dir"),
            ("LOG_LEVEL", "log_level"),
            ("RESPONSE_TOPIC", "res_topic"),
            ("REQUEST_TOPIC", "req_topic"),
        ]:
            val = os.environ.get(env_var)
            if val:
                setattr(self, config_key, val)

        # use print because logger may not be setup properly
        print(f"Config: {self}")


_config_ = ConfigHolder()


def __getattr__(key: str) -> str:
    # print(f"--> Getting {key}")
    if key == "all":
        return str(_config_)

    val = getattr(_config_, key, "")
    if val:
        return val
    else:
        raise RuntimeError(f"Config key {key} used before being set")


# use a funciton because overriding __setattr__ is not allowed
def setv(key: str, value: Any) -> None:
    # print(f"--> Setting {key} to {value}")
    if key in [f.name for f in fields(_config_)]:
        setattr(_config_, key, str(value))
    else:
        raise RuntimeError(f"Config key {key} is not allowed")
