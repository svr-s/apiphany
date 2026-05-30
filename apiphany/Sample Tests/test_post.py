import asyncio
from apiphany.orchestrator import APIOrchestrator
from pydantic import BaseModel
import httpx

config = {
    "api_config": [
        {
            "entity_name": "test",
            "api_list": [
                {
                    "api_identifier": "test_post",
                    "api_name": "Test POST",
                    "method": "POST",
                    "url": "https://jsonplaceholder.typicode.com/posts",
                    "payload": {
                        "title": "foo",
                        "body": "bar",
                        "userId": "{{user_id}}"
                    }
                }
            ],
            "client_credentials": {
                "user_id": "123"
            }
        }
    ]
}

import json
with open("test_config.json", "w") as f:
    json.dump(config, f)

async def run():
    client = APIOrchestrator(config_file="test_config.json", entity_name="test")
    res = await client.execute("test_post", output_format="raw")
    print(res)

asyncio.run(run())
