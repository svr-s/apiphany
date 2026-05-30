# Apiphany

Apiphany is an API orchestration engine. It transforms the chaotic process of extracting data from complex REST and GraphQL APIs into a clean, strictly-typed, and fully declarative workflow. Powered by `httpx` and `asyncio`, Apiphany tears through deeply chained API requests concurrently without ever triggering rate limits, handles massive multi-page payloads automatically, and seamlessly unpacks deeply nested JSON blobs into pristine Pandas DataFrames or SQL tables.

## Features
- **Asynchronous Concurrency**: Built entirely on `httpx` and `asyncio`, the engine fires hundreds of concurrent requests simultaneously with minimal memory overhead.
- **Strict Rate Limiting**: Built-in `aiolimiter` safely paces outgoing requests to ensure you never exceed API quotas, strictly adhering to your configured `requests_per_second`.
- **Chained Requests**: Feed the extracted outputs of one API call as parameters into another dynamically (e.g., fetch Users -> Posts -> Comments).
- **Deep Extraction**: Native integration with `json_extract_pandas` to extract, unpack, and normalize nested JSON responses into clean DataFrames.
- **Incremental State Tracking**: Native `state.json` watermarking. Works locally or via S3 (`s3://bucket/state.json`) to persist the latest timestamps or IDs fetched.
- **Secrets Management**: Dynamically fetch client credentials or API keys directly from AWS Secrets Manager using ARNs.
- **Robust Failure Handling**: Automatic exponential backoff for server errors (`500`, `502`, `503`, `504`).
- **Cloud-Agnostic Logging**: Emits pure JSON logs using `python-json-logger` natively compatible with AWS CloudWatch and Datadog.

## Requirements
- Python 3.8+
- `httpx`
- `aiolimiter`
- `pydantic`
- `python-json-logger`

*(Optional based on cloud providers)*
- `boto3` (for S3 exports, state management, and AWS Secrets Manager)
- `pandas` & `sqlalchemy` (for SQL database exports)

---

## Installation & Usage

Install the core package:
```bash
pip install apiphany
```

To install with AWS capabilities (S3 exports and AWS Secrets Manager) or SQL export capabilities:
```bash
pip install apiphany[aws]
pip install apiphany[sql]
```

### Python API
Import `APIOrchestrator` directly into your data pipeline, Lambda function, or test suite:

```python
import asyncio
from apiphany import APIOrchestrator

# 1. Initialize the Client
client = APIOrchestrator(
    config_file="apiphany_config.json", 
    entity_name="jsonplaceholder"
)

# 2. Execute an API asynchronously!
# output_format can be "raw" (list of JSON dicts) or "flattened" (Pandas DataFrame)
df = asyncio.run(client.execute(
    api_identifier="get_users", 
    output_format="flattened"
))

print(df.head())
```

---

## Configuration Schema Details

Apiphany operates entirely off a declarative JSON configuration file (`apiphany_config.json`). Every attribute is strictly validated at runtime by `pydantic`.

### 1. Root Entity Structure

The top level defines logical groups of APIs (Entities), global credentials, and rate limit rules.

- **`entity_name`** (`string`): The logical name of the API provider (e.g., `"stripe"`, `"salesforce"`).
- **`post_process`** (`string`): *(Optional)* Filepath to a custom Python script to mutate raw JSON before extraction.
- **`certificates`** (`dict`): *(Optional)* Must contain `"cert"` and `"key"` paths. Supports local or `s3://` URIs for mTLS.
- **`client_credentials`** (`dict`): Global dictionary for Secrets. Supports templating (`{{variable}}`). If you provide `"secrets_manager_arn"`, Apiphany resolves it directly from AWS Secrets Manager.
- **`retry_config`** (`dict`): Contains `total_retries` (default: 3) and `requests_per_second` (default: 10).
- **`api_list`** (`list`): The core array of `APISchema` endpoint objects.

**Example:**
```json
{
    "api_config": [
        {
            "entity_name": "stripe",
            "client_credentials": {
                "secrets_manager_arn": "arn:aws:secretsmanager:us-east-1:123:secret:stripe-keys"
            },
            "retry_config": {
                "total_retries": 5,
                "requests_per_second": 20
            },
            "api_list": [ ... ]
        }
    ]
}
```

---

### 2. API Object (`api_list`)

Each object inside `api_list` defines a single executable endpoint.

- **`api_identifier`** (`string`): Unique ID used to execute the endpoint via `.execute(api_identifier="my_api")`.
- **`api_name`** (`string`): Human-readable description.
- **`method`** (`string`): HTTP Method (`"GET"`, `"POST"`, `"PUT"`, etc.).
- **`url`** (`string`): The URL. Supports templating from parent outputs (e.g., `https://api.com/users/{{id}}`).
- **`auth_type`** (`string`): Authentication type: `"None"`, `"Bearer"`, `"Basic"`, or `"APIKey"`.
- **`api_key_name`** (`string`): If auth_type is `"APIKey"`, the header key name (default: `"x-api-key"`).
- **`headers`** (`dict`): Static HTTP headers to inject.
- **`query_params`** (`dict`): Static URL parameters to inject. Supports templating.
- **`payload`** (`dict`): JSON body payload for POST/PUT requests.
- **`graphql_query`** (`string`): If supplied, forces a POST request and wraps the string as `{"query": "..."}`. Supports templating.

**Example:**
```json
{
    "api_identifier": "create_user",
    "api_name": "Create a new User",
    "method": "POST",
    "url": "https://api.example.com/users",
    "auth_type": "Bearer",
    "headers": {
        "Content-Type": "application/json"
    },
    "payload": {
        "name": "Jane",
        "role": "admin"
    }
}
```

---

### 3. Pagination Configuration

Apiphany automatically scrolls through pages until the payload is empty.

- **`type`** (`string`): `"page_based"` or `"offset_based"`.
- **`page_key`** (`string`): Parameter name for page number (default: `"page"`).
- **`size_key`** (`string`): Parameter name for page size (default: `"limit"`).
- **`page_size`** (`int`): Number of records to request per page (default: 100).
- **`offset_key`** (`string`): Parameter name for offset (default: `"offset"`).
- **`limit_key`** (`string`): Parameter name for limit (default: `"limit"`).
- **`limit_value`** (`int`): Offset increment amount (default: 100).
- **`stop_condition`** (`string`): Defines when to stop. `"no_data"` stops when returned array length < limit.

**Example (Offset Based):**
```json
"pagination": {
    "type": "offset_based",
    "offset_key": "start",
    "limit_key": "limit",
    "limit_value": 500
}
```

---

### 4. Chained Requests

Dynamically feeds the output of this API into another child API.

- **`child_api_identifier`** (`string`): The `api_identifier` of the next endpoint to trigger.
- **`key_mapping`** (`dict`): Maps parent JSON keys to child URL template parameters (e.g., `{"id": "userId"}`).
- **`max_concurrent_requests`** (`int`): Maximum parallel threads hitting the child API (default: 5).
- **`batch_size`** (`int`): If > 1, combines parent values into comma-separated lists (e.g. `1,2,3,4,5`).

**Example (Batched Child Resolution):**
```json
"chained_request": {
    "child_api_identifier": "get_user_posts",
    "key_mapping": {
        "id": "userIds"
    },
    "max_concurrent_requests": 10,
    "batch_size": 20
}
```
*If 100 IDs are fetched, it will group them into 5 concurrent requests, injecting `?userIds=1,2,3...20` into the child URL.*

---

### 5. Extractor Configuration

Defines how deeply nested API JSON is parsed and flattened into Pandas DataFrames. Powered by `json_extract_pandas`.

- **`response_data_extractor`** (`string`): Target JSON path to pluck the core array from the root response (e.g. `"data.results"`).
- **`json_extract_config.record_path`** (`list`): Path to the nested array within the row to explode into multiple rows (e.g., `["line_items"]`).
- **`json_extract_config.meta`** (`list`): Fields to duplicate across every exploded row (e.g., `["transaction_id", ["user", "name"]]`).
- **`json_extract_config.errors`** (`string`): How to handle missing keys (`"raise"` or `"ignore"`).

**Example:**
```json
"extractor_config": {
    "response_data_extractor": "data.results",
    "json_extract_config": {
        "record_path": [
            "line_items"
        ],
        "meta": [
            "transaction_id",
            "timestamp",
            ["user", "name"]
        ]
    }
}
```
*The above will flatten an array of `line_items` while ensuring `transaction_id`, `timestamp`, and `user.name` are attached as columns to every item's row.*

---

### 6. Export Configuration

Defines where Apiphany automatically saves the final dataset when execution finishes.

- **`save_output`** (`bool`): Master toggle to enable saving (default: `true`).
- **`output_type`** (`string`): `"flattened"` (Pandas CSV/JSON/SQL) or `"raw"` (Pure JSON dicts).
- **`target.type`** (`string`): Destination: `"file"` or `"sql"`.
- **`target.location`** (`string`): Local filepath (`"out.csv"`), S3 URI (`"s3://bucket/out.csv"`), or SQL URI (`"sqlite:///db.sqlite"`).
- **`target.table_name`** (`string`): (SQL Only) Name of the target database table.
- **`target.if_exists`** (`string`): (SQL Only) Behavior if table exists (`"append"`, `"replace"`, `"fail"`).

**Example (Export to SQL):**
```json
"export_config": {
    "save_output": true,
    "output_type": "flattened",
    "target": {
        "type": "sql",
        "location": "sqlite:///my_db.db",
        "table_name": "transactions",
        "if_exists": "append"
    }
}
```

---

### 7. State Tracking (Incremental Syncs)

Saves the highest watermark detected in a payload to skip historical downloads on the next run.

- **`incremental_key`** (`string`): The column to evaluate for the highest watermark (e.g., `"updated_at"`, `"id"`).

**Example:**
```json
"state_tracking": {
    "incremental_key": "updated_at"
}
```
*When you run Apiphany, it scans the DataFrame for the highest `updated_at` and saves it. On your next run, you can inject it directly into the URL by putting `{{state_updated_at}}` in your `query_params`.*

---

## Full End-to-End Example Config

Here is a complete example demonstrating how Apiphany can fetch a list of Users, automatically extract the `id` from each user, and concurrently fetch all the Posts authored by those users using batched Chained Requests. 

```json
{
    "api_config": [
        {
            "entity_name": "jsonplaceholder",
            "client_credentials": {},
            "retry_config": {
                "total_retries": 3,
                "requests_per_second": 10
            },
            "api_list": [
                {
                    "api_identifier": "get_users",
                    "api_name": "Fetch Users",
                    "method": "GET",
                    "url": "https://jsonplaceholder.typicode.com/users",
                    "auth_type": "None",
                    "chained_request": {
                        "child_api_identifier": "get_user_posts",
                        "key_mapping": {
                            "id": "userId"
                        },
                        "batch_size": 5,
                        "max_concurrent_requests": 10
                    },
                    "export_config": {
                        "save_output": true,
                        "output_type": "flattened",
                        "target": {
                            "type": "file",
                            "location": "users.csv"
                        }
                    }
                },
                {
                    "api_identifier": "get_user_posts",
                    "api_name": "Fetch Posts for User",
                    "method": "GET",
                    "url": "https://jsonplaceholder.typicode.com/posts",
                    "query_params": {
                        "userId": "{{userId}}"
                    },
                    "auth_type": "None",
                    "export_config": {
                        "save_output": true,
                        "output_type": "flattened",
                        "target": {
                            "type": "file",
                            "location": "posts.csv"
                        }
                    }
                }
            ]
        }
    ]
}
```
