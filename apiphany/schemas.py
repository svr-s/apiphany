from typing import Optional, List, Dict, Union, Any
from pydantic import BaseModel, Field

class PaginationSchema(BaseModel):
    """
    Defines how the engine should handle API pagination automatically.
    
    Attributes:
        type (str): The pagination strategy to use (e.g., "page_based", "offset_based").
        page_key (str): The URL query parameter representing the current page number (default: "page").
        size_key (str): The URL query parameter representing the page size limit (default: "limit").
        page_size (int): The number of records to request per page (default: 100).
        offset_key (str): The URL query parameter for offset-based pagination (default: "offset").
        limit_key (str): The URL query parameter for offset limits (default: "limit").
        limit_value (int): The amount to increment the offset by per page (default: 100).
        cursor_path (str): The dot-notation path to extract the next cursor from the JSON response (e.g., "meta.next_token").
        cursor_query_key (str): The URL query parameter where the cursor should be injected (default: "cursor").
        stop_condition (str): Defines when pagination should cease (default: "no_data").
    """
    type: str
    page_key: Optional[str] = "page"
    size_key: Optional[str] = "limit"
    page_size: Optional[int] = 100
    offset_key: Optional[str] = "offset"
    limit_key: Optional[str] = "limit"
    limit_value: Optional[int] = 100
    cursor_path: Optional[str] = None
    cursor_query_key: Optional[str] = "cursor"
    stop_condition: Optional[str] = "no_data"


class ChainedRequestSchema(BaseModel):
    """
    Defines a relational API request that automatically triggers for every record fetched.
    
    Attributes:
        child_api_identifier (str): The unique ID of the next API to execute.
        key_mapping (Dict[str, str]): A dictionary mapping the parent record's key to the child URL parameter. 
                                      (e.g., {"id": "postId"} -> injects the parent's "id" value into {{postId}} in the child URL).
        max_concurrent_requests (int): The strict asyncio semaphore limit for hitting the child API (default: 5).
        batch_size (int): How many parameters to bundle together if the child API supports comma-separated batched requests (default: 1).
    """
    child_api_identifier: str
    key_mapping: Dict[str, str]
    max_concurrent_requests: Optional[int] = 5
    batch_size: Optional[int] = 1


class StateTrackingSchema(BaseModel):
    """
    Tracks high-watermarks (state) to ensure incremental data fetches only pull new records.
    
    Attributes:
        incremental_key (str): The column or key in the API payload representing the watermark (e.g., "updated_at", "id").
    """
    incremental_key: str


class JsonExtractConfigSchema(BaseModel):
    """
    Configuration parameters explicitly passed to `json_extract_pandas` to parse deeply nested JSON.
    
    Attributes:
        record_path (str | List[str]): The JSON path to the nested array to unpack.
        meta (List): The JSON paths to scalar fields to attach to every unpacked record.
        meta_prefix (str): A string prefix to attach to all unpacked meta columns.
        record_prefix (str): A string prefix to attach to all unpacked record columns.
        errors (str): How to handle KeyError extraction failures (default: "raise").
        sep (str): The delimiter used for nested flattening (default: ".").
        max_level (int): Maximum depth to automatically flatten.
    """
    record_path: Optional[Union[str, List[str]]] = None
    meta: Optional[List[Union[str, List[str], Dict[str, Any]]]] = None
    meta_prefix: Optional[str] = None
    record_prefix: Optional[str] = None
    errors: Optional[str] = "raise"
    sep: Optional[str] = "."
    max_level: Optional[int] = None


class ExtractorConfigSchema(BaseModel):
    """
    Defines how the raw API response is parsed and flattened into a Pandas DataFrame.
    
    Attributes:
        response_data_extractor (str): The initial JSON path to pluck out the list of records from the root payload (e.g. "data.users").
        json_extract_config (JsonExtractConfigSchema): The configuration passed to `json_extract_pandas`.
    """
    response_data_extractor: Optional[str] = None
    json_extract_config: Optional[JsonExtractConfigSchema] = None


class ExportTargetSchema(BaseModel):
    """
    Defines the destination for an automated data export.
    
    Attributes:
        type (str): The destination type ("sql" or "file").
        location (str): The physical destination. Can be a local filepath ("output.csv"), an S3 URI ("s3://bucket/output.json"), or a SQL Alchemy URI ("sqlite:///db.sqlite").
        table_name (str): (SQL Only) The name of the database table to insert data into.
        if_exists (str): (SQL Only) Action to take if the table already exists ("append", "replace", "fail").
    """
    type: str # "sql", "file"
    location: str # path or s3 uri
    table_name: Optional[str] = None
    if_exists: Optional[str] = "append"


class ExportConfigSchema(BaseModel):
    """
    Configuration defining whether and how the final dataset should be automatically saved.
    
    Attributes:
        save_output (bool): Master toggle to enable/disable automated saving.
        output_type (str): "flattened" (Pandas CSV/JSON/SQL) or "raw" (Pure list of JSON dictionaries).
        target (ExportTargetSchema): The exact destination block.
    """
    save_output: bool = True
    output_type: Optional[str] = "flattened"
    target: Optional[ExportTargetSchema] = None


class OAuth2ConfigSchema(BaseModel):
    """
    Configuration for automated OAuth2 token refresh flows.
    """
    token_url: str
    client_id_key: str = "client_id"
    client_secret_key: str = "client_secret"
    grant_type: str = "client_credentials"

class CircuitBreakerSchema(BaseModel):
    """
    Configuration for system resilience against failing downstream APIs.
    """
    failure_threshold: int = 10
    recovery_timeout_seconds: int = 300

class RateLimitSchema(BaseModel):
    """
    Configuration for dynamic rate-limit header parsing (Smart Throttle).
    """
    remaining_header: str = "x-ratelimit-remaining"
    reset_header: str = "x-ratelimit-reset"


class APISchema(BaseModel):
    """
    The core schema representing a single runnable API endpoint definition.
    
    Attributes:
        api_identifier (str): The unique ID used to trigger this API in the code.
        api_name (str): A human-readable description.
        method (str): HTTP method (e.g., "GET", "POST").
        url (str): The endpoint URL. Supports {{template}} variables.
        auth_type (str): "None", "Bearer", "Basic", or "APIKey".
        api_key_name (str): If auth_type is "APIKey", the HTTP header key to inject the token into (default: "x-api-key").
        headers (Dict): Static HTTP headers to inject into the request.
        query_params (Dict): URL query parameters to attach. Supports {{template}} variables.
        payload (Dict): JSON body payload for POST/PUT requests.
        graphql_query (str): If supplied, forces the request to POST and sends the string as a GraphQL `{"query": ...}` body.
        pagination (PaginationSchema): Configuration for handling automated page scrolling.
        extractor_config (ExtractorConfigSchema): Configuration for transforming the JSON response into a Pandas DataFrame.
        chained_request (ChainedRequestSchema): Configuration for automatically triggering a downstream child API.
        state_tracking (StateTrackingSchema): Configuration for watermarking incremental data loads.
        oauth2_config (OAuth2ConfigSchema): Configuration for auto-refreshing OAuth tokens.
        circuit_breaker (CircuitBreakerSchema): Configuration for opening the circuit on cascading failures.
        rate_limit_config (RateLimitSchema): Configuration for Smart Throttling.
    """
    api_identifier: str
    api_name: Optional[str] = "Unnamed API"
    method: Optional[str] = "GET"
    url: str
    auth_type: Optional[str] = "None"
    api_key_name: Optional[str] = "x-api-key"
    headers: Optional[Dict[str, str]] = None
    query_params: Optional[Dict[str, str]] = None
    payload: Optional[Dict[str, Any]] = None
    graphql_query: Optional[str] = None
    
    pagination: Optional[PaginationSchema] = None
    extractor_config: Optional[ExtractorConfigSchema] = None
    chained_request: Optional[ChainedRequestSchema] = None
    state_tracking: Optional[StateTrackingSchema] = None
    oauth2_config: Optional[OAuth2ConfigSchema] = None
    circuit_breaker: Optional[CircuitBreakerSchema] = None
    rate_limit_config: Optional[RateLimitSchema] = None
    export_config: Optional[ExportConfigSchema] = None


class RetryConfigSchema(BaseModel):
    """
    Defines engine-level resilience and rate-limiting constraints.
    
    Attributes:
        total_retries (int): Maximum number of retry attempts for network failures or 5xx errors (default: 3).
        requests_per_second (int): The strict hard limit for requests sent per second to prevent API blocking (default: 10).
    """
    total_retries: Optional[int] = 3
    requests_per_second: Optional[int] = 10


class EntitySchema(BaseModel):
    """
    The root schema representing the entire `apiphany_config.json` definition for an API group.
    
    Attributes:
        entity_name (str): The logical name of the API entity group (e.g., "stripe", "jsonplaceholder").
        api_list (List[APISchema]): The list of configured API endpoints belonging to this entity.
        post_process (str): Optional python script filepath to execute after fetching but before extraction.
        certificates (Dict): SSL configuration. Keys must be "cert" and "key". Values can be local filepaths or `s3://` URIs.
        client_credentials (Dict): Core dictionary for secrets and templating. Insert "secrets_manager_arn" to dynamically fetch from AWS.
        retry_config (RetryConfigSchema): The global resilience limits applied to the HTTP client.
    """
    entity_name: str
    api_list: List[APISchema]
    post_process: Optional[str] = None
    certificates: Optional[Dict[str, str]] = Field(default_factory=dict)
    client_credentials: Optional[Dict[str, Any]] = Field(default_factory=dict)
    retry_config: Optional[RetryConfigSchema] = Field(default_factory=RetryConfigSchema)
