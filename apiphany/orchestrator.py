import json
import json5
import yaml
import os
import asyncio
import tempfile
import pandas as pd
from urllib.parse import urlparse
from typing import Any, Dict, List, Union

import httpx
from aiolimiter import AsyncLimiter

from .utils import resolve_template
from .logger import get_logger
from .state import LocalStateManager, S3StateManager
from .secrets import AWSSecretProvider
from .schemas import EntitySchema
from json_extract_pandas import extract_json

logger = get_logger("Apiphany")

class CircuitBreakerOpenException(Exception):
    """Raised when an API circuit breaker trips due to excessive failures."""
    pass


class APIOrchestrator:
    """
    The core execution engine for the Apiphany API Orchestrator.
    
    This class handles dynamically loading configurations, parsing secrets from cloud providers,
    authenticating, and managing the heavily concurrent execution of chained API requests using `httpx` and `asyncio`.
    """
    
    def __init__(self, config_file: str, entity_name: str, client_credentials: str = None, state_file: str = "state.json"):
        """
        Initializes the Apiphany engine.
        
        Args:
            config_file (str): The physical filepath to the JSON configuration schema (e.g., `apiphany_config.json`).
            entity_name (str): The name of the API entity block to load from the config file (e.g., "stripe" or "jsonplaceholder").
            client_credentials (str, optional): A filepath or JSON string overriding `client_credentials` in the config file at runtime.
            state_file (str, optional): The location to persist incremental watermarks. If it starts with `s3://`, the engine will attempt to read/write state from AWS S3 natively. Defaults to a local "state.json".
        """
        self.config_file = config_file
        self.entity_name = entity_name
        self.state_file = state_file
        
        # Determine State Provider (Local or AWS S3)
        if self.state_file.startswith("s3://"):
            self.state_manager = S3StateManager(self.state_file)
        else:
            self.state_manager = LocalStateManager(self.state_file)
            
        self.state = self.state_manager.load()
        
        # Load and validate config strictly using Pydantic V2
        raw_config = self._load_config()
        try:
            self.api_config_model = EntitySchema.model_validate(raw_config)
        except Exception as e:
            logger.error(f"Configuration validation failed. Ensure your JSON adheres to the Pydantic schema: {e}")
            raise
            
        # Merge credentials
        self.client_credentials = self.api_config_model.client_credentials.copy() if self.api_config_model.client_credentials else {}
        if client_credentials:
            self.client_credentials.update(self._load_credentials(client_credentials))
            
        # Secrets Abstraction (Resolves AWS Secrets Manager ARNs)
        if "secrets_manager_arn" in self.client_credentials:
            arn = self.client_credentials["secrets_manager_arn"]
            if arn.startswith("arn:aws:secretsmanager"):
                logger.info("Resolving secrets using AWSSecretProvider", extra={"arn": arn})
                provider = AWSSecretProvider()
                secrets = provider.fetch_secrets(arn)
                self.client_credentials.update(secrets)
            else:
                logger.warning("Unsupported secrets manager ARN format.", extra={"arn": arn})
                
        # Certificates for Mutual TLS
        self.certificates = self.api_config_model.certificates or {}
        self.cert = None
        self._temp_certs = []
        
        if "cert" in self.certificates and "key" in self.certificates:
            resolved_cert = self._resolve_cert_path(self.certificates["cert"])
            resolved_key = self._resolve_cert_path(self.certificates["key"])
            if resolved_cert and resolved_key and os.path.exists(resolved_cert) and os.path.exists(resolved_key):
                self.cert = (resolved_cert, resolved_key)
            else:
                logger.warning("Certificate files not found or failed to download.")
                
        self.client = None
        
        # Token bucket Limiter to prevent HTTP 429 Too Many Requests errors
        retry_config = self.api_config_model.retry_config
        rps = retry_config.requests_per_second if retry_config else 10
        self.limiter = AsyncLimiter(rps, 1)
        self.access_token = None
        
        # Enterprise Upgrades State
        self.oauth_lock = asyncio.Lock()
        self._circuit_failures = {}
        self._circuit_open_until = {}

    def __del__(self):
        """Ensures any temporary certificate files downloaded from S3 are deleted when the orchestrator is destroyed."""
        for temp_file in getattr(self, "_temp_certs", []):
            try:
                if os.path.exists(temp_file):
                    os.remove(temp_file)
            except Exception:
                pass

    def _resolve_cert_path(self, path: str) -> str:
        """
        Resolves certificate paths. If the path is an S3 URI, it downloads the certificate to a secure local tempfile.
        """
        if not path: return path
        if path.startswith("s3://"):
            try:
                import boto3
                parsed = urlparse(path)
                bucket = parsed.netloc
                key = parsed.path.lstrip('/')
                s3_client = boto3.client('s3')
                fd, temp_path = tempfile.mkstemp(suffix=".pem")
                os.close(fd)
                s3_client.download_file(bucket, key, temp_path)
                self._temp_certs.append(temp_path)
                return temp_path
            except ImportError:
                logger.error("boto3 is required for S3 certs.")
                return None
            except Exception as e:
                logger.error(f"Failed to download cert from S3: {e}")
                return None
        return path

    def _load_config(self) -> Dict:
        """Reads the core `apiphany_config.json` file and plucks the active entity."""
        ext = os.path.splitext(self.config_file)[1].lower()
        with open(self.config_file, 'r') as f:
            if ext in ['.yaml', '.yml']:
                full_config = yaml.safe_load(f)
            elif ext == '.json5':
                full_config = json5.load(f)
            else:
                full_config = json.load(f)
                
            for entity in full_config.get("api_config", []):
                if entity.get("entity_name") == self.entity_name:
                    return entity
        raise ValueError(f"Entity '{self.entity_name}' not found.")
        
    def _load_credentials(self, client_credentials: Union[str, Dict]) -> Dict:
        """Parses runtime client credential overrides."""
        if isinstance(client_credentials, dict):
            return client_credentials
        elif isinstance(client_credentials, str) and os.path.exists(client_credentials):
            ext = os.path.splitext(client_credentials)[1].lower()
            with open(client_credentials, 'r') as f:
                if ext in ['.yaml', '.yml']:
                    return yaml.safe_load(f)
                elif ext == '.json5':
                    return json5.load(f)
                else:
                    return json.load(f)
        else:
            try:
                return json5.loads(client_credentials)
            except:
                return json.loads(client_credentials)

    async def _build_auth_headers(self, api_def) -> Dict:
        """
        Dynamically constructs HTTP authorization headers by reading `client_credentials`
        and resolving string templates like `{{username}}`.
        """
        auth_type = api_def.auth_type or "None"
        headers = api_def.headers.copy() if api_def.headers else {}
        
        if auth_type == "Bearer":
            headers["Authorization"] = f"Bearer {self.access_token}"
        elif auth_type == "Basic":
            user = resolve_template("{{username}}", self.client_credentials)
            pwd = resolve_template("{{password}}", self.client_credentials)
            import base64
            auth_str = base64.b64encode(f"{user}:{pwd}".encode()).decode()
            headers["Authorization"] = f"Basic {auth_str}"
        elif auth_type == "APIKey":
            key_name = api_def.api_key_name or "x-api-key"
            key_val = resolve_template(f"{{{{{key_name}}}}}", self.client_credentials)
            headers[key_name] = key_val
            
        return headers

    async def _refresh_oauth_token(self, api_def):
        """Acquires a lock to prevent stampedes and fetches a new OAuth2 access token."""
        async with self.oauth_lock:
            oauth = api_def.oauth2_config
            if not oauth: return
            
            token_url = oauth.token_url
            client_id = resolve_template(f"{{{{{oauth.client_id_key}}}}}", self.client_credentials)
            client_secret = resolve_template(f"{{{{{oauth.client_secret_key}}}}}", self.client_credentials)
            
            data = {
                "grant_type": oauth.grant_type,
                "client_id": client_id,
                "client_secret": client_secret
            }
            logger.info("Exchanging OAuth2 token...", extra={"token_url": token_url})
            try:
                resp = await self.client.post(token_url, data=data)
                resp.raise_for_status()
                token_data = resp.json()
                self.access_token = token_data.get("access_token")
                logger.info("OAuth2 token successfully refreshed.")
            except Exception as e:
                logger.error(f"Failed to refresh OAuth2 token: {e}")
                raise e

    async def _make_request(self, api_def, method: str, url: str, headers: Dict, params: Dict = None, payload: Dict = None, graphql_query: str = None):
        """
        Executes a single HTTP request natively using `httpx`.
        
        Implements strict rate limiting (governed by aiolimiter) and Exponential Backoff.
        If a server error (500, 502, 503, 504) is caught, it backs off asynchronously using `2 ^ attempt`.
        """
        if graphql_query:
            method = "POST"
            payload = {"query": graphql_query}
            
        api_id = api_def.api_identifier
        cb_config = api_def.circuit_breaker
        rl_config = api_def.rate_limit_config
        
        # Circuit Breaker Pre-Check
        if cb_config:
            import time
            open_until = self._circuit_open_until.get(api_id, 0)
            if time.time() < open_until:
                raise CircuitBreakerOpenException(f"Circuit open for {api_id}. Failing fast.")
            elif open_until != 0:
                self._circuit_open_until[api_id] = 0 # Half-open state
                
        retry_config = self.api_config_model.retry_config
        retries = retry_config.total_retries if retry_config else 3
        
        for attempt in range(retries):
            async with self.limiter:
                try:
                    response = await self.client.request(method, url, headers=headers, params=params, json=payload)
                    
                    # Circuit Breaker Reset on success
                    if cb_config:
                        self._circuit_failures[api_id] = 0
                        
                    # Smart Throttle
                    if rl_config and rl_config.remaining_header in response.headers:
                        try:
                            remaining = int(response.headers[rl_config.remaining_header])
                            if remaining <= 0:
                                reset_val = response.headers.get(rl_config.reset_header)
                                if reset_val:
                                    import time
                                    reset_val_float = float(reset_val)
                                    sleep_time = max(0, reset_val_float - time.time()) if reset_val_float > 1e9 else reset_val_float
                                    logger.warning(f"Smart Throttle active. Sleeping for {sleep_time}s.")
                                    await asyncio.sleep(sleep_time)
                        except Exception as e:
                            logger.error(f"Failed to parse rate limit headers: {e}")

                    response.raise_for_status()
                    return response.json()
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 401 and api_def.oauth2_config:
                        logger.warning(f"401 Unauthorized for {api_id}. Triggering OAuth2 refresh.")
                        await self._refresh_oauth_token(api_def)
                        headers = await self._build_auth_headers(api_def) # Update auth header
                        continue
                        
                    if e.response.status_code == 429:
                        retry_after = int(e.response.headers.get("Retry-After", 2))
                        logger.warning("Rate limited.", extra={"url": url, "retry_after": retry_after})
                        await asyncio.sleep(retry_after)
                    elif e.response.status_code in [500, 502, 503, 504]:
                        if cb_config:
                            import time
                            fails = self._circuit_failures.get(api_id, 0) + 1
                            self._circuit_failures[api_id] = fails
                            if fails >= cb_config.failure_threshold:
                                self._circuit_open_until[api_id] = time.time() + cb_config.recovery_timeout_seconds
                                logger.error(f"Circuit breaker tripped for {api_id}! Fails: {fails}")
                                raise CircuitBreakerOpenException(f"Circuit breaker tripped for {api_id}")
                                
                        # Exponential Backoff execution
                        backoff_time = (2 ** attempt)
                        logger.warning("Server error. Retrying...", extra={"url": url, "status_code": e.response.status_code, "backoff": backoff_time})
                        await asyncio.sleep(backoff_time)
                    else:
                        logger.error("HTTP Error", extra={"url": url, "status_code": e.response.status_code, "text": e.response.text})
                        raise e
                except Exception as e:
                    logger.warning("Request failed.", extra={"url": url, "error": str(e)})
                    await asyncio.sleep(1)
        raise Exception(f"Max retries exceeded for {url}")

    def _run_post_process(self, data: Any, script_path: str) -> Any:
        """Executes an optional custom python script to mutate raw JSON prior to extraction."""
        if not script_path or not os.path.exists(script_path):
            return data
        import importlib.util
        spec = importlib.util.spec_from_file_location("post_process_module", script_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        if hasattr(module, 'process'):
            return module.process(data)
        return data

    def _process_response_data(self, response_data: Any, data_extractor: str) -> List[Dict]:
        """Plucks the core data array out of a nested API response. e.g. 'data.users'."""
        if not data_extractor:
            if isinstance(response_data, list): return response_data
            elif isinstance(response_data, dict): return [response_data]
            return [{"value": response_data}]
            
        extracted = response_data
        for key in data_extractor.split('.'):
            if isinstance(extracted, dict) and key in extracted:
                extracted = extracted[key]
            else:
                return []
                
        if isinstance(extracted, list): return extracted
        elif isinstance(extracted, dict): return [extracted]
        return [{"value": extracted}]

    def _resolve_payload(self, payload: Any, context: Dict) -> Any:
        """Recursively resolves templates within a JSON payload."""
        if isinstance(payload, dict):
            return {k: self._resolve_payload(v, context) for k, v in payload.items()}
        elif isinstance(payload, list):
            return [self._resolve_payload(item, context) for item in payload]
        elif isinstance(payload, str):
            return resolve_template(payload, context)
        return payload

    async def _fetch_all_pages(self, api_def, base_url: str, base_params: Dict, headers: Dict, payload: Dict = None, graphql_query: str = None) -> List[Dict]:
        """
        Manages automatic pagination (e.g. Page/Limit or Offset/Limit), continuously aggregating payloads 
        until the API stops returning data.
        """
        pag_config = api_def.pagination
        data_extractor = api_def.extractor_config.response_data_extractor if api_def.extractor_config else ""
        post_process = self.api_config_model.post_process
        
        if not pag_config:
            method = api_def.method or "GET"
            json_resp = await self._make_request(api_def, method, base_url, headers, params=base_params, payload=payload, graphql_query=graphql_query)
            if post_process:
                json_resp = self._run_post_process(json_resp, post_process)
            return self._process_response_data(json_resp, data_extractor)
            
        all_records = []
        pag_type = pag_config.type
        stop_cond = pag_config.stop_condition or "no_data"
        
        if pag_type == "page_based":
            page = 1
            page_key = pag_config.page_key or "page"
            size_key = pag_config.size_key or "limit"
            page_size = pag_config.page_size or 100
            
            while True:
                params = base_params.copy()
                params[page_key] = str(page)
                params[size_key] = str(page_size)
                
                method = api_def.method or "GET"
                json_resp = await self._make_request(api_def, method, base_url, headers, params=params, payload=payload, graphql_query=graphql_query)
                if post_process:
                    json_resp = self._run_post_process(json_resp, post_process)
                records = self._process_response_data(json_resp, data_extractor)
                
                if not records: break
                all_records.extend(records)
                if stop_cond == "no_data" and len(records) < page_size: break
                page += 1
                
        elif pag_type == "offset_based":
            offset = 0
            offset_key = pag_config.offset_key or "offset"
            limit_key = pag_config.limit_key or "limit"
            limit_val = pag_config.limit_value or 100
            
            while True:
                params = base_params.copy()
                params[offset_key] = str(offset)
                params[limit_key] = str(limit_val)
                
                method = api_def.method or "GET"
                json_resp = await self._make_request(api_def, method, base_url, headers, params=params, payload=payload, graphql_query=graphql_query)
                if post_process:
                    json_resp = self._run_post_process(json_resp, post_process)
                records = self._process_response_data(json_resp, data_extractor)
                
                if not records: break
                all_records.extend(records)
                if stop_cond == "no_data" and len(records) < limit_val: break
                offset += limit_val
                
        elif pag_type == "cursor_based":
            cursor = None
            cursor_path = pag_config.cursor_path
            cursor_query_key = pag_config.cursor_query_key or "cursor"
            
            if not cursor_path:
                raise ValueError("cursor_based pagination requires 'cursor_path' to be defined in config.")
                
            while True:
                params = base_params.copy()
                if cursor is not None:
                    params[cursor_query_key] = str(cursor)
                
                method = api_def.method or "GET"
                json_resp = await self._make_request(api_def, method, base_url, headers, params=params, payload=payload, graphql_query=graphql_query)
                
                # Extract next cursor before post-processing or data extraction strips the metadata
                next_cursor = None
                if isinstance(json_resp, dict):
                    val = json_resp
                    for k in cursor_path.split('.'):
                        if isinstance(val, dict):
                            val = val.get(k)
                        else:
                            val = None
                            break
                    next_cursor = val
                
                if post_process:
                    json_resp = self._run_post_process(json_resp, post_process)
                
                records = self._process_response_data(json_resp, data_extractor)
                if records:
                    all_records.extend(records)
                
                if not next_cursor or next_cursor == cursor:
                    break
                cursor = next_cursor
                
        else:
            raise NotImplementedError(f"Pagination type {pag_type} is not implemented.")
            
        return all_records

    async def execute(self, api_identifier: str, query_params: Dict[str, str] = None, path_params: Dict[str, str] = None, payload: Any = None, output_format: str = "flattened"):
        """
        The master entrypoint for executing an API call.
        
        Args:
            api_identifier (str): The unique ID of the endpoint to execute from the config JSON.
            query_params (Dict): Optional dynamic query parameters to inject.
            path_params (Dict): Optional dynamic URL template replacements.
            payload (Any): Optional runtime payload to override the config file payload.
            output_format (str): "flattened" (Pandas DataFrame) or "raw" (Pure list of JSON dictionaries).
            
        Returns:
            The raw JSON array or Pandas DataFrame of the fully extracted and flattened records.
        """
        client_managed_here = False
        if self.client is None:
            self.client = httpx.AsyncClient(cert=self.cert, timeout=60.0)
            client_managed_here = True
            
        try:
            api_def = next((api for api in self.api_config_model.api_list if api.api_identifier == api_identifier), None)
            if not api_def:
                raise ValueError(f"API Identifier '{api_identifier}' not found.")
                
            query_params = query_params or {}
            context = self.client_credentials.copy()
            if path_params: context.update(path_params)
            
            url = resolve_template(api_def.url, context)
            graphql_query = resolve_template(api_def.graphql_query, context) if api_def.graphql_query else None
            
            base_params = {k: resolve_template(v, context) for k, v in (api_def.query_params or {}).items()}
            base_params.update(query_params)
            
            if payload is not None:
                resolved_payload = payload
            else:
                resolved_payload = self._resolve_payload(api_def.payload, context) if api_def.payload else None
            
            headers = await self._build_auth_headers(api_def)
            
            # Fetch root
            logger.info("Executing API", extra={"api": api_identifier, "url": url})
            all_records = await self._fetch_all_pages(api_def, url, base_params, headers, resolved_payload, graphql_query)
            
            extract_config = None
            if api_def.extractor_config and api_def.extractor_config.json_extract_config:
                # Convert Pydantic object back to dict so json_extract_pandas can use kwargs
                extract_config = api_def.extractor_config.json_extract_config.model_dump(exclude_unset=True)
                
            if output_format == "raw" or not extract_config:
                final_output = all_records
                df = None
            else:
                meta, df = extract_json(all_records, **extract_config)
                final_output = df

            # Chained Requests execution loop
            chained_request = api_def.chained_request
            if chained_request:
                child_api_id = chained_request.child_api_identifier
                key_mapping = chained_request.key_mapping
                max_concurrent = chained_request.max_concurrent_requests or 1
                batch_size = chained_request.batch_size or 1
                
                param_combinations = []
                if df is not None:
                    missing_cols = [col for col in key_mapping.keys() if col not in df.columns]
                    if not missing_cols:
                        unique_rows = df.dropna(subset=list(key_mapping.keys())).drop_duplicates(subset=list(key_mapping.keys()))
                        for _, row in unique_rows.iterrows():
                            combo = {child_param: str(row[parent_col]) for parent_col, child_param in key_mapping.items()}
                            param_combinations.append(combo)
                elif isinstance(final_output, list):
                    seen = set()
                    for rec in final_output:
                        if isinstance(rec, dict):
                            combo = {}
                            for parent_col, child_param in key_mapping.items():
                                if parent_col in rec:
                                    combo[child_param] = str(rec[parent_col])
                            if len(combo) == len(key_mapping):
                                combo_tuple = tuple(sorted(combo.items()))
                                if combo_tuple not in seen:
                                    seen.add(combo_tuple)
                                    param_combinations.append(combo)
                                    
                if param_combinations:
                    batched = []
                    for i in range(0, len(param_combinations), batch_size):
                        chunk = param_combinations[i:i + batch_size]
                        if batch_size > 1:
                            merged = {k: ",".join([c[k] for c in chunk]) for k in key_mapping.values()}
                            batched.append(merged)
                        else:
                            batched.append(chunk[0])

                    child_results = []
                    # Throttle massive chained jobs using asyncio Semaphore
                    sem = asyncio.Semaphore(max_concurrent)
                    
                    async def _bounded_fetch(combo):
                        async with sem:
                            try:
                                child_path_params = path_params.copy() if path_params else {}
                                child_path_params.update(combo)
                                # Recursively trigger execute on child endpoint
                                child_res = await self.execute(child_api_id, query_params=query_params, path_params=child_path_params, output_format=output_format)
                                
                                if output_format == "raw" or df is None:
                                    if isinstance(child_res, list):
                                        for item in child_res:
                                            if isinstance(item, dict):
                                                for k, v in combo.items(): item[f"_parent_batch_{k}"] = v
                                else:
                                    if child_res is not None and not child_res.empty:
                                        for k, v in combo.items(): child_res[f"_parent_batch_{k}"] = v
                                return child_res
                            except Exception as e:
                                logger.error("Chained request failed.", extra={"combo": combo, "error": str(e)})
                                return None

                    tasks = [_bounded_fetch(combo) for combo in batched]
                    results = await asyncio.gather(*tasks)
                    
                    for res in results:
                        if res is not None:
                            if isinstance(res, list) and (output_format == "raw" or df is None):
                                child_results.extend(res)
                            else:
                                child_results.append(res)
                                
                    if output_format == "raw" or df is None:
                        return child_results
                    else:
                        valid_dfs = [cr for cr in child_results if cr is not None and not cr.empty]
                        return pd.concat(valid_dfs, ignore_index=True) if valid_dfs else pd.DataFrame()
                        
            # State Tracking Evaluation
            state_tracking = api_def.state_tracking
            if state_tracking:
                inc_key = state_tracking.incremental_key
                max_val = None
                if df is not None and inc_key in df.columns:
                    max_val = df[inc_key].dropna().max()
                elif isinstance(final_output, list):
                    vals = [rec.get(inc_key) for rec in final_output if isinstance(rec, dict) and rec.get(inc_key) is not None]
                    if vals: max_val = max(vals)
                        
                if max_val is not None:
                    self.state[inc_key] = str(max_val)
                    self.state_manager.save(self.state)
                    
            # Auto-Export Triggers
            exp = api_def.export_config
            if exp and exp.save_output:
                self._export_data(final_output, exp)
                
            return final_output
        finally:
            if client_managed_here and self.client is not None:
                await self.client.aclose()
                self.client = None

    def _export_data(self, data: Any, exp):
        """Dumps final Pandas DataFrames or JSON payloads straight to physical files, S3, or SQL."""
        if not exp.target: return
        target = exp.target
        type_ = target.type
        location = target.location
        output_type = exp.output_type or "flattened"
        
        if type_ == "sql":
            if not isinstance(data, pd.DataFrame):
                logger.error("Data must be a DataFrame to export to SQL.")
                return
            table_name = target.table_name or "output"
            import sqlalchemy
            engine = sqlalchemy.create_engine(location)
            data.to_sql(table_name, engine, if_exists=target.if_exists or "append", index=False)
            logger.info("Exported data to SQL", extra={"table": table_name, "location": location})
            
        elif type_ == "file":
            if output_type == "raw":
                content = json.dumps(data, indent=4)
            else:
                if isinstance(data, pd.DataFrame):
                    content = data.to_csv(index=False) if location.endswith(".csv") else data.to_json(orient="records", indent=4)
                else:
                    content = json.dumps(data, indent=4)
                    
            if location.startswith("s3://"):
                import boto3
                parsed = urlparse(location)
                boto3.client('s3').put_object(Bucket=parsed.netloc, Key=parsed.path.lstrip('/'), Body=content)
            else:
                with open(location, "w") as f:
                    f.write(content)
            logger.info("Exported data to file", extra={"location": location})
