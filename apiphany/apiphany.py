import json
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import Any, Dict, List, Union, Optional

try:
    from pydantic import BaseModel, ValidationError
    from typing import Literal
    HAS_PYDANTIC = True
    
    class PaginationSchema(BaseModel):
        type: Literal["page_based", "offset_based", "cursor_based"]
        stop_condition: str = "no_data"
        
    class ChainedRequestSchema(BaseModel):
        child_api_identifier: str
        key_mapping: Dict[str, str]
        max_concurrent_requests: Optional[int] = 1
        batch_size: Optional[int] = 1
        
    class StateTrackingSchema(BaseModel):
        incremental_key: str
        
    class ExportTargetSchema(BaseModel):
        type: str
        connection_string: Optional[str] = None
        table_name: Optional[str] = None
        if_exists: Optional[str] = "append"
        bucket_name: Optional[str] = None
        object_key: Optional[str] = None
        
    class APISchema(BaseModel):
        api_identifier: str
        method: str
        url: str
        auth_type: str
        pagination: Optional[PaginationSchema] = None
        chained_request: Optional[ChainedRequestSchema] = None
        state_tracking: Optional[StateTrackingSchema] = None
        export_target: Optional[ExportTargetSchema] = None
        
    class EntitySchema(BaseModel):
        entity_name: str
        api_list: List[APISchema]
        post_process: Optional[str] = None
        
except ImportError:
    HAS_PYDANTIC = False

import re
from datetime import datetime, timedelta
import argparse
import sys

import os
import logging
import time
import concurrent.futures

# Configure basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("Apiphany")

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../JSON Extract/json-extract-package/src")))

try:
    from json_extract_pandas import extract_json
except ImportError:
    extract_json = None

def resolve_template(value: str, context: Dict[str, Any]) -> str:
    """Replaces {{key}} in a string with the corresponding value from the context, or evaluates native macros."""
    if not isinstance(value, str):
        return value
    
    # 1. Resolve macros first (e.g. {{macro:date:today-7|%Y-%m-%d}})
    macro_matches = re.findall(r"\{\{macro:(.*?)\}\}", value)
    for macro in macro_matches:
        parts = macro.split("|")
        instruction = parts[0]
        fmt = parts[1] if len(parts) > 1 else "%Y-%m-%d"
        
        if instruction.startswith("date:"):
            date_logic = instruction.split(":")[1] # e.g. "today-7", "today"
            target_date = datetime.now()
            
            if "today-" in date_logic:
                try:
                    days_back = int(date_logic.split("-")[1])
                    target_date -= timedelta(days=days_back)
                except ValueError:
                    pass
            elif "today+" in date_logic:
                try:
                    days_fwd = int(date_logic.split("+")[1])
                    target_date += timedelta(days=days_fwd)
                except ValueError:
                    pass
                    
            replacement = target_date.strftime(fmt)
            value = value.replace(f"{{{{macro:{macro}}}}}", replacement)
    
    # 2. Resolve Environment Variables (e.g. {{env:AWS_SECRET_KEY}})
    env_matches = re.findall(r"\{\{env:(.*?)\}\}", value)
    for env_var in env_matches:
        replacement = os.environ.get(env_var, "")
        value = value.replace(f"{{{{env:{env_var}}}}}", replacement)
    
    # 3. Resolve context variables
    for k, v in context.items():
        if k != "state":  # avoid recursive dict replacement issue
            value = value.replace(f"{{{{{k}}}}}", str(v))
            
    # 4. Resolve state variables (e.g. {{state:last_modified}})
    state_matches = re.findall(r"\{\{state:(.*?)\}\}", value)
    for var in state_matches:
        state_val = context.get("state", {}).get(var, "")
        value = value.replace(f"{{{{state:{var}}}}}", str(state_val))
        
    return value

def extract_data(payload: Any, path: str) -> Any:
    """Extracts nested data from a dictionary using dot-notation (e.g., 'data.workers')."""
    if not path:
        return payload
    
    keys = path.split(".")
    current = payload
    for key in keys:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return None
    return current


class APIOrchestrator:
    def __init__(self, config_file: str, entity_name: str, client_credentials: str = None, state_file: str = "state.json"):
        self.config_file = config_file
        self.entity_name = entity_name
        self.state_file = state_file
        self.state = self._load_state()
        
        # Load API definition
        self.api_config = self._load_config()
        self.certificates = self.api_config.get("certificates", {})
        
        # Merge config credentials with any runtime credentials provided
        self.client_credentials = self.api_config.get("client_credentials", {}).copy()
        if client_credentials:
            self.client_credentials.update(self._load_credentials(client_credentials))
            
        self.auth_strategy = self.api_config.get("auth_strategy", {})
        
        # Requests cert format: (cert_file, key_file)
        self.cert = None
        if "cert" in self.certificates and "key" in self.certificates:
            self.cert = (self.certificates["cert"], self.certificates["key"])
            
        # Requests Session
        self.session = requests.Session()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        self.session.mount("http://", HTTPAdapter(max_retries=retries))
        self.session.mount("https://", HTTPAdapter(max_retries=retries))
        
        self.access_token = None

    def _load_state(self) -> Dict[str, Any]:
        if os.path.exists(self.state_file):
            try:
                import json
                with open(self.state_file, "r") as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Error reading state file {self.state_file}: {e}")
        return {}

    def _save_state(self):
        try:
            import json
            with open(self.state_file, "w") as f:
                json.dump(self.state, f, indent=4)
        except Exception as e:
            logger.error(f"Error writing state file {self.state_file}: {e}")

    def _load_config(self) -> Dict[str, Any]:
        with open(self.config_file, 'r') as f:
            data = json.load(f)
        for entity in data.get("api_config", []):
            if entity.get("entity_name") == self.entity_name:
                return entity
        raise ValueError(f"Entity '{self.entity_name}' not found in configuration.")

    def _load_credentials(self, cred_string: str) -> Dict[str, Any]:
        """Loads client credentials from a JSON string."""
        if not cred_string:
            return {}
        try:
            return json.loads(cred_string)
        except Exception as e:
            logger.error(f"Error parsing runtime client credentials: {e}")
            return {}

    def _make_request(self, method: str, url: str, **kwargs) -> requests.Response:
        """Wrapper around session.request to intelligently handle HTTP 429 Rate Limiting."""
        max_retries = 5
        backoff_factor = 1.0
        
        for attempt in range(max_retries + 1):
            response = self.session.request(method, url, **kwargs)
            
            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    sleep_time = int(retry_after)
                else:
                    sleep_time = backoff_factor * (2 ** attempt)
                    
                if attempt < max_retries:
                    logger.warning(f"HTTP 429 Rate Limited. Sleeping for {sleep_time}s before retry (Attempt {attempt+1}/{max_retries})...")
                    time.sleep(sleep_time)
                    continue
                else:
                    logger.error("Max retries exceeded for HTTP 429.")
                    response.raise_for_status()
                    
            response.raise_for_status()
            return response
            
        return response

    def authenticate(self):
        """Authenticates based on the auth_strategy and stores the access token."""
        if not self.auth_strategy:
            logger.info("No auth strategy defined. Skipping authentication.")
            return

        auth_type = self.auth_strategy.get("type")
        
        if auth_type == "oauth2_client_credentials":
            url = self.auth_strategy.get("url")
            method = self.auth_strategy.get("method", "POST")
            headers = self.auth_strategy.get("headers", {})
            raw_payload = self.auth_strategy.get("payload", {})
            
            payload = {}
            for k, v in raw_payload.items():
                payload[k] = resolve_template(v, self.client_credentials)
                
            logger.info(f"Authenticating against {url}...")
            response = self.session.request(method=method, url=url, headers=headers, data=payload, cert=self.cert)
            response.raise_for_status()
            
            token_path = self.auth_strategy.get("token_extractor_path", "access_token")
            self.access_token = extract_data(response.json(), token_path)
            
            if not self.access_token:
                raise ValueError(f"Failed to extract access token using path: '{token_path}'")
                
            logger.info("Successfully authenticated and extracted token.")
        else:
            raise NotImplementedError(f"Auth type '{auth_type}' is not yet implemented.")

    def _build_auth_headers(self, api_def: Dict[str, Any]) -> Dict[str, str]:
        """Constructs authentication headers based on the API definition and active tokens."""
        headers = api_def.get("headers", {}).copy()
        auth_type = api_def.get("auth_type", "")
        
        if auth_type == "Bearer":
            if not self.access_token:
                self.authenticate()
            headers["Authorization"] = f"Bearer {self.access_token}"
            
        elif auth_type == "Basic":
            import base64
            user = self.client_credentials.get("username", self.client_credentials.get("client_id", ""))
            pwd = self.client_credentials.get("password", self.client_credentials.get("client_secret", ""))
            token = base64.b64encode(f"{user}:{pwd}".encode()).decode()
            headers["Authorization"] = f"Basic {token}"
            
        return headers

    def _process_response_data(self, json_resp: Any, data_extractor: str) -> List[Any]:
        """Extracts data and consistently coerces it into a list of records."""
        extracted_data = extract_data(json_resp, data_extractor)
        
        if isinstance(extracted_data, list):
            return extracted_data
        elif extracted_data:
            return [extracted_data]
        return []

    def _export_dataframe(self, df, export_config: Dict[str, Any]):
        target_type = export_config.get("type")
        if target_type == "sql":
            try:
                from sqlalchemy import create_engine
                conn_str = resolve_template(export_config.get("connection_string", ""), self.client_credentials)
                table = export_config.get("table_name", "api_export")
                if_exists = export_config.get("if_exists", "append")
                
                engine = create_engine(conn_str)
                df.to_sql(table, con=engine, if_exists=if_exists, index=False)
                logger.info(f"Successfully exported {len(df)} rows to SQL table '{table}'")
            except ImportError:
                logger.error("SQL export failed: 'sqlalchemy' is not installed. (pip install sqlalchemy)")
            except Exception as e:
                logger.error(f"SQL export failed: {e}")
        elif target_type == "s3":
            try:
                import boto3
                import io
                
                bucket = resolve_template(export_config.get("bucket_name", ""), self.client_credentials)
                key = resolve_template(export_config.get("object_key", ""), self.client_credentials)
                
                csv_buffer = io.StringIO()
                df.to_csv(csv_buffer, index=False)
                
                s3_resource = boto3.resource('s3')
                s3_resource.Object(bucket, key).put(Body=csv_buffer.getvalue())
                logger.info(f"Successfully exported {len(df)} rows to s3://{bucket}/{key}")
            except ImportError:
                logger.error("S3 export failed: 'boto3' is not installed. (pip install boto3)")
            except Exception as e:
                logger.error(f"S3 export failed: {e}")

    def _run_post_process(self, data: Any, script_path: str) -> Any:
        import importlib.util
        if not os.path.exists(script_path):
            logger.error(f"Post-process script {script_path} not found.")
            return data
            
        try:
            spec = importlib.util.spec_from_file_location("post_process_module", script_path)
            post_process_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(post_process_module)
            
            if hasattr(post_process_module, "process"):
                logger.info(f"Running post-process hook: {script_path}")
                return post_process_module.process(data)
            else:
                logger.warning(f"Post-process script {script_path} missing 'process(data)' function.")
                return data
        except Exception as e:
            logger.error(f"Error running post-process script {script_path}: {e}")
            return data

    def execute(self, api_identifier: str, path_params: Dict[str, Any] = None, query_params: Dict[str, Any] = None, output_format: str = "auto") -> Union[List[Any], Any]:
        """
        Executes an API request (including pagination if configured).
        
        :param api_identifier: The ID of the API from the config.
        :param path_params: Runtime dictionary to replace {{var}} in URL.
        :param query_params: Runtime dictionary to merge with config query params.
        :param output_format: 'auto' (applies json_extract_pandas if configured), 'raw' (always returns raw stitched JSON).
        :return: List of all fetched records (raw JSON) or a Pandas DataFrame.
        """
        path_params = path_params or {}
        query_params = query_params or {}
        
        api_def = next((api for api in self.api_config.get("api_list", []) if api.get("api_identifier") == api_identifier), None)
        if not api_def:
            raise ValueError(f"API Identifier '{api_identifier}' not found for entity '{self.entity_name}'")
            
        context = self.client_credentials.copy()
        if path_params:
            context.update(path_params)
        context["state"] = self.state
        
        raw_url = api_def.get("url")
        url = resolve_template(raw_url, context)
        
        # Merge configuration query parameters with runtime query parameters
        config_query_params = api_def.get("query_params", {})
        base_params = {**config_query_params, **query_params}
        
        method = api_def.get("method", "GET")
        headers = self._build_auth_headers(api_def)
        pagination = api_def.get("pagination", {})
        # Apply dynamic configurations
        data_extractor = api_def.get("response_data_extractor", "")
        post_process = api_def.get("post_process", None)
        
        all_records = []
        pag_type = pagination.get("type")
        
        if pag_type == "page_based":
            page = 1
            page_size = pagination.get("page_size", 100)
            page_key = pagination.get("page_key", "page")
            size_key = pagination.get("size_key", "pageSize")
            stop_condition = pagination.get("stop_condition", "no_data")
            
            while True:
                params = {
                    **base_params,
                    page_key: page,
                    size_key: page_size
                }
                
                logger.info(f"Fetching page {page} from {url}...")
                response = self._make_request(
                    method=method,
                    url=url,
                    headers=headers,
                    params=params,
                    cert=self.cert
                )
                
                json_resp = response.json()
                if post_process:
                    json_resp = self._run_post_process(json_resp, post_process)
                
                records = self._process_response_data(json_resp, data_extractor)
                all_records.extend(records)
                
                # Check stop condition
                if stop_condition == "no_data" and len(records) == 0:
                    break
                elif stop_condition == "no_data" and len(records) < page_size:
                    # If we got fewer records than a full page, it's the last page
                    break
                    
                page += 1
                
        elif pag_type == "offset_based":
            offset = pagination.get("start_offset", 0)
            page_size = pagination.get("page_size", 50)
            offset_key = pagination.get("offset_key", "$skip")
            size_key = pagination.get("size_key", "$top")
            stop_condition = pagination.get("stop_condition", "no_data")
            
            while True:
                params = {
                    **base_params,
                    offset_key: offset,
                    size_key: page_size
                }
                
                logger.info(f"Fetching offset {offset} from {url}...")
                response = self._make_request(
                    method=method,
                    url=url,
                    headers=headers,
                    params=params,
                    cert=self.cert
                )
                
                json_resp = response.json()
                if post_process:
                    json_resp = self._run_post_process(json_resp, post_process)
                
                records = self._process_response_data(json_resp, data_extractor)
                all_records.extend(records)
                
                # Check stop condition
                if stop_condition == "no_data" and len(records) == 0:
                    break
                elif stop_condition == "no_data" and len(records) < page_size:
                    # If we got fewer records than requested, it's the last pull
                    break
                    
                offset += page_size

        elif pag_type == "cursor_based":
            cursor_path = pagination.get("cursor_path")
            cursor_query_key = pagination.get("cursor_query_key", "cursor")
            stop_condition = pagination.get("stop_condition", "no_cursor")
            
            if not cursor_path:
                raise ValueError("cursor_based pagination requires 'cursor_path' to be defined in config.")
                
            cursor_value = None
            
            while True:
                params = base_params.copy()
                if cursor_value:
                    params[cursor_query_key] = cursor_value
                    
                logger.info(f"Fetching cursor '{cursor_value or 'INITIAL'}' from {url}...")
                response = self._make_request(
                    method=method,
                    url=url,
                    headers=headers,
                    params=params,
                    cert=self.cert
                )
                
                json_resp = response.json()
                if post_process:
                    json_resp = self._run_post_process(json_resp, post_process)
                
                records = self._process_response_data(json_resp, data_extractor)
                all_records.extend(records)
                
                # Extract cursor
                cursor_value = extract_data(json_resp, cursor_path)
                
                if stop_condition == "no_cursor" and not cursor_value:
                    break

        elif not pag_type:
            # No pagination
            logger.info(f"Fetching data from {url}...")
            response = self._make_request(
                method=method,
                url=url,
                headers=headers,
                params=base_params,
                cert=self.cert
            )
            
            json_resp = response.json()
            if post_process:
                json_resp = self._run_post_process(json_resp, post_process)
            
            records = self._process_response_data(json_resp, data_extractor)
            all_records.extend(records)
        else:
            raise NotImplementedError(f"Pagination type '{pag_type}' is not yet implemented.")
            
        extract_config = api_def.get("json_extract_config")
        
        # If user explicitly requests raw, or there's no extract config, return the stitched raw JSON
        if output_format == "raw" or not extract_config:
            if output_format == "raw":
                logger.info("Returning raw JSON output as requested (bypassing json-extract-pandas).")
            final_output = all_records
            df = None
        else:
            # Otherwise, apply json-extract-pandas if it is installed
            if extract_json is None:
                logger.warning("Warning: json_extract_pandas is not installed. Returning raw stitched JSON list.")
                final_output = all_records
                df = None
            else:
                logger.info(f"Applying json_extract_pandas with config: {extract_config}")
                meta, df = extract_json(all_records, **extract_config)
                final_output = df

        # --- Handle Chained Requests ---
        chained_request = api_def.get("chained_request")
        if chained_request:
            child_api_id = chained_request.get("child_api_identifier")
            key_mapping = chained_request.get("key_mapping", {})
            max_concurrent_requests = chained_request.get("max_concurrent_requests", 1)
            batch_size = chained_request.get("batch_size", 1)
            
            param_combinations = []
            
            if df is not None:
                missing_cols = [col for col in key_mapping.keys() if col not in df.columns]
                if missing_cols:
                    logger.error(f"Chained request failed: Missing columns {missing_cols} in dataframe.")
                else:
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
                # Apply Batching
                batched_combinations = []
                for i in range(0, len(param_combinations), batch_size):
                    chunk = param_combinations[i:i + batch_size]
                    if batch_size > 1:
                        # Merge dicts by joining values with commas
                        merged_combo = {}
                        for k in key_mapping.values():
                            merged_combo[k] = ",".join([c[k] for c in chunk])
                        batched_combinations.append(merged_combo)
                    else:
                        batched_combinations.append(chunk[0])

                logger.info(f"Initiating chained request to '{child_api_id}' with {len(batched_combinations)} batched parameter combinations (Concurrency: {max_concurrent_requests})...")
                child_results = []
                
                def _fetch_child(combo):
                    try:
                        child_path_params = path_params.copy() if path_params else {}
                        child_path_params.update(combo)
                        
                        child_res = self.execute(
                            api_identifier=child_api_id,
                            path_params=child_path_params,
                            query_params=query_params,
                            output_format=output_format
                        )
                        
                        # --- Lineage Injection ---
                        # If batch_size > 1, injecting lineage is complex because 1 request returns N items.
                        # We will inject the batched combo strings. For true granular lineage on batched calls,
                        # the child API response should inherently contain the IDs.
                        if output_format == "raw" or df is None:
                            if isinstance(child_res, list):
                                for item in child_res:
                                    if isinstance(item, dict):
                                        for k, v in combo.items():
                                            item[f"_parent_batch_{k}"] = v
                            elif isinstance(child_res, dict):
                                for k, v in combo.items():
                                    child_res[f"_parent_batch_{k}"] = v
                        else:
                            if child_res is not None and not child_res.empty:
                                for k, v in combo.items():
                                    child_res[f"_parent_batch_{k}"] = v
                                    
                        return child_res
                    except Exception as e:
                        logger.error(f"Chained request failed for {combo}: {e}")
                        return None

                if max_concurrent_requests > 1:
                    with concurrent.futures.ThreadPoolExecutor(max_workers=max_concurrent_requests) as executor:
                        results = list(executor.map(_fetch_child, batched_combinations))
                        for res in results:
                            if res is not None:
                                if isinstance(res, list) and (output_format == "raw" or df is None):
                                    child_results.extend(res)
                                else:
                                    child_results.append(res)
                else:
                    for combo in batched_combinations:
                        res = _fetch_child(combo)
                        if res is not None:
                            if isinstance(res, list) and (output_format == "raw" or df is None):
                                child_results.extend(res)
                            else:
                                child_results.append(res)
                        
                # Merge the child results
                if output_format == "raw" or df is None:
                    return child_results
                else:
                    import pandas as pd
                    valid_dfs = [cr for cr in child_results if cr is not None and not cr.empty]
                    if valid_dfs:
                        return pd.concat(valid_dfs, ignore_index=True)
                    return pd.DataFrame()
            else:
                logger.warning("Chained request skipped: No valid keys extracted.")
                
        # --- Handle State Tracking ---
        state_tracking = api_def.get("state_tracking")
        if state_tracking:
            incremental_key = state_tracking.get("incremental_key")
            max_val = None
            if df is not None and incremental_key in df.columns:
                # Basic string/number max calculation
                max_val = df[incremental_key].dropna().max()
            elif isinstance(final_output, list):
                vals = [rec.get(incremental_key) for rec in final_output if isinstance(rec, dict) and incremental_key in rec and rec.get(incremental_key) is not None]
                if vals:
                    max_val = max(vals)
                    
            if max_val is not None:
                self.state[incremental_key] = str(max_val)
                self._save_state()
                logger.info(f"Updated state watermark '{incremental_key}' to: {max_val}")
                
        # --- Handle Export Targets ---
        export_target = api_def.get("export_target")
        if export_target and df is not None:
            self._export_dataframe(df, export_target)
            
        return final_output

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Apiphany Execution Engine")
    parser.add_argument("--config", required=True, help="Path to JSON configuration file")
    parser.add_argument("--entity", required=True, help="Name of the entity to execute (e.g. 'adp', 'hibob', 'rick_and_morty')")
    parser.add_argument("--api", type=str, required=True, help="The api_identifier to execute (e.g. 'get_employee_data')")
    parser.add_argument("--output", type=str, choices=["auto", "raw"], default="auto", help="Output format ('auto' for Pandas dataframe, 'raw' for stitched JSON)")
    parser.add_argument("--outfile", type=str, help="Path to save the output file (e.g., output.csv or output.json)")
    parser.add_argument("--path-params", type=str, help="JSON string of dynamic path parameters (e.g. '{\"currenttimecardguid\": \"12345\"}')")
    parser.add_argument("--query-params", type=str, help="JSON string of dynamic query parameters (e.g. '{\"limit\": 5}')")
    parser.add_argument("--client-credentials", type=str, help="JSON string of runtime credentials (e.g. '{\"client_id\": \"...\", \"client_secret\": \"...\"}')")
    
    args = parser.parse_args()
    
    # Parse dynamic JSON parameters if provided
    path_params = json.loads(args.path_params) if args.path_params else None
    query_params = json.loads(args.query_params) if args.query_params else None
    runtime_credentials = json.loads(args.client_credentials) if args.client_credentials else None
    
    try:
        client = APIOrchestrator(
            config_file=args.config,
            entity_name=args.entity,
            client_credentials=runtime_credentials
        )
        
        logger.info(f"Loaded config for entity: {client.entity_name}")
        logger.info(f"Executing API: {args.api}...")
        
        result = client.execute(
            args.api, 
            path_params=path_params,
            query_params=query_params,
            output_format=args.output
        )
        
        # Automatic File Output Management
        if args.outfile:
            if args.output == "raw":
                with open(args.outfile, "w") as f:
                    json.dump(result, f, indent=4)
                logger.info(f"Successfully saved {len(result)} raw records to {args.outfile}")
            else:
                # Assuming result is a Pandas DataFrame
                result.to_csv(args.outfile, index=False)
                logger.info(f"Successfully saved {len(result)} rows to {args.outfile}")
        else:
            logger.info("Execution complete! Use --outfile to save results.")
            if args.output == "auto":
                print(result.head())
            else:
                logger.info(f"Total raw records: {len(result)}")
                
    except Exception as e:
        logger.error(f"Error during execution: {e}")
        sys.exit(1)
