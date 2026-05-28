import json
import asyncio
import logging
from apiphany.orchestrator import APIOrchestrator

# Setup basic lambda logger (Apiphany uses python-json-logger internally for robust logging)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    AWS Lambda entrypoint for the Apiphany API Orchestration Engine.
    
    Expected Event Payload:
    {
        "config_file": "apiphany_config.json", // Optional, defaults to "apiphany_config.json"
        "entity_name": "jsonplaceholder",      // Required
        "api_identifier": "get_users"          // Required
    }
    """
    try:
        config_file = event.get("config_file", "apiphany_config.json")
        entity_name = event.get("entity_name")
        api_identifier = event.get("api_identifier")
        
        if not entity_name or not api_identifier:
            raise ValueError("Both 'entity_name' and 'api_identifier' must be provided in the event payload.")
            
        logger.info(f"Initializing Apiphany orchestrator for entity: '{entity_name}', target API: '{api_identifier}'")
        
        # 1. Initialize the Orchestrator
        orchestrator = APIOrchestrator(
            config_file=config_file,
            entity_name=entity_name
        )
        
        # 2. Execute the workflow synchronously in the Lambda environment
        # Apiphany handles all internal async concurrency, pagination, and data extraction
        df = asyncio.run(orchestrator.execute(
            api_identifier=api_identifier,
            output_format="flattened"
        ))
        
        records_processed = len(df) if df is not None else 0
        logger.info(f"Successfully processed {records_processed} records.")
        
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Apiphany execution completed successfully",
                "records_processed": records_processed,
                "entity_name": entity_name,
                "api_identifier": api_identifier
            })
        }
        
    except Exception as e:
        logger.error(f"Apiphany execution failed: {str(e)}", exc_info=True)
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Apiphany execution failed",
                "error": str(e)
            })
        }
