import argparse
import json
import sys
import logging
from apiphany import APIOrchestrator

logger = logging.getLogger("Apiphany")

def main():
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
                try:
                    print(result.head())
                except AttributeError:
                    print(result)
            else:
                logger.info(f"Total raw records: {len(result)}")
                
    except Exception as e:
        logger.error(f"Error during execution: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
