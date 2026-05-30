import asyncio
from apiphany.orchestrator import APIOrchestrator

async def run_get_test():
    import os
    config_path = os.path.join(os.path.dirname(__file__), "test_get_config.json")
    
    # Initialize Orchestrator with the 'test_get' block from the config
    engine = APIOrchestrator(
        config_file=config_path,
        entity_name="test_get"
    )
    
    # Execute the GET request
    print("\n--- Running GET Request on JSONPlaceholder /users ---")
    results = await engine.execute("get_users")
    
    print("\nResults:")
    if getattr(results, 'head', None):
        print(results.head(2)) # It's a pandas dataframe
    else:
        print(results[:2]) # It's raw json

if __name__ == "__main__":
    asyncio.run(run_get_test())
