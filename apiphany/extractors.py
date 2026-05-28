from typing import Any

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
