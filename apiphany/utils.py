import re
import os
from datetime import datetime, timedelta
from typing import Any, Dict

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
