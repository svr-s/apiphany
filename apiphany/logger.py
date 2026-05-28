import logging
from pythonjsonlogger import jsonlogger

def get_logger(name: str = "Apiphany"):
    logger = logging.getLogger(name)
    
    # Only configure if no handlers exist
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        logHandler = logging.StreamHandler()
        # Ensure we capture typical fields in JSON
        formatter = jsonlogger.JsonFormatter('%(asctime)s %(levelname)s %(name)s %(message)s')
        logHandler.setFormatter(formatter)
        logger.addHandler(logHandler)
        
    return logger
