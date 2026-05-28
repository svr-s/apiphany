import json
from abc import ABC, abstractmethod
from typing import Dict, Any

class AbstractSecretProvider(ABC):
    @abstractmethod
    def fetch_secrets(self, identifier: str) -> Dict[str, Any]:
        pass

class AWSSecretProvider(AbstractSecretProvider):
    def __init__(self):
        try:
            import boto3
            self.sm_client = boto3.client('secretsmanager')
        except ImportError:
            raise ImportError("boto3 must be installed to use AWSSecretProvider. Run pip install apiphany[aws]")

    def fetch_secrets(self, identifier: str) -> Dict[str, Any]:
        try:
            secret_value = self.sm_client.get_secret_value(SecretId=identifier)
            if 'SecretString' in secret_value:
                return json.loads(secret_value['SecretString'])
            return {}
        except Exception as e:
            raise Exception(f"Failed to fetch secret {identifier} from AWS: {e}")
