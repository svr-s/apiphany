import os
import json
from abc import ABC, abstractmethod
from typing import Dict, Any

class AbstractStateManager(ABC):
    @abstractmethod
    def load(self) -> Dict[str, Any]:
        pass

    @abstractmethod
    def save(self, state: Dict[str, Any]) -> None:
        pass

class LocalStateManager(AbstractStateManager):
    def __init__(self, file_path: str = "state.json"):
        self.file_path = file_path

    def load(self) -> Dict[str, Any]:
        if os.path.exists(self.file_path):
            try:
                with open(self.file_path, 'r') as f:
                    return json.load(f)
            except Exception:
                pass
        return {}

    def save(self, state: Dict[str, Any]) -> None:
        with open(self.file_path, 'w') as f:
            json.dump(state, f, indent=4)

class S3StateManager(AbstractStateManager):
    def __init__(self, s3_uri: str):
        self.s3_uri = s3_uri
        self.bucket = s3_uri.split('/')[2]
        self.key = '/'.join(s3_uri.split('/')[3:])
        try:
            import boto3
            self.s3_client = boto3.client('s3')
        except ImportError:
            raise ImportError("boto3 must be installed to use S3StateManager. Run pip install apiphany[aws]")

    def load(self) -> Dict[str, Any]:
        try:
            response = self.s3_client.get_object(Bucket=self.bucket, Key=self.key)
            return json.loads(response['Body'].read().decode('utf-8'))
        except Exception:
            return {}

    def save(self, state: Dict[str, Any]) -> None:
        self.s3_client.put_object(Bucket=self.bucket, Key=self.key, Body=json.dumps(state, indent=4))
