from typing import Dict, List

import yaml
from fastapi import HTTPException


class ConfigValidationService:
    def __init__(self):
        self.alerts = []

    def load_config(self, config_path: str) -> Dict:
        """Load YAML configuration file"""
        try:
            with open(config_path, "r") as file:
                return yaml.safe_load(file)
        except Exception as e:
            raise HTTPException(
                status_code=500, detail=f"Failed to load config: {str(e)}"
            )

    def validate_config(
        self, config: Dict, expected_quota: int, expected_replication_factor: int
    ):
        """Validate quota and replication factor"""
        mismatches = []

        if config.get("quota") != expected_quota:
            mismatches.append(
                f"Quota mismatch: Expected {expected_quota}, Found {config.get('quota')}"
            )

        if config.get("replication_factor") != expected_replication_factor:
            mismatches.append(
                f"Replication factor mismatch: Expected {expected_replication_factor}, Found {config.get('replication_factor')}"
            )

        if mismatches:
            self.alerts.extend(mismatches)

    def get_alerts(self) -> List[str]:
        """Retrieve all validation alerts"""
        return self.alerts


def get_config_service():
    """Factory function for config validation service"""
    return ConfigValidationService()
