from __future__ import annotations
from typing import Any, Dict
from pydantic import BaseModel, Field

class ClusterSpec(BaseModel):
    nodes: int = Field(default=1, ge=1)
    cores_per_node: int = Field(default=4, ge=1)
    memory_gb_per_node: float = Field(default=16.0, gt=0)

class CostSpec(BaseModel):
    rate_per_node_hour: float = Field(default=0.0, ge=0.0)
    rate_per_dbu_hour: float = Field(default=0.0, ge=0.0)
    dbus_per_node: float = Field(default=0.0, ge=0.0)

class SparkConf(BaseModel):
    conf: Dict[str, Any] = Field(default_factory=dict)

    def get_int(self, key: str, default: int) -> int:
        try:
            return int(self.conf.get(key, default))
        except Exception:
            return default

    def get_bool(self, key: str, default: bool) -> bool:
        v = self.conf.get(key)
        if v is None:
            return default
        if isinstance(v, bool):
            return v
        s = str(v).strip().lower()
        if s in ("true", "1", "yes", "y"):
            return True
        if s in ("false", "0", "no", "n"):
            return False
        return default
