from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Dict, Any

@dataclass
class CostEstimate:
    runtime_seconds: int
    nodes: int
    node_hours: float
    rate_per_node_hour: float
    estimated_cost: float
    dbus: Optional[float] = None
    rate_per_dbu_hour: Optional[float] = None
    estimated_cost_dbu: Optional[float] = None

def estimate_cost(runtime_seconds: int, nodes: int, rate_per_node_hour: float = 0.0,
                  dbus_per_node: float = 0.0, rate_per_dbu_hour: float = 0.0) -> CostEstimate:
    runtime_seconds = int(max(0, runtime_seconds))
    nodes = int(max(1, nodes))
    node_hours = (runtime_seconds / 3600.0) * nodes
    estimated_cost = node_hours * float(rate_per_node_hour)

    dbus = None
    estimated_cost_dbu = None
    if dbus_per_node and rate_per_dbu_hour:
        dbus = (runtime_seconds / 3600.0) * nodes * float(dbus_per_node)
        estimated_cost_dbu = dbus * float(rate_per_dbu_hour)

    return CostEstimate(
        runtime_seconds=runtime_seconds,
        nodes=nodes,
        node_hours=node_hours,
        rate_per_node_hour=float(rate_per_node_hour),
        estimated_cost=estimated_cost,
        dbus=dbus,
        rate_per_dbu_hour=float(rate_per_dbu_hour) if rate_per_dbu_hour else None,
        estimated_cost_dbu=estimated_cost_dbu,
    )

def compare(current: CostEstimate, optimized: CostEstimate) -> Dict[str, Any]:
    return {
        "current_cost": current.estimated_cost,
        "optimized_cost": optimized.estimated_cost,
        "savings": current.estimated_cost - optimized.estimated_cost,
        "current_node_hours": current.node_hours,
        "optimized_node_hours": optimized.node_hours,
        "node_hours_saved": current.node_hours - optimized.node_hours,
    }
