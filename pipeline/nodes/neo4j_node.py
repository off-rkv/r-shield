"""
neo4j_node.py — stub for initial testing
"""
from pipeline.state import RShieldState

def neo4j_node(state: RShieldState) -> RShieldState:
    print("[neo4j_node] Skipped — not configured yet")
    return {
        **state,
        "neo4j_result":       {},
        "linked_users":       [],
        "banned_connections":  0,
        "network_confirmed":  False,
        "all_cluster_banned": False,
        "dbscan_cluster_id":  -1,
    }