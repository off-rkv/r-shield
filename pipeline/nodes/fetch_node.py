"""
fetch_node.py — simplified version (no PRAW)
Works with data already sent by Devvit trigger
"""
from pipeline.state import RShieldState

def fetch_node(state: RShieldState) -> RShieldState:
    raw_author = state.get("raw_author", {})
    username   = raw_author.get("username", "")

    print(f"[fetch_node] Processing u/{username}")
    print(f"[fetch_node] Using trigger payload only")

    return {
        **state,
        "post_history":    [],
        "comment_history": [],
        "image_base64":    None,
        "existing_neo4j":  None,
    }