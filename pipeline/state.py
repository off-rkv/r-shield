"""
pipeline/state.py

Single shared state dictionary that flows through all 5 nodes.
Every node reads from it and adds its output to it.
Nothing is ever deleted — only added.
"""

from typing import TypedDict, Optional, Any


class RShieldState(TypedDict, total=False):

    # ── INPUT from Devvit (set before pipeline starts) ───────────
    schema_version:  str
    trigger_event:   str          # PostSubmit / CommentSubmit

    raw_post:        dict         # raw post object from Devvit
    raw_author:      dict         # raw author object from Devvit
    raw_subreddit:   dict         # raw subreddit object from Devvit
    raw_comment:     Optional[dict]

    # ── NODE 1 output: fetch_node ─────────────────────────────────
    post_history:       list      # last 100 posts by this user
    comment_history:    list      # last 100 comments by this user
    image_base64:       Optional[str]   # base64 encoded image if present
    existing_neo4j:     Optional[dict]  # existing Neo4j profile if user seen before

    # ── NODE 2 output: parser_node ────────────────────────────────
    parsed_input:    dict         # clean input schema v3.0.0 ready for Qwen

    # timing signals computed by parser
    avg_gap_seconds:    Optional[float]
    gap_std_deviation:  Optional[str]   # VERY_LOW / LOW / MEDIUM / HIGH
    burst_intensity:    Optional[str]   # EXTREME / HIGH / MEDIUM / LOW / NONE
    vocab_diversity:    Optional[str]   # VERY_LOW / LOW / MEDIUM / HIGH
    external_link_ratio: Optional[str] # ALWAYS / MOSTLY / SOMETIMES / RARELY / NEVER

    # ── NODE 3 output: qwen_node ──────────────────────────────────
    qwen_output:     dict         # full output schema v3.0.0

    # quick access fields from qwen_output
    primary_label:       Optional[str]
    primary_confidence:  Optional[str]
    authenticity_label:  Optional[str]
    false_positive_risk: Optional[str]
    overall_risk:        Optional[str]
    qwen_action:         Optional[str]  # Qwen's recommended action
    qwen_tier:           Optional[str]  # IMMEDIATE / REVIEW
    reasoning:           Optional[str]

    # ── NODE 4 output: neo4j_node ─────────────────────────────────
    neo4j_result:    dict         # graph update results + DBSCAN

    # quick access fields from neo4j_result
    linked_users:        Optional[list]   # accounts sharing same domain
    banned_connections:  Optional[int]    # how many linked users are banned
    dbscan_cluster_id:   Optional[int]    # cluster this user belongs to (-1 = outlier)
    all_cluster_banned:  Optional[bool]   # are all cluster members banned
    network_confirmed:   Optional[bool]   # coordinated network detected

    # ── NODE 5 output: action_node ────────────────────────────────
    final_action:        Optional[str]    # BAN / REMOVE / WARN / WATCHLIST / NO_ACTION
    final_tier:          Optional[str]    # IMMEDIATE / REVIEW / LOG_ONLY
    score:               Optional[int]    # 0-100+ composite score
    mod_mail_text:       Optional[str]
    additional_actions:  Optional[list]
    ban_duration:        Optional[int]    # days, 0 = permanent
    reasoning_summary:   Optional[str]   # short version for mod mail subject

    # ── META ──────────────────────────────────────────────────────
    error:           Optional[str]        # set if any node fails
    pipeline_ms:     Optional[int]        # total pipeline time in ms