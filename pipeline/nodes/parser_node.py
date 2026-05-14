"""
pipeline/nodes/parser_node.py

Node 2 — Parser
Takes raw fetched data, computes all behavioral signals,
assembles clean input schema v3.0.0 ready for Qwen.
Sets: parsed_input + all timing/behavior quick-access fields
"""

import re
import math
from datetime import datetime, timezone
from collections import Counter
from pipeline.state import RShieldState


# ── MAIN NODE FUNCTION ────────────────────────────────────────────
def parser_node(state: RShieldState) -> RShieldState:
    """
    Input:  state with post_history, comment_history, raw_post, raw_author
    Output: state + parsed_input + timing signals
    """
    raw_author   = state.get("raw_author",   {})
    raw_post     = state.get("raw_post",     {})
    raw_subreddit= state.get("raw_subreddit",{})
    post_history = state.get("post_history", [])
    comment_history = state.get("comment_history", [])
    existing     = state.get("existing_neo4j")
    image_b64    = state.get("image_base64")

    username = raw_author.get("username", "")
    print(f"[parser_node] Parsing u/{username} — {len(post_history)} posts, {len(comment_history)} comments")

    # ── 1. Sort post history by timestamp ────────────────────────
    posts_sorted = sorted(post_history, key=lambda p: p.get("created_utc", 0))

    # ── 2. Timing analysis ────────────────────────────────────────
    timestamps = [p["created_utc"] for p in posts_sorted if "created_utc" in p]
    timing     = compute_timing(timestamps, raw_author)

    # ── 3. Behavioral signals ─────────────────────────────────────
    behavior   = compute_behavior(posts_sorted, comment_history)

    # ── 4. Language analysis ──────────────────────────────────────
    language   = compute_language(posts_sorted, comment_history, raw_post)

    # ── 5. PII check ──────────────────────────────────────────────
    all_text = " ".join([
        raw_post.get("title",""),
        raw_post.get("body",""),
        *[p.get("body","") for p in posts_sorted[-10:]],
        *[c.get("body","") for c in comment_history[-10:]],
    ])
    pii = check_pii(all_text)

    # ── 6. Build post history sequence for Qwen ───────────────────
    history_sequence = []
    for i, post in enumerate(posts_sorted[-20:]):  # last 20 posts
        prev_ts = posts_sorted[i-1]["created_utc"] if i > 0 else None
        curr_ts = post.get("created_utc")
        mins_after = None
        if prev_ts and curr_ts:
            mins_after = round((curr_ts - prev_ts) / 60, 1)

        history_sequence.append({
            "sequence":             i + 1,
            "title":                post.get("title",""),
            "body":                 post.get("body","")[:300],
            "subreddit":            f"r/{post.get('subreddit','')}",
            "posted_hour_utc":      post.get("created_at",""),
            "minutes_after_previous": mins_after,
            "external_link":        post.get("external_link"),
            "community_reception":  "downvoted" if post.get("score",0) < 0 else "upvoted",
            "replies_received":     post.get("num_comments", 0),
        })

    # ── 7. Assemble input schema v3.0.0 ───────────────────────────
    account_age_days = compute_account_age_days(raw_author)

    parsed_input = {
        "schema_version": "3.0.0",
        "trigger_event":  state.get("trigger_event", "PostSubmit"),

        "user": {
            "username":        username,
            "account_age":     format_account_age(account_age_days),
            "karma":           f"{raw_author.get('karma',0)} total",
            "reddit_premium":  raw_author.get("is_gold", False),
            "reddit_flagged":  raw_author.get("spam", False),
            "suspended":       raw_author.get("suspended", False),
            "bio":             raw_author.get("description") or None,
            "community_flair": raw_post.get("flair") or None,
        },

        "current_content": {
            "type":               "post",
            "title":              raw_post.get("title",""),
            "body":               raw_post.get("body",""),
            "external_link":      raw_post.get("url") if not raw_post.get("is_self") else None,
            "posted_hour_utc":    format_hour_utc(raw_post.get("created_at", 0)),
            "reports":            raw_post.get("num_reports", 0),
            "community_reception":"heavily downvoted" if raw_post.get("score",0) < -5 else
                                  "downvoted" if raw_post.get("score",0) < 0 else "neutral",
            "reply_count":        raw_post.get("num_comments", 0),
            "flair_used":         raw_post.get("flair") or None,
            "crowd_control_active": raw_post.get("crowdControlLevel", 0) > 0,
            "crossposted":        False,
            "self_deleted":       raw_post.get("deleted", False),
            "image":              "<raw image passed to Qwen vision>" if image_b64 else None,
            "possible_pii_detected": pii["detected"],
        },

        "post_history_sequence": history_sequence,

        "comment_history": [
            c.get("body","")[:200]
            for c in comment_history[-10:]
        ],

        "timing_analysis": timing,

        "behavior_summary": behavior,

        "language_context": language,

        "image_analysis_input": {
            "image_present":    image_b64 is not None,
            "raw_image":        image_b64,
            "parser_pre_check": "image present, passed to Qwen" if image_b64 else "no image",
        },

        "graph_history": {
            "previous_risk_level":              existing.get("overall_risk","none") if existing else "none",
            "previous_tags":                    [],
            "flagged_accounts_sharing_same_domain": "unknown — Neo4j will check",
            "cross_subreddit_coordination_detected": len(set(
                p.get("subreddit","") for p in posts_sorted
            )) > 2,
            "risk_trend":                       "returning user" if existing else "new account — first appearance",
        },
    }

    print(f"[parser_node] Input schema assembled — gap_std={timing.get('gap_standard_deviation','?')}, burst={behavior.get('posts_last_24hrs','?')}")

    return {
        **state,
        "parsed_input":       parsed_input,
        "avg_gap_seconds":    timing.get("_avg_gap_seconds"),
        "gap_std_deviation":  timing.get("gap_standard_deviation"),
        "burst_intensity":    behavior.get("_burst_intensity"),
        "vocab_diversity":    behavior.get("_vocab_diversity"),
        "external_link_ratio":behavior.get("_external_link_ratio"),
    }


# ── TIMING HELPERS ────────────────────────────────────────────────
def compute_timing(timestamps: list, raw_author: dict) -> dict:
    if len(timestamps) < 2:
        return {
            "post_timestamps_utc":          [],
            "average_gap_seconds":          "insufficient data",
            "gap_standard_deviation":       "insufficient data",
            "gap_pattern":                  "insufficient data",
            "first_post_after_account_creation": "unknown",
            "active_hours_utc":             [],
            "timezone_plausibility":        "unknown",
            "_avg_gap_seconds":             None,
        }

    gaps = [timestamps[i] - timestamps[i-1] for i in range(1, len(timestamps))]
    avg  = sum(gaps) / len(gaps)
    std  = math.sqrt(sum((g - avg)**2 for g in gaps) / len(gaps))

    # Classify std deviation
    if std < 15:
        std_label = "VERY_LOW — gaps nearly identical — automation indicator"
    elif std < 45:
        std_label = "LOW — fairly consistent posting rhythm"
    elif std < 120:
        std_label = "MEDIUM — somewhat irregular"
    else:
        std_label = "HIGH — irregular — human pattern"

    # Pattern label
    if std < 15:
        pattern = "rhythmic and consistent — automation indicator"
    elif std < 45:
        pattern = "fairly consistent — possible automation"
    else:
        pattern = "irregular — consistent with human behaviour"

    # Active hours
    active_hours = []
    for ts in timestamps[-20:]:
        hour = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%-Iam").lstrip("0")
        if hour not in active_hours:
            active_hours.append(hour)

    # Timezone plausibility
    hour_nums = [datetime.fromtimestamp(ts, tz=timezone.utc).hour for ts in timestamps]
    night_posts = sum(1 for h in hour_nums if 1 <= h <= 5)
    if night_posts / max(len(hour_nums), 1) > 0.7:
        tz_plausibility = "late night across entire US — weak human plausibility"
    else:
        tz_plausibility = "normal posting hours — consistent with human behaviour"

    # Burst in last 24h
    now = timestamps[-1] if timestamps else 0
    last_24h = sum(1 for ts in timestamps if now - ts <= 86400)

    ts_strings = [
        datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%H:%M:%S")
        for ts in timestamps[-10:]
    ]

    return {
        "post_timestamps_utc":          ts_strings,
        "average_gap_seconds":          f"{round(avg)} seconds",
        "gap_standard_deviation":       std_label,
        "gap_pattern":                  pattern,
        "first_post_after_account_creation": "unknown",
        "active_hours_utc":             active_hours[:6],
        "timezone_plausibility":        tz_plausibility,
        "_avg_gap_seconds":             round(avg, 1),
    }


def compute_behavior(posts: list, comments: list) -> dict:
    if not posts:
        return {
            "posts_last_24hrs":         "0 posts",
            "average_gap_between_posts":"unknown",
            "gap_consistency":          "insufficient data",
            "replies_to_others":        "unknown",
            "external_links_in_posts":  "unknown",
            "cross_subreddit_spam":     "unknown",
            "writing_variety":          "unknown",
            "edits_posts":              "unknown",
            "self_deleted_posts":       "0",
            "total_reports_received":   "0",
            "posts_removed_by_mods":    "0",
            "defensive_comments_detected": "no",
            "_burst_intensity":         "NONE",
            "_vocab_diversity":         "unknown",
            "_external_link_ratio":     "NEVER",
        }

    now = posts[-1]["created_utc"] if posts else 0
    last_24h = [p for p in posts if now - p.get("created_utc",0) <= 86400]

    # External links
    with_links = [p for p in posts if p.get("external_link")]
    link_ratio = len(with_links) / max(len(posts), 1)
    if link_ratio > 0.9:   link_label = "ALWAYS — all posts contain external links"
    elif link_ratio > 0.6: link_label = "MOSTLY — most posts contain external links"
    elif link_ratio > 0.3: link_label = "SOMETIMES"
    else:                  link_label = "RARELY"
    link_ratio_key = "ALWAYS" if link_ratio > 0.9 else "MOSTLY" if link_ratio > 0.6 else "SOMETIMES" if link_ratio > 0.3 else "RARELY" if link_ratio > 0.05 else "NEVER"

    # Vocab diversity
    all_words = " ".join(p.get("body","") + " " + p.get("title","") for p in posts[-30:]).lower().split()
    unique_ratio = len(set(all_words)) / max(len(all_words), 1)
    if unique_ratio < 0.15:   vocab_label = "very low — same phrases repeating"
    elif unique_ratio < 0.30: vocab_label = "low"
    elif unique_ratio < 0.50: vocab_label = "moderate"
    else:                     vocab_label = "high — diverse vocabulary"
    vocab_key = "VERY_LOW" if unique_ratio < 0.15 else "LOW" if unique_ratio < 0.30 else "MEDIUM" if unique_ratio < 0.50 else "HIGH"

    # Cross-subreddit
    subreddits = list(set(p.get("subreddit","") for p in posts))
    cross_sub  = ", ".join(f"r/{s}" for s in subreddits[:5]) if len(subreddits) > 2 else "minimal"

    # Replies
    reply_ratio = len([c for c in comments if c.get("body","")]) / max(len(posts), 1)
    replies_label = "frequently" if reply_ratio > 0.5 else "occasionally" if reply_ratio > 0.1 else "never"

    # Deleted posts
    deleted = len([p for p in posts if p.get("deleted")])

    # Removed by mods
    removed = len([p for p in posts if p.get("removed")])

    # Reports
    total_reports = sum(p.get("num_reports", 0) for p in posts)

    # Defensive comments
    defense_words = ["not a scam","trust me","legit","i promise","check my profile","this is real"]
    defensive = any(w in c.get("body","").lower() for c in comments[-20:] for w in defense_words)

    # Burst intensity
    burst_24h = len(last_24h)
    if burst_24h >= 20:     burst_key = "EXTREME"
    elif burst_24h >= 12:   burst_key = "HIGH"
    elif burst_24h >= 5:    burst_key = "MEDIUM"
    elif burst_24h >= 2:    burst_key = "LOW"
    else:                   burst_key = "NONE"

    # Gaps
    timestamps = [p["created_utc"] for p in posts if "created_utc" in p]
    if len(timestamps) >= 2:
        gaps = [timestamps[i] - timestamps[i-1] for i in range(1, len(timestamps))]
        avg  = sum(gaps) / len(gaps)
        std  = math.sqrt(sum((g - avg)**2 for g in gaps) / len(gaps))
        gap_label = f"{round(avg/60, 1)} minutes average"
        consistency = "very consistent — low human plausibility" if std < 15 else \
                      "fairly consistent" if std < 45 else \
                      "irregular — human pattern"
    else:
        gap_label    = "unknown"
        consistency  = "insufficient data"

    return {
        "posts_last_24hrs":         f"{burst_24h} posts",
        "average_gap_between_posts":gap_label,
        "gap_consistency":          consistency,
        "replies_to_others":        replies_label,
        "external_links_in_posts":  link_label,
        "cross_subreddit_spam":     cross_sub,
        "writing_variety":          vocab_label,
        "edits_posts":              "unknown",
        "self_deleted_posts":       str(deleted),
        "total_reports_received":   str(total_reports),
        "posts_removed_by_mods":    str(removed),
        "defensive_comments_detected": "yes" if defensive else "no",
        "_burst_intensity":         burst_key,
        "_vocab_diversity":         vocab_key,
        "_external_link_ratio":     link_ratio_key,
    }


def compute_language(posts: list, comments: list, raw_post: dict) -> dict:
    return {
        "detected_languages": ["English"],
        "primary":            "English",
        "code_switching":     False,
        "is_suspicious":      False,
        "note":               "Basic language detection — extend with langdetect if needed",
    }


def check_pii(text: str) -> dict:
    phone_pattern = re.compile(r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b')
    email_pattern = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
    detected = bool(phone_pattern.search(text) or email_pattern.search(text))
    return {"detected": detected}


def compute_account_age_days(raw_author: dict) -> int:
    created = raw_author.get("created_utc")
    if created:
        return max(0, int((datetime.now(timezone.utc).timestamp() - created) / 86400))
    return 0


def format_account_age(days: int) -> str:
    if days < 7:    return f"{days} days"
    elif days < 30: return f"{days // 7} weeks"
    elif days < 365:return f"{days // 30} months"
    else:           return f"{days // 365} years"


def format_hour_utc(created_at) -> str:
    try:
        if isinstance(created_at, int) and created_at > 0:
            dt = datetime.fromtimestamp(created_at / 1000 if created_at > 1e10 else created_at,
                                        tz=timezone.utc)
            return dt.strftime("%-I%p").lower()
    except Exception:
        pass
    return "unknown"