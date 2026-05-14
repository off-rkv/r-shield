"""
pipeline/nodes/action_node.py

Node 5 — Action
Score-based decision engine.
Reads Qwen output + Neo4j result.
Produces final_action, final_tier, score, mod_mail_text.
"""

import json
import redis
import os
from datetime import datetime, timezone
from pipeline.state import RShieldState

r = None
try:
    r = redis.Redis(host=os.getenv("REDIS_HOST","localhost"),
                   port=int(os.getenv("REDIS_PORT","6379")),
                   decode_responses=True)
    r.ping()
except Exception:
    r = None


# ── MAIN NODE FUNCTION ────────────────────────────────────────────
def action_node(state: RShieldState) -> RShieldState:
    qwen   = state.get("qwen_output", {})
    neo4j  = state.get("neo4j_result", {})

    username = state.get("raw_author", {}).get("username", "")
    post_id  = state.get("raw_post",   {}).get("id", "")

    print(f"[action_node] Computing decision for u/{username}")

    # ── Read pipeline settings from Redis ────────────────────────
    settings = {}
    if r:
        raw = r.get("rg:pipeline_settings")
        if raw:
            try: settings = json.loads(raw)
            except Exception: pass

    ban_threshold  = settings.get("score_ban_threshold",  85)
    warn_threshold = settings.get("score_warn_threshold", 45)
    watch_threshold= 25
    score_auto_on  = settings.get("score_auto", False)
    qwen_auto_on   = settings.get("qwen_auto",  False)
    pipeline_paused= settings.get("pipeline_paused", False)

    # If pipeline paused — everything goes to review
    if pipeline_paused:
        return build_result(state, "REVIEW", "REVIEW", 0,
                           "Pipeline paused — human review required", username, post_id)

    # ── COMPUTE SCORE ─────────────────────────────────────────────
    score = 0
    score_breakdown = []

    dbscan     = qwen.get("dbscan_vector", {})
    counter_ev = qwen.get("counter_evidence", {})
    action_rec = qwen.get("action_recommendation", {})
    image_anal = qwen.get("image_analysis", {})
    signals    = qwen.get("signal_attribution", [])

    # Overall risk (+0 to +40)
    risk_pts = {"NONE":0,"VERY_LOW":5,"LOW":10,"MEDIUM":20,
                "HIGH":30,"VERY_HIGH":35,"CRITICAL":40}
    rp = risk_pts.get(dbscan.get("overall_risk","NONE"), 0)
    score += rp
    if rp: score_breakdown.append(f"overall_risk={dbscan.get('overall_risk','?')} +{rp}")

    # Bot confidence (+0 to +20)
    bot_pts = {"NONE":0,"LOW":5,"MEDIUM":10,"HIGH":15,"VERY_HIGH":20}
    bp = bot_pts.get(dbscan.get("bot_confidence","NONE"), 0)
    score += bp
    if bp: score_breakdown.append(f"bot_confidence={dbscan.get('bot_confidence','?')} +{bp}")

    # Template usage confirmed (+8)
    if dbscan.get("template_usage") == "CONFIRMED":
        score += 8
        score_breakdown.append("template_usage=CONFIRMED +8")

    # Burst intensity extreme (+7)
    burst_pts = {"EXTREME":7,"HIGH":5,"MEDIUM":3,"LOW":1,"NONE":0}
    bup = burst_pts.get(dbscan.get("burst_intensity","NONE"), 0)
    score += bup
    if bup: score_breakdown.append(f"burst={dbscan.get('burst_intensity','?')} +{bup}")

    # External link ratio (+0 to +5)
    link_pts = {"ALWAYS":5,"MOSTLY":4,"SOMETIMES":2,"RARELY":1,"NEVER":0}
    lp = link_pts.get(dbscan.get("external_link_ratio","NEVER"), 0)
    score += lp
    if lp: score_breakdown.append(f"ext_link_ratio={dbscan.get('external_link_ratio','?')} +{lp}")

    # Reply behavior none (+3)
    if dbscan.get("reply_behavior") == "NONE":
        score += 3
        score_breakdown.append("reply_behavior=NONE +3")

    # Image: QR matches post link (+15) — strongest image signal
    if image_anal.get("qr_matches_post_link"):
        score += 15
        score_breakdown.append("qr_matches_post_link=True +15")
    elif image_anal.get("verdict") == "FAKE_PROFIT_SCREENSHOT":
        score += 10
        score_breakdown.append("fake_profit_screenshot +10")

    # Community rejection (+5)
    if dbscan.get("community_rejection") == "STRONG":
        score += 5
        score_breakdown.append("community_rejection=STRONG +5")

    # Account age brand new (+3)
    if dbscan.get("account_age") in ("BRAND_NEW","VERY_NEW"):
        score += 3
        score_breakdown.append(f"account_age={dbscan.get('account_age','?')} +3")

    # ── NETWORK AMPLIFIER ─────────────────────────────────────────
    banned_connections = state.get("banned_connections", 0) or neo4j.get("banned_connections", 0)
    net_pts = min(banned_connections * 10, 30)
    score += net_pts
    if net_pts: score_breakdown.append(f"banned_connections={banned_connections} +{net_pts}")

    # All DBSCAN cluster members banned (+10)
    if state.get("all_cluster_banned") or neo4j.get("dbscan_result",{}).get("all_members_banned"):
        score += 10
        score_breakdown.append("all_cluster_banned=True +10")

    # ── FALSE POSITIVE PENALTY ────────────────────────────────────
    fp_penalties = {"VERY_LOW":0,"LOW":-5,"MEDIUM":-20,"HIGH":-40,"VERY_HIGH":-60}
    fp_risk = counter_ev.get("false_positive_risk","MEDIUM")
    fp_pen  = fp_penalties.get(fp_risk, -20)
    score  += fp_pen
    if fp_pen: score_breakdown.append(f"fp_risk={fp_risk} {fp_pen}")

    # Clamp score to 0 minimum
    score = max(0, score)

    print(f"[action_node]   Score: {score}")
    print(f"[action_node]   Breakdown: {' | '.join(score_breakdown)}")

    # ── DETERMINE ACTION ──────────────────────────────────────────
    if score >= ban_threshold:
        action = "REMOVE_AND_BAN"
    elif score >= warn_threshold:
        action = "REMOVE_AND_WARN"
    elif score >= watch_threshold:
        action = "WATCHLIST"
    else:
        action = "NO_ACTION"

    # ── DETERMINE TIER ───────────────────────────────────────────
    # Score auto mode: IMMEDIATE for high scores
    if score_auto_on and score >= ban_threshold and fp_risk in ("VERY_LOW","LOW"):
        tier = "IMMEDIATE"
    # Qwen auto mode: use Qwen's tier recommendation
    elif qwen_auto_on and action_rec.get("tier") == "IMMEDIATE" and fp_risk in ("VERY_LOW","LOW"):
        tier = "IMMEDIATE"
    # High FP risk always goes to review regardless
    elif fp_risk in ("HIGH","VERY_HIGH"):
        tier = "REVIEW"
    # Medium scores go to review
    elif score >= warn_threshold:
        tier = "REVIEW"
    else:
        tier = "LOG_ONLY"

    # ── HARD OVERRIDES ────────────────────────────────────────────
    # Never auto-ban if FP risk is HIGH or VERY_HIGH
    if fp_risk in ("HIGH","VERY_HIGH") and tier == "IMMEDIATE":
        tier = "REVIEW"

    # If REVIEW tier → save to hold queue
    if tier == "REVIEW" and r:
        hold_item = {
            "username":    username,
            "post_id":     post_id,
            "score":       score,
            "qwen_output": qwen,
            "neo4j_result":neo4j,
            "timestamp":   datetime.now(timezone.utc).isoformat(),
        }
        r.lpush("rg:hold_queue", json.dumps(hold_item))
        r.ltrim("rg:hold_queue", 0, 199)
        print(f"[action_node]   Added to hold queue for human review")

    # ── BUILD MOD MAIL ────────────────────────────────────────────
    reasoning = qwen.get("reasoning","")
    network_info = ""
    if banned_connections > 0:
        network_info = f"\n\nNetwork: {banned_connections} banned accounts share the same domain."

    signals_text = ""
    for sig in signals:
        signals_text += f"\n  - {sig.get('signal','?')}: {sig.get('combined_strength','?')}"

    mod_mail = f"""r-shield Auto-Action Report
User: u/{username}
Action: {action}
Score: {score}/100
Tier: {tier}

Reason: {reasoning}{network_info}

Signals:{signals_text}

Score breakdown: {' | '.join(score_breakdown)}
"""

    # ── ADDITIONAL ACTIONS ────────────────────────────────────────
    additional = list(action_rec.get("additional_actions", []))

    # Block image hash if fake screenshot confirmed
    img_hash = image_anal.get("perceptual_hash")
    if img_hash and image_anal.get("verdict") == "FAKE_PROFIT_SCREENSHOT":
        additional.append(f"BLOCK_IMAGE_HASH: {img_hash}")
        if r: r.sadd("rg:blocked_hashes", img_hash)

    # Block domains
    for edge in qwen.get("neo4j",{}).get("edges",[]):
        if edge.get("type") == "POSTED_LINK":
            domain = edge.get("target","")
            if domain and score >= ban_threshold:
                additional.append(f"ADD_DOMAIN_TO_AUTOMOD: {domain}")
                if r: r.sadd("rg:blocked_domains", domain)

    # Mark user as banned in Neo4j if action is ban
    if action == "REMOVE_AND_BAN" and tier == "IMMEDIATE":
        try:
            from neo4j import GraphDatabase
            driver = GraphDatabase.driver(
                os.getenv("NEO4J_URI","bolt://localhost:7687"),
                auth=(os.getenv("NEO4J_USER","neo4j"), os.getenv("NEO4J_PASSWORD","password"))
            )
            with driver.session() as session:
                session.run("MATCH (u:User {username:$u}) SET u.banned=true, u.ban_time=timestamp()",
                           u=username)
            driver.close()
        except Exception as e:
            print(f"[action_node] Neo4j ban flag failed: {e}")

    print(f"[action_node]   Final: {action} | tier: {tier} | score: {score}")

    return build_result(state, action, tier, score, reasoning, username, post_id,
                       mod_mail, additional)


# ── HELPERS ───────────────────────────────────────────────────────
def build_result(state, action, tier, score, reasoning, username, post_id,
                 mod_mail="", additional=None):
    return {
        **state,
        "final_action":      action,
        "final_tier":        tier,
        "score":             score,
        "reasoning_summary": reasoning[:200] if reasoning else "",
        "mod_mail_text":     mod_mail,
        "additional_actions":additional or [],
        "ban_duration":      0,
        "ban_message":       "Your account has been banned for violating subreddit rules.",
    }