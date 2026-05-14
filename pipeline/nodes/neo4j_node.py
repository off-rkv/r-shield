"""
pipeline/nodes/neo4j_node.py

Node 4 — Neo4j + DBSCAN
1. MERGE user node with 22 properties from dbscan_vector
2. Create edges from neo4j.edges block
3. Run spider web query — who shares same domains?
4. Run DBSCAN on all accumulated user vectors
Sets: neo4j_result + quick-access fields
"""

import os
import json
from pipeline.state import RShieldState

# ── Neo4j config ─────────────────────────────────────────────────
NEO4J_URI      = os.getenv("NEO4J_URI",      "bolt://localhost:7687")
NEO4J_USER     = os.getenv("NEO4J_USER",     "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

# ── DBSCAN encoding ───────────────────────────────────────────────
ENCODE = {
    "NONE": 0, "VERY_LOW": 1, "LOW": 2, "MEDIUM": 3,
    "HIGH": 4, "VERY_HIGH": 5, "CRITICAL": 6, "EXTREME": 7,
    "CONFIRMED": 8, "ALWAYS": 7, "MOSTLY": 5, "SOMETIMES": 3,
    "RARELY": 1, "NEVER": 0, "BRAND_NEW": 1, "VERY_NEW": 1,
    "NEW": 2, "MODERATE": 3, "OLD": 5, "VETERAN": 6,
    "NIGHT_EXCLUSIVE": 6, "NIGHT_HEAVY": 4, "MIXED": 2, "DAY_HEAVY": 1,
    "STRONG": 5, "WEAK": 2, "RAPIDLY_ESCALATING": 6, "ESCALATING": 4,
    "STABLE": 2, "DECLINING": 1, "YES": 6, "NO": 0,
}

DBSCAN_DIMS = [
    "overall_risk","bot_confidence","coordination","content_danger",
    "account_trust","template_usage","vocab_diversity","reply_behavior",
    "active_window","burst_intensity","network_risk","image_risk",
    "account_age","posting_speed","external_link_ratio","edit_behavior",
    "flagged_connections","subreddit_spread","risk_direction",
    "community_rejection","false_positive_risk","gap_deviation"
]


def get_driver():
    try:
        from neo4j import GraphDatabase
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        driver.verify_connectivity()
        return driver
    except Exception as e:
        print(f"[neo4j_node] Connection failed: {e}")
        return None


# ── MAIN NODE FUNCTION ────────────────────────────────────────────
def neo4j_node(state: RShieldState) -> RShieldState:
    qwen_output = state.get("qwen_output", {})
    username    = state.get("raw_author", {}).get("username", "")

    if not qwen_output:
        print("[neo4j_node] No Qwen output — skipping")
        return {**state, "neo4j_result": {}, "banned_connections": 0,
                "network_confirmed": False, "all_cluster_banned": False}

    dbscan_vec  = qwen_output.get("dbscan_vector", {})
    neo4j_block = qwen_output.get("neo4j", {})
    edges       = neo4j_block.get("edges", [])

    print(f"[neo4j_node] Updating graph for u/{username}")

    driver = get_driver()
    if not driver:
        print("[neo4j_node] No Neo4j connection — returning empty result")
        return {**state, "neo4j_result": {}, "banned_connections": 0,
                "network_confirmed": False, "all_cluster_banned": False,
                "linked_users": [], "dbscan_cluster_id": -1}

    linked_users    = []
    banned_connections = 0
    dbscan_result   = {}
    network_confirmed = False

    try:
        with driver.session() as session:

            # ── Step 1: MERGE user node ───────────────────────────
            props = {k: v for k, v in dbscan_vec.items()}
            props.update(neo4j_block.get("properties", {}))
            props["username"]         = username
            props["primary_label"]    = qwen_output.get("labels",{}).get("primary",{}).get("label","?")
            props["last_active_utc"]  = qwen_output.get("analyzed_at_utc","")
            props["node_type"]        = neo4j_block.get("node_type","USER")

            merge_query = """
            MERGE (u:User {username: $username})
            SET u += $props
            RETURN u
            """
            session.run(merge_query, username=username, props=props)
            print(f"[neo4j_node]   User node merged")

            # ── Step 2: Create edges ──────────────────────────────
            for edge in edges:
                edge_type = edge.get("type","")
                target    = edge.get("target","")
                weight    = edge.get("weight","MODERATE")
                times     = edge.get("times_posted", 1)
                co_occ    = edge.get("co_occurrences", 0)

                if edge_type == "POSTED_LINK" and target:
                    session.run("""
                    MATCH (u:User {username: $username})
                    MERGE (d:Domain {url: $target})
                    MERGE (u)-[r:POSTED_LINK]->(d)
                    SET r.weight = $weight,
                        r.times_posted = $times,
                        r.last_seen = timestamp()
                    """, username=username, target=target,
                         weight=weight, times=times)

                elif edge_type == "QR_LINKS_TO" and target:
                    session.run("""
                    MATCH (u:User {username: $username})
                    MERGE (d:Domain {url: $target})
                    MERGE (u)-[r:QR_LINKS_TO]->(d)
                    SET r.weight = $weight
                    """, username=username, target=target, weight=weight)

                elif edge_type == "SHARES_LINK_WITH" and target:
                    session.run("""
                    MATCH (u:User {username: $username})
                    MERGE (t:User {username: $target})
                    MERGE (u)-[r:SHARES_LINK_WITH]->(t)
                    SET r.weight = $weight,
                        r.co_occurrences = $co_occ
                    """, username=username, target=target,
                         weight=weight, co_occ=co_occ or 0)

            print(f"[neo4j_node]   {len(edges)} edges created")

            # ── Step 3: Spider web query ──────────────────────────
            # "Who else has posted the same domains as this user?"
            spider_result = session.run("""
            MATCH (u:User {username: $username})-[:POSTED_LINK]->(d:Domain)
            MATCH (other:User)-[:POSTED_LINK]->(d)
            WHERE other.username <> $username
            RETURN other.username AS username,
                   d.url AS domain,
                   other.overall_risk AS risk,
                   other.primary_label AS label,
                   COUNT(*) AS co_occurrences
            ORDER BY co_occurrences DESC
            LIMIT 20
            """, username=username)

            for record in spider_result:
                linked_users.append({
                    "username":       record["username"],
                    "domain":         record["domain"],
                    "risk":           record["risk"],
                    "label":          record["label"],
                    "co_occurrences": record["co_occurrences"],
                    "status":         get_user_status(session, record["username"]),
                })

            banned_connections = sum(1 for u in linked_users if u.get("status") == "BANNED")
            print(f"[neo4j_node]   Linked users: {len(linked_users)} — banned: {banned_connections}")

            # Confirm network if 2+ banned connections
            if banned_connections >= 2:
                network_confirmed = True
                # Create SHARES_LINK_WITH edges for confirmed network members
                for lu in linked_users[:5]:
                    session.run("""
                    MATCH (u:User {username: $username})
                    MATCH (t:User {username: $target})
                    MERGE (u)-[r:SHARES_LINK_WITH]->(t)
                    SET r.co_occurrences = $co_occ,
                        r.confirmed = true
                    """, username=username,
                         target=lu["username"],
                         co_occ=lu["co_occurrences"])

            # ── Step 4: DBSCAN clustering ─────────────────────────
            dbscan_result = run_dbscan(session, username, dbscan_vec)
            print(f"[neo4j_node]   DBSCAN cluster: {dbscan_result.get('cluster_id','?')}")

    except Exception as e:
        print(f"[neo4j_node] Error: {e}")
    finally:
        driver.close()

    neo4j_result = {
        "graph_updated":    True,
        "linked_users":     linked_users,
        "banned_connections": banned_connections,
        "network_confirmed":network_confirmed,
        "network_size":     len(linked_users) + 1,
        "dbscan_result":    dbscan_result,
        "network_verdict":  "COORDINATED_NETWORK_CONFIRMED" if network_confirmed else
                           "SUSPICIOUS_CONNECTIONS" if linked_users else
                           "NO_NETWORK_DETECTED",
    }

    return {
        **state,
        "neo4j_result":      neo4j_result,
        "linked_users":      linked_users,
        "banned_connections":banned_connections,
        "network_confirmed": network_confirmed,
        "dbscan_cluster_id": dbscan_result.get("cluster_id", -1),
        "all_cluster_banned":dbscan_result.get("all_members_banned", False),
    }


# ── DBSCAN ────────────────────────────────────────────────────────
def run_dbscan(session, username: str, current_vector: dict) -> dict:
    try:
        from sklearn.cluster import DBSCAN
        import numpy as np

        # Pull all user vectors from Neo4j
        result = session.run("""
        MATCH (u:User)
        WHERE u.overall_risk IS NOT NULL
        RETURN u.username AS username,
               u.overall_risk AS overall_risk,
               u.bot_confidence AS bot_confidence,
               u.burst_intensity AS burst_intensity,
               u.gap_deviation AS gap_deviation,
               u.template_usage AS template_usage,
               u.vocab_diversity AS vocab_diversity,
               u.reply_behavior AS reply_behavior,
               u.active_window AS active_window,
               u.account_trust AS account_trust,
               u.external_link_ratio AS external_link_ratio,
               u.image_risk AS image_risk,
               u.primary_label AS primary_label
        LIMIT 500
        """)

        records     = list(result)
        usernames   = [r["username"] for r in records]

        if len(records) < 3:
            return {"cluster_id": -1, "cluster_members": [],
                    "all_members_banned": False, "cluster_avg_risk": "unknown",
                    "note": "insufficient users for clustering"}

        # Encode vectors
        vectors = []
        for rec in records:
            v = [ENCODE.get(str(rec.get(dim,"NONE")).upper(), 0) for dim in DBSCAN_DIMS[:12]]
            vectors.append(v)

        vectors_np = np.array(vectors, dtype=float)

        # Run DBSCAN
        clustering = DBSCAN(eps=2.5, min_samples=3)
        labels     = clustering.fit_predict(vectors_np)

        # Find current user's cluster
        current_idx = usernames.index(username) if username in usernames else -1
        cluster_id  = int(labels[current_idx]) if current_idx >= 0 else -1

        # Find cluster members
        cluster_members = []
        if cluster_id >= 0:
            for i, (uname, label) in enumerate(zip(usernames, labels)):
                if int(label) == cluster_id and uname != username:
                    cluster_members.append(uname)

        # Check if all cluster members are banned
        all_banned = False
        if cluster_members:
            banned_count = 0
            for member in cluster_members[:10]:
                res = session.run("MATCH (u:User {username:$u}) RETURN u.banned AS banned",
                                  u=member)
                rec = res.single()
                if rec and rec["banned"]:
                    banned_count += 1
            all_banned = banned_count == len(cluster_members[:10])

        return {
            "cluster_id":        cluster_id,
            "cluster_members":   cluster_members[:10],
            "all_members_banned":all_banned,
            "cluster_avg_risk":  "CRITICAL" if cluster_id >= 0 else "unknown",
            "is_outlier":        cluster_id == -1,
            "total_clusters":    int(max(labels)) + 1 if max(labels) >= 0 else 0,
        }

    except ImportError:
        print("[neo4j_node] sklearn not installed — skipping DBSCAN")
        return {"cluster_id": -1, "is_outlier": True, "note": "sklearn not available"}
    except Exception as e:
        print(f"[neo4j_node] DBSCAN error: {e}")
        return {"cluster_id": -1, "is_outlier": True, "error": str(e)}


def get_user_status(session, username: str) -> str:
    try:
        res = session.run(
            "MATCH (u:User {username:$u}) RETURN u.banned AS banned, u.overall_risk AS risk",
            u=username
        )
        rec = res.single()
        if rec:
            if rec["banned"]: return "BANNED"
            risk = rec.get("risk","")
            if risk in ("CRITICAL","VERY_HIGH"): return "FLAGGED"
        return "ACTIVE"
    except Exception:
        return "UNKNOWN"