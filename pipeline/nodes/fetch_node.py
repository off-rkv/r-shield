"""
pipeline/nodes/fetch_node.py

Node 1 — Fetch
Pulls user post history, comment history, and existing Neo4j profile.
Sets: post_history, comment_history, image_base64, existing_neo4j
"""

import praw
import base64
import httpx
import os
from datetime import datetime, timezone
from pipeline.state import RShieldState

# ── Reddit API client ────────────────────────────────────────────
# Set these in .env or environment variables
# You need a Reddit app at https://www.reddit.com/prefs/apps
def get_reddit_client() -> praw.Reddit:
    return praw.Reddit(
        client_id     = os.getenv("REDDIT_CLIENT_ID",     "YOUR_CLIENT_ID"),
        client_secret = os.getenv("REDDIT_CLIENT_SECRET", "YOUR_CLIENT_SECRET"),
        user_agent    = "r-shield:v1.0 (by u/DifficultyLeast2323)",
        username      = os.getenv("REDDIT_USERNAME",      "YOUR_BOT_USERNAME"),
        password      = os.getenv("REDDIT_PASSWORD",      "YOUR_BOT_PASSWORD"),
    )

# ── Neo4j client ─────────────────────────────────────────────────
NEO4J_URI      = os.getenv("NEO4J_URI",      "bolt://localhost:7687")
NEO4J_USER     = os.getenv("NEO4J_USER",     "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

def get_neo4j_driver():
    try:
        from neo4j import GraphDatabase
        return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    except Exception as e:
        print(f"[fetch_node] Neo4j connection failed: {e}")
        return None


# ── MAIN NODE FUNCTION ────────────────────────────────────────────
def fetch_node(state: RShieldState) -> RShieldState:
    """
    Input:  state with raw_author, raw_post, raw_subreddit
    Output: state + post_history, comment_history, image_base64, existing_neo4j
    """
    username   = state.get("raw_author", {}).get("username", "")
    post_data  = state.get("raw_post",   {})

    print(f"[fetch_node] Fetching data for u/{username}")

    # ── 1. Fetch post history ────────────────────────────────────
    post_history = []
    try:
        reddit = get_reddit_client()
        redditor = reddit.redditor(username)

        for submission in redditor.submissions.new(limit=100):
            post_history.append({
                "id":          submission.id,
                "title":       submission.title,
                "body":        submission.selftext or "",
                "subreddit":   str(submission.subreddit),
                "url":         submission.url or "",
                "external_link": submission.url if not submission.is_self else None,
                "score":       submission.score,
                "num_comments":submission.num_comments,
                "num_reports": getattr(submission, "num_reports", 0) or 0,
                "created_utc": int(submission.created_utc),
                "created_at":  datetime.fromtimestamp(
                                   submission.created_utc, tz=timezone.utc
                               ).strftime("%H:%M:%S"),
                "is_self":     submission.is_self,
                "removed":     submission.removed_by_category is not None,
                "deleted":     submission.selftext == "[deleted]",
                "flair":       submission.link_flair_text or "",
            })

        print(f"[fetch_node]   Posts fetched: {len(post_history)}")

    except Exception as e:
        print(f"[fetch_node] Post history fetch failed: {e}")
        # Use empty list — parser node handles missing history gracefully

    # ── 2. Fetch comment history ──────────────────────────────────
    comment_history = []
    try:
        reddit = get_reddit_client()
        redditor = reddit.redditor(username)

        for comment in redditor.comments.new(limit=100):
            comment_history.append({
                "id":         comment.id,
                "body":       comment.body or "",
                "subreddit":  str(comment.subreddit),
                "score":      comment.score,
                "created_utc":int(comment.created_utc),
                "post_id":    comment.link_id,
            })

        print(f"[fetch_node]   Comments fetched: {len(comment_history)}")

    except Exception as e:
        print(f"[fetch_node] Comment history fetch failed: {e}")

    # ── 3. Fetch image if present ─────────────────────────────────
    image_base64 = None
    media_urls   = post_data.get("media_urls", [])
    post_url     = post_data.get("url", "")

    # Check if post has an image
    image_url = None
    if media_urls:
        image_url = media_urls[0]
    elif post_url and any(post_url.endswith(ext) for ext in [".jpg",".jpeg",".png",".webp"]):
        image_url = post_url
    elif "i.redd.it" in post_url or "i.imgur.com" in post_url:
        image_url = post_url

    if image_url:
        try:
            async_client = httpx.Client(timeout=10)
            response = async_client.get(image_url)
            if response.status_code == 200:
                image_base64 = base64.b64encode(response.content).decode("utf-8")
                print(f"[fetch_node]   Image fetched: {len(image_base64)} chars")
        except Exception as e:
            print(f"[fetch_node] Image fetch failed: {e}")

    # ── 4. Check Neo4j for existing profile ───────────────────────
    existing_neo4j = None
    try:
        driver = get_neo4j_driver()
        if driver:
            with driver.session() as session:
                result = session.run(
                    "MATCH (u:User {username: $username}) RETURN u",
                    username=username
                )
                record = result.single()
                if record:
                    existing_neo4j = dict(record["u"])
                    print(f"[fetch_node]   Existing Neo4j profile found — risk: {existing_neo4j.get('overall_risk','?')}")
                else:
                    print(f"[fetch_node]   New user — no existing Neo4j profile")
            driver.close()
    except Exception as e:
        print(f"[fetch_node] Neo4j query failed: {e}")

    # ── 5. Return updated state ───────────────────────────────────
    return {
        **state,
        "post_history":    post_history,
        "comment_history": comment_history,
        "image_base64":    image_base64,
        "existing_neo4j":  existing_neo4j,
    }