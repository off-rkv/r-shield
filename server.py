"""
r-shield FastAPI backend
Run: uvicorn server:app --host 0.0.0.0 --port 8000 --reload

For local testing with Devvit:
  ngrok http 8000
  Then set FASTAPI_URL in triggers.ts to ngrok URL
"""

from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List, Any
import redis
import json
from datetime import datetime, timezone
from dotenv import load_dotenv
load_dotenv()

app = FastAPI(title="r-shield backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── REDIS ────────────────────────────────────────────────────────
try:
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    r.ping()
    print("Redis connected")
except Exception:
    r = None
    print("Redis not connected — running without cache")

# ── MODELS ───────────────────────────────────────────────────────
class PostData(BaseModel):
    id:          Optional[str] = None
    title:       Optional[str] = ""
    body:        Optional[str] = ""
    url:         Optional[str] = ""
    permalink:   Optional[str] = ""
    num_reports: Optional[int] = 0
    nsfw:        Optional[bool] = False
    score:       Optional[int] = 0
    created_at:  Optional[int] = 0
    flair:       Optional[str] = None
    is_video:    Optional[bool] = False
    media_type:  Optional[str] = ""
    media_urls:  Optional[List[str]] = []

class AuthorData(BaseModel):
    id:        Optional[str] = None
    username:  Optional[str] = ""
    karma:     Optional[int] = 0
    is_gold:   Optional[bool] = False
    banned:    Optional[bool] = False
    suspended: Optional[bool] = False
    spam:      Optional[bool] = False

class SubredditData(BaseModel):
    id:          Optional[str] = None
    name:        Optional[str] = ""
    subscribers: Optional[int] = 0

class AnalyzeRequest(BaseModel):
    schema_version: Optional[str] = "3.0.0"
    trigger_event:  Optional[str] = "PostSubmit"
    post:           Optional[PostData] = None
    comment:        Optional[Any] = None
    author:         Optional[AuthorData] = None
    subreddit:      Optional[SubredditData] = None

class ExecuteRequest(BaseModel):
    username:   str
    action:     str
    post_id:    Optional[str] = None
    reason:     Optional[str] = ""
    taken_by:   Optional[str] = "human_officer"
    timestamp:  Optional[str] = None

class MessageRequest(BaseModel):
    username:     str
    subject:      str
    body:         str

class BanRequest(BaseModel):
    username:  str
    subreddit: str
    reason:    str
    duration:  Optional[int] = 0
    message:   Optional[str] = ""

class RemoveRequest(BaseModel):
    post_id: str
    spam:    Optional[bool] = False

# ── HEALTH ───────────────────────────────────────────────────────
@app.get("/")
def health():
    return {
        "status":  "r-shield backend running",
        "redis":   "connected" if r else "disconnected",
        "time":    datetime.now(timezone.utc).isoformat(),
    }

# ── MAIN ENTRY POINT ─────────────────────────────────────────────
@app.post("/analyze")
async def analyze(request: AnalyzeRequest, background_tasks: BackgroundTasks):
    """
    Receives raw event from Devvit triggers.ts
    Runs LangGraph pipeline in background
    Returns decision to Devvit
    """
    print(f"\n[r-shield] /analyze received")
    print(f"  Event:  {request.trigger_event}")
    print(f"  User:   u/{request.author.username if request.author else '?'}")
    print(f"  Post:   {request.post.title[:60] if request.post else '?'}")

    # ── LAYER 0: Instant rules check (CPU, <1ms) ─────────────────
    # Check if user is already banned or domain already blocked
    username = request.author.username if request.author else ""
    post_id  = request.post.id if request.post else ""

    if r:
        # Already banned?
        if r.sismember("rg:banned_users", username):
            return build_response("REMOVE_AND_BAN", "IMMEDIATE", 100,
                                  f"User {username} is already banned",
                                  username, post_id)

        # Domain already blocked?
        if request.post and request.post.url:
            domain = extract_domain(request.post.url)
            if domain and r.sismember("rg:blocked_domains", domain):
                return build_response("REMOVE_AND_BAN", "IMMEDIATE", 95,
                                      f"Domain {domain} is blocklisted",
                                      username, post_id)

    # ── Run full LangGraph pipeline in background ─────────────────
    # For now returns dummy response — replace with real pipeline call
    background_tasks.add_task(run_pipeline, request)

    # ── DUMMY RESPONSE while pipeline is being built ──────────────
    # Once pipeline is ready, this will be replaced by pipeline output
    dummy = build_response(
        action  = "NO_ACTION",
        tier    = "LOG_ONLY",
        score   = 10,
        reason  = "Pipeline not yet connected — dummy response",
        username= username,
        post_id = post_id,
    )

    # Save to Redis audit log
    if r:
        r.lpush("rg:audit_log", json.dumps({
            **dummy,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "taken_by":  "pipeline",
        }))
        r.incr("rg:stats:analyzed")

    return dummy

# ── EXECUTE ACTION (called by Streamlit Office or auto-path) ─────
@app.post("/execute")
async def execute(request: ExecuteRequest):
    """
    Executes a moderation action on Reddit.
    Called by:
      - triggers.ts (auto path, IMMEDIATE tier)
      - Streamlit Office (human path, REVIEW tier)
    """
    print(f"[r-shield] /execute  action={request.action}  user={request.username}")

    # TODO: Wire to Reddit API via PRAW or Devvit callback
    # For now logs and saves to Redis

    log_entry = {
        "username":  request.username,
        "action":    request.action,
        "post_id":   request.post_id,
        "reason":    request.reason,
        "taken_by":  request.taken_by,
        "timestamp": request.timestamp or datetime.now(timezone.utc).isoformat(),
    }

    if r:
        r.lpush("rg:audit_log", json.dumps(log_entry))
        if request.action == "BAN":
            r.incr("rg:stats:banned")
            r.sadd("rg:banned_users", request.username)
        elif request.action == "REMOVE":
            r.incr("rg:stats:removed")
        elif request.action == "WARN":
            r.incr("rg:stats:warned")
        elif request.action == "APPROVE":
            r.incr("rg:stats:approved")

    return {"status": "ok", "action": request.action, "username": request.username}

# ── MESSAGE ──────────────────────────────────────────────────────
@app.post("/message")
async def send_message(request: MessageRequest):
    print(f"[r-shield] /message  to=u/{request.username}")
    # TODO: Wire to Reddit API
    if r:
        r.lpush("rg:message_log", json.dumps({
            "username":     request.username,
            "subject":      request.subject,
            "message_type": "CUSTOM",
            "timestamp":    datetime.now(timezone.utc).isoformat(),
        }))
    return {"status": "ok"}

# ── REDDIT ACTIONS (called by triggers.ts) ────────────────────────
@app.post("/reddit/ban")
async def reddit_ban(request: BanRequest):
    """Called by triggers.ts to ban a user"""
    print(f"[r-shield] Ban request: u/{request.username} on r/{request.subreddit}")
    # TODO: implement via PRAW
    # import praw
    # reddit = praw.Reddit(...)
    # subreddit = reddit.subreddit(request.subreddit)
    # subreddit.banned.add(request.username, ban_reason=request.reason, ...)
    if r:
        r.sadd("rg:banned_users", request.username)
        r.incr("rg:stats:banned")
    return {"status": "ok", "action": "ban", "username": request.username}

@app.post("/reddit/remove")
async def reddit_remove(request: RemoveRequest):
    """Called by triggers.ts to remove a post"""
    print(f"[r-shield] Remove request: post {request.post_id}  spam={request.spam}")
    # TODO: implement via PRAW
    if r:
        r.incr("rg:stats:removed")
    return {"status": "ok", "action": "remove", "post_id": request.post_id}

@app.post("/reddit/modmail")
async def reddit_modmail(body: dict):
    print(f"[r-shield] Modmail: {body.get('subject','')}")
    return {"status": "ok"}

# ── SETTINGS ─────────────────────────────────────────────────────
@app.post("/settings")
async def update_settings(body: dict):
    if r:
        r.set("rg:pipeline_settings", json.dumps(body))
    return {"status": "ok"}

@app.get("/settings")
async def get_settings():
    if r:
        raw = r.get("rg:pipeline_settings")
        if raw:
            return json.loads(raw)
    return {
        "score_auto":           False,
        "qwen_auto":            False,
        "score_ban_threshold":  85,
        "score_warn_threshold": 45,
        "pipeline_paused":      False,
    }

@app.post("/settings/domain/add")
async def domain_add(body: dict):
    domain = body.get("domain","")
    if r and domain:
        r.sadd("rg:blocked_domains", domain)
    return {"status": "ok"}

@app.post("/settings/domain/remove")
async def domain_remove(body: dict):
    domain = body.get("domain","")
    if r and domain:
        r.srem("rg:blocked_domains", domain)
    return {"status": "ok"}

# ── USER LOOKUP ───────────────────────────────────────────────────
@app.get("/user/{username}")
async def user_lookup(username: str):
    result = {"user": {}, "action_history": [], "neo4j_profile": {}}
    if r:
        profile = r.hgetall(f"rg:user:{username}")
        result["user"] = profile

        # Get action history for this user
        raw_log = r.lrange("rg:audit_log", 0, 499)
        history = []
        for entry in raw_log:
            try:
                item = json.loads(entry)
                if item.get("username") == username:
                    history.append(item)
            except Exception:
                pass
        result["action_history"] = history[:20]
    return result

# ── NETWORK ───────────────────────────────────────────────────────
@app.get("/network")
async def network():
    """Returns graph data for Streamlit network visualizer"""
    # TODO: query Neo4j for real network data
    # For now returns empty — Streamlit shows placeholder
    return {"nodes": [], "edges": []}

# ── UNDO ──────────────────────────────────────────────────────────
@app.post("/undo")
async def undo(body: dict):
    username = body.get("username","")
    action   = body.get("action","")
    print(f"[r-shield] Undo: {action} on u/{username}")
    # TODO: reverse the Reddit action via PRAW
    if r and action == "BAN":
        r.srem("rg:banned_users", username)
    return {"status": "ok", "undone": action, "username": username}

# ── STATS ─────────────────────────────────────────────────────────
@app.get("/stats")
async def stats():
    if not r:
        return {"analyzed":0,"banned":0,"removed":0,"warned":0,"approved":0,"queue_size":0}
    return {
        "analyzed":   int(r.get("rg:stats:analyzed")  or 0),
        "banned":     int(r.get("rg:stats:banned")     or 0),
        "removed":    int(r.get("rg:stats:removed")    or 0),
        "warned":     int(r.get("rg:stats:warned")     or 0),
        "approved":   int(r.get("rg:stats:approved")   or 0),
        "queue_size": int(r.llen("rg:hold_queue")       or 0),
    }

# ── HELPERS ───────────────────────────────────────────────────────
def extract_domain(url: str) -> Optional[str]:
    try:
        from urllib.parse import urlparse
        return urlparse(url).netloc.replace("www.","")
    except Exception:
        return None

def build_response(action: str, tier: str, score: int,
                   reason: str, username: str, post_id: str) -> dict:
    return {
        "final_action":       action,
        "final_tier":         tier,
        "score":              score,
        "reasoning_summary":  reason,
        "mod_mail_text":      f"r-shield: {action} for u/{username}. {reason}",
        "additional_actions": [],
        "ban_duration":       0,
        "ban_message":        "Your account has been banned for violating subreddit rules.",
        "username":           username,
        "post_id":            post_id,
    }

async def run_pipeline(request: AnalyzeRequest):
    from pipeline.graph import run_graph
    result = await run_graph(request)

    # Update Redis with result
    if r:
        r.incr("rg:stats:analyzed")
        r.lpush("rg:audit_log", json.dumps({
            **result,
            "username":  request.author.username if request.author else "",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "taken_by":  "pipeline",
        }))