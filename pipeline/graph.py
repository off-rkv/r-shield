"""
pipeline/graph.py

Wires all 5 nodes into a LangGraph StateGraph.
This is what server.py calls from run_pipeline().
"""

from langgraph.graph import StateGraph, END
from pipeline.state import RShieldState
from pipeline.nodes.fetch_node  import fetch_node
from pipeline.nodes.parser_node import parser_node
from pipeline.nodes.qwen_node   import qwen_node
from pipeline.nodes.neo4j_node  import neo4j_node
from pipeline.nodes.action_node import action_node
import time


def build_graph():
    """Build and compile the LangGraph pipeline"""
    graph = StateGraph(RShieldState)

    # Add all 5 nodes
    graph.add_node("fetch",  fetch_node)
    graph.add_node("parser", parser_node)
    graph.add_node("qwen",   qwen_node)
    graph.add_node("neo4j",  neo4j_node)
    graph.add_node("action", action_node)

    # Wire them in sequence
    graph.set_entry_point("fetch")
    graph.add_edge("fetch",  "parser")
    graph.add_edge("parser", "qwen")
    graph.add_edge("qwen",   "neo4j")
    graph.add_edge("neo4j",  "action")
    graph.add_edge("action", END)

    return graph.compile()


# Singleton — compile once
_graph = None

def get_graph():
    global _graph
    if _graph is None:
        print("[graph] Compiling LangGraph pipeline...")
        _graph = build_graph()
        print("[graph] Pipeline ready")
    return _graph


async def run_graph(request) -> dict:
    """
    Entry point called by server.py
    Takes AnalyzeRequest, returns final decision dict
    """
    t0 = time.time()

    # Build initial state from request
    initial_state: RShieldState = {
        "schema_version":  request.schema_version or "3.0.0",
        "trigger_event":   request.trigger_event  or "PostSubmit",
        "raw_post":        request.post.dict()     if request.post      else {},
        "raw_author":      request.author.dict()   if request.author    else {},
        "raw_subreddit":   request.subreddit.dict()if request.subreddit else {},
        "raw_comment":     request.comment         if request.comment   else None,
    }

    print(f"\n[graph] Pipeline starting for u/{initial_state['raw_author'].get('username','?')}")

    try:
        graph  = get_graph()
        result = graph.invoke(initial_state)

        elapsed_ms = int((time.time() - t0) * 1000)
        result["pipeline_ms"] = elapsed_ms

        print(f"[graph] Pipeline complete — {elapsed_ms}ms")
        print(f"[graph] Decision: {result.get('final_action','?')} | tier: {result.get('final_tier','?')} | score: {result.get('score','?')}")

        # Return only what server.py needs to send back to Devvit
        return {
            "final_action":      result.get("final_action","NO_ACTION"),
            "final_tier":        result.get("final_tier","LOG_ONLY"),
            "score":             result.get("score",0),
            "mod_mail_text":     result.get("mod_mail_text",""),
            "additional_actions":result.get("additional_actions",[]),
            "reasoning_summary": result.get("reasoning_summary",""),
            "ban_duration":      result.get("ban_duration",0),
            "ban_message":       result.get("ban_message",""),
            "pipeline_ms":       elapsed_ms,
        }

    except Exception as e:
        print(f"[graph] Pipeline error: {e}")
        return {
            "final_action":  "REVIEW",
            "final_tier":    "REVIEW",
            "score":         0,
            "mod_mail_text": f"Pipeline error: {e}",
            "additional_actions": [],
            "reasoning_summary":  str(e),
            "pipeline_ms":   int((time.time() - t0) * 1000),
        }