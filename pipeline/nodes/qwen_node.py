"""
pipeline/nodes/qwen_node.py

Node 3 — Qwen
Loads your fine-tuned GGUF model, passes parsed input schema,
returns full output schema v3.0.0
Sets: qwen_output + all quick-access fields
"""

import json
import re
import os
from pipeline.state import RShieldState

# ── MODEL CONFIG ─────────────────────────────────────────────────
# Path to your GGUF file — update to match your setup
# Options:
#   1. Local file path
#   2. Downloaded from HF via huggingface_hub
MODEL_PATH = os.getenv("QWEN_MODEL_PATH", "./models/redditguard_int8.gguf")

# Download from HF if not present locally
HF_REPO_INT8 = "Ratnesh777/redditguard-qwen-gguf-int8"
HF_REPO_INT4 = "Ratnesh777/redditguard-qwen-gguf-int4"
HF_FILE_INT8 = "redditguard_int8.gguf"
HF_FILE_INT4 = "redditguard_int4.gguf"

# Singleton — load once, reuse across all requests
_llm = None

def get_llm():
    global _llm
    if _llm is not None:
        return _llm

    print("[qwen_node] Loading model...")

    # Auto-download from HF if file not found locally
    if not os.path.exists(MODEL_PATH):
        print(f"[qwen_node] Model not found at {MODEL_PATH} — downloading from HuggingFace...")
        try:
            from huggingface_hub import hf_hub_download
            model_file = hf_hub_download(
                repo_id   = HF_REPO_INT8,
                filename  = HF_FILE_INT8,
                local_dir = "./models"
            )
            print(f"[qwen_node] Downloaded to {model_file}")
        except Exception as e:
            print(f"[qwen_node] HF download failed: {e}")
            raise

    from llama_cpp import Llama
    _llm = Llama(
        model_path   = MODEL_PATH,
        n_ctx        = 6000,
        n_gpu_layers = int(os.getenv("QWEN_GPU_LAYERS", "0")),  # -1 = all on GPU
        n_threads    = 8,
        n_batch      = 512,
        verbose      = False,
    )
    print("[qwen_node] Model loaded and ready")
    return _llm


# ── SYSTEM PROMPT ─────────────────────────────────────────────────
SYSTEM_PROMPT = """You are RedditGuard, an AI moderation system for Reddit.
Analyze the given Reddit user input schema and output a complete JSON analysis following output schema v3.0.0 exactly.

Your output must be valid JSON containing:
schema_version, model_version, analyzed_at_utc, username,
labels (primary, secondary, authenticity, operator_type, language),
counter_evidence (considered, rebuttal, false_positive_risk),
signal_attribution (array of signals with evidence and combined_strength),
image_analysis, pii_flags,
dbscan_vector (all 22 dimensions),
delta_signals, neo4j (node_type, properties, edges),
action_recommendation (primary_action, tier, alternatives_considered, additional_actions, review_priority, reversibility, appeal_handling),
reasoning (plain English paragraph).

Output ONLY valid JSON. No markdown. No explanation outside JSON."""


# ── MAIN NODE FUNCTION ────────────────────────────────────────────
def qwen_node(state: RShieldState) -> RShieldState:
    """
    Input:  state with parsed_input
    Output: state + qwen_output + quick-access fields
    """
    parsed_input = state.get("parsed_input", {})
    username     = parsed_input.get("user", {}).get("username", "?")
    image_b64    = state.get("image_base64")

    print(f"[qwen_node] Running inference for u/{username}")

    user_prompt = f"Analyze this Reddit user:\n\n{json.dumps(parsed_input, indent=2)}"

    # ── Call model ────────────────────────────────────────────────
    try:
        llm = get_llm()

        response = llm.create_chat_completion(
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user",   "content": user_prompt}
            ],
            max_tokens     = 3000,
            temperature    = 0.1,
            top_p          = 0.9,
            repeat_penalty = 1.1,
        )

        raw_output = response["choices"][0]["message"]["content"]
        usage      = response.get("usage", {})
        print(f"[qwen_node] Inference complete — tokens: {usage}")

    except Exception as e:
        print(f"[qwen_node] Inference failed: {e}")
        return {
            **state,
            "qwen_output":        {},
            "primary_label":      "INFERENCE_ERROR",
            "false_positive_risk":"HIGH",
            "qwen_action":        "REVIEW",
            "qwen_tier":          "REVIEW",
            "overall_risk":       "UNKNOWN",
            "reasoning":          f"Qwen inference failed: {e}",
            "error":              str(e),
        }

    # ── Parse JSON output ─────────────────────────────────────────
    qwen_output = parse_qwen_output(raw_output)

    if not qwen_output:
        print("[qwen_node] JSON parse failed — sending to REVIEW")
        return {
            **state,
            "qwen_output":        {"raw": raw_output},
            "primary_label":      "PARSE_ERROR",
            "false_positive_risk":"HIGH",
            "qwen_action":        "REVIEW",
            "qwen_tier":          "REVIEW",
            "overall_risk":       "UNKNOWN",
            "reasoning":          "Could not parse Qwen output — human review required",
        }

    # ── Extract quick-access fields ───────────────────────────────
    labels     = qwen_output.get("labels", {})
    primary    = labels.get("primary", {})
    auth       = labels.get("authenticity", {})
    counter    = qwen_output.get("counter_evidence", {})
    action_rec = qwen_output.get("action_recommendation", {})
    dbscan     = qwen_output.get("dbscan_vector", {})

    print(f"[qwen_node] Label: {primary.get('label','?')} ({primary.get('confidence','?')})")
    print(f"[qwen_node] Auth:  {auth.get('label','?')}")
    print(f"[qwen_node] Action: {action_rec.get('primary_action','?')} tier={action_rec.get('tier','?')}")

    return {
        **state,
        "qwen_output":        qwen_output,
        "primary_label":      primary.get("label"),
        "primary_confidence": primary.get("confidence"),
        "authenticity_label": auth.get("label"),
        "false_positive_risk":counter.get("false_positive_risk"),
        "overall_risk":       dbscan.get("overall_risk"),
        "qwen_action":        action_rec.get("primary_action"),
        "qwen_tier":          action_rec.get("tier"),
        "reasoning":          qwen_output.get("reasoning",""),
    }


# ── HELPERS ───────────────────────────────────────────────────────
def parse_qwen_output(raw: str) -> dict:
    """Strip markdown fences and parse JSON"""
    try:
        text = re.sub(r"```json\s*", "", raw)
        text = re.sub(r"```\s*",     "", text)
        text = text.strip()
        start = text.find("{")
        end   = text.rfind("}") + 1
        if start == -1 or end == 0:
            return {}
        return json.loads(text[start:end])
    except Exception as e:
        print(f"[qwen_node] JSON parse error: {e}")
        return {}