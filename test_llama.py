from llama_cpp import Llama

print("Loading model...")

llm = Llama(
    model_path="./models/redditguard_int8.gguf",
    n_ctx=1024,
    n_gpu_layers=0,
    n_threads=8,
    verbose=False
)

print("Running quick test...")

response = llm.create_chat_completion(
    messages=[
        {
            "role": "system",
            "content": "You are RedditGuard. Output only JSON."
        },
        {
            "role": "user",
            "content": "Say hello in JSON format like {\"status\": \"hello\"}"
        }
    ],
    max_tokens=50,
    temperature=0.1,
)

print(response["choices"][0]["message"]["content"])

print("Test passed")