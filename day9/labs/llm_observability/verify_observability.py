import json
import os
import sys

def main():
    print("Checking LLM Observability Lab completion status...")
    
    try:
        from phoenix.client import Client
    except ImportError:
        print("❌ Install phoenix: pip install arize-phoenix")
        sys.exit(1)

    # ✅ Use 127.0.0.1 (IMPORTANT FIX)
    try:
        client = Client(base_url="http://127.0.0.1:6006")
        spans_df = client.spans.get_spans_dataframe()
    except Exception as e:
        print("❌ Error: Cannot connect to Phoenix.")
        print(e)
        sys.exit(1)

    if spans_df is None or spans_df.empty:
        print("❌ No spans found. Run your app first.")
        sys.exit(1)

    total_spans = len(spans_df)

    llm_spans = 0
    if "span_kind" in spans_df.columns:
        llm_spans = len(spans_df[spans_df["span_kind"] == "LLM"])

    print(f"✓ Total Spans: {total_spans}")
    print(f"✓ LLM Calls: {llm_spans}")

    # ✅ Save output
    os.makedirs("../output", exist_ok=True)

    result = {
        "status": "success",
        "phoenix_active": True,
        "total_spans_captured": total_spans,
        "llm_inference_calls": llm_spans,
        "llm_observability_verified": True
    }

    with open("../output/llm_observability_success.json", "w") as f:
        json.dump(result, f, indent=2)

    print("🎉 SUCCESS! File created.")

if __name__ == "__main__":
    main()