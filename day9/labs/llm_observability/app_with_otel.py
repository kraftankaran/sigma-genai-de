import boto3
import phoenix as px
from openinference.instrumentation.bedrock import BedrockInstrumentor
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
import json
import time

# ── 1. LAUNCH PHOENIX ──
print("Launching Phoenix...")
session = px.launch_app()

# ── 2. SETUP OTEL ──
provider = TracerProvider()
provider.add_span_processor(
    SimpleSpanProcessor(
        OTLPSpanExporter(endpoint="http://localhost:6006/v1/traces")
    )
)
trace.set_tracer_provider(provider)

# ── 3. INSTRUMENT BEDROCK ──
BedrockInstrumentor().instrument()

# ── 4. RUN LLM CALL ──
def run_support_agent():
    print("Running LLM call...")

    bedrock = boto3.client("bedrock-runtime", region_name="us-east-1")

    prompt = "User was charged twice. Help with refund."

    body = {
        "messages": [
            {
                "role": "user",
                "content": [{"text": prompt}]
            }
        ],
        "inferenceConfig": {
            "maxTokens": 100,
            "temperature": 0.2
        }
    }

    response = bedrock.invoke_model(
        modelId="amazon.nova-lite-v1:0",
        body=json.dumps(body)
    )

    response_body = json.loads(response["body"].read())
    output = response_body["output"]["message"]["content"][0]["text"]

    print("\nLLM Response:\n", output)


if __name__ == "__main__":
    run_support_agent()

    print("\nPhoenix running at http://localhost:6006")
    print("KEEP THIS TERMINAL OPEN")

    while True:
        time.sleep(1)