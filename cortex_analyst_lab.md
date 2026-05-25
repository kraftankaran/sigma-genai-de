# ============================================================
# cortex_analyst.py — FINAL CLEAN VERSION
# ============================================================

import json
import time
import os
import snowflake.connector
from cryptography.hazmat.primitives import serialization

# ── CONFIGURATION ──────────────────────────────────────────
ACCOUNT = 'GEJKIOG-TKC55632'
USER = 'student_genai'
KEY_FILE = os.path.join(os.path.dirname(__file__), 'student_key.p8')

# Load private key
with open(KEY_FILE, 'rb') as f:
    private_key = serialization.load_pem_private_key(f.read(), password=None)

PRIVATE_KEY_BYTES = private_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

# ── CONNECTION ─────────────────────────────────────────────
def get_connection():
    return snowflake.connector.connect(
        user=USER,
        account=ACCOUNT,
        private_key=PRIVATE_KEY_BYTES,
        database='SIGMA_DE',
        schema='PUBLIC',
        warehouse='COMPUTE_WH',
        role='STUDENT_CORTEX'
    )

# ── ASK CORTEX (NL → SQL → EXECUTE) ────────────────────────
def ask_cortex(question: str) -> dict:
    print(f"\n[Cortex] Question: {question}")
    start_time = time.time()

    conn = get_connection()
    cur = conn.cursor()

    # Prompt to generate SQL
    prompt = f"""
You are a Snowflake SQL expert.

Schema:
FACT_TRANSACTIONS(TRANSACTION_ID, AMOUNT, STATUS, MERCHANT_ID, CUSTOMER_ID, TRANSACTION_DATE, PAYMENT_METHOD)
DIM_MERCHANT(MERCHANT_ID, MERCHANT_NAME, CATEGORY, CITY)

Rules:
Revenue = SUM(AMOUNT) WHERE STATUS = 'COMPLETED'

Write a Snowflake SQL query to answer:
{question}

Return ONLY SQL. No explanation.
"""

    # Call Cortex COMPLETE
    cur.execute(
        "SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large2', %s)",
        (prompt,)
    )

    sql_response = cur.fetchone()[0].strip()

    # Clean markdown fences if present
    if sql_response.startswith("```"):
        sql_response = sql_response.split("\n", 1)[1]
        sql_response = sql_response.rsplit("```", 1)[0].strip()

    print(f"\n[Cortex] Generated SQL:\n{sql_response}")

    # Execute SQL
    result = {
        "question": question,
        "sql": sql_response,
        "columns": [],
        "rows": [],
        "elapsed_seconds": 0,
        "error": None
    }

    try:
        cur.execute(sql_response)
        result["columns"] = [desc[0] for desc in cur.description]
        result["rows"] = cur.fetchall()
        print(f"[Cortex] Rows returned: {len(result['rows'])}")
    except Exception as e:
        result["error"] = str(e)
        print(f"[Cortex] Execution error: {e}")

    conn.close()
    result["elapsed_seconds"] = time.time() - start_time
    return result


# ── DISPLAY RESULTS ────────────────────────────────────────
def display_results(result: dict):
    print("\n" + "─" * 60)
    print(f"Q: {result['question']}")
    print("─" * 60)

    if result["error"]:
        print("ERROR:", result["error"])
        return

    print("\nSQL:")
    print(result["sql"])

    if result["columns"]:
        header = " | ".join(result["columns"])
        print("\n" + header)
        print("-" * len(header))

        for row in result["rows"][:10]:
            print(" | ".join(str(v) for v in row))

    print(f"\nTime: {result['elapsed_seconds']:.2f}s")


# ── QUESTIONS ──────────────────────────────────────────────
COMPARISON_QUESTIONS = [
    "How many transactions do we have in total?",
    "How many transactions failed?",
    "Which merchant had the highest revenue?",
    "What is the failure rate for each payment method?",
    "What was the total revenue generated across all merchants?"
]


# ── RUNNER ────────────────────────────────────────────────
def run_comparison():
    print("\n" + "=" * 60)
    print("CORTEX ANALYST — 5 QUESTION TEST")
    print("=" * 60)

    results = []

    for i, question in enumerate(COMPARISON_QUESTIONS, 1):
        print(f"\n[Question {i}/5]")
        result = ask_cortex(question)
        display_results(result)
        results.append(result)
        time.sleep(1)

    # Save results
    with open("cortex_results.json", "w") as f:
        json.dump(results, f, indent=2)

    print("\nResults saved to cortex_results.json")


# ── MAIN ──────────────────────────────────────────────────
if __name__ == "__main__":
    run_comparison()