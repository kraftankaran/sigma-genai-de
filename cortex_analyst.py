# ============================================================
# Cortex Analyst Client — FINAL WORKING VERSION
# ============================================================

import json
import time
import os
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

# ── CONFIGURATION ──────────────────────────────────────────
ACCOUNT = 'GEJKIOG-TKC55632'
USER = 'student_genai'
KEY_FILE = os.path.join(os.path.dirname(__file__), 'student_key.p8')

# ── LOAD PRIVATE KEY ───────────────────────────────────────
print("🔐 Loading private key...")

try:
    with open(KEY_FILE, 'rb') as f:
        private_key = serialization.load_pem_private_key(
            f.read(),
            password=None,
            backend=default_backend()
        )
except Exception as e:
    print("❌ ERROR loading key:", e)
    exit(1)

PRIVATE_KEY_BYTES = private_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

# Semantic model path
SEMANTIC_MODEL = '@SIGMA_DE.PUBLIC.SEMANTIC_MODELS/sigma_semantic_model.yaml'


# ── CONNECTION ─────────────────────────────────────────────
def get_connection():
    print("🔌 Connecting to Snowflake...")
    return snowflake.connector.connect(
        user=USER,
        account=ACCOUNT,
        private_key=PRIVATE_KEY_BYTES,
        database='SIGMA_DE',
        schema='PUBLIC',
        warehouse='COMPUTE_WH',
        role='STUDENT_CORTEX'
    )


# ── ASK CORTEX ─────────────────────────────────────────────
def ask_cortex(question: str) -> dict:
    print(f"\n[Cortex] Question: {question}")
    start_time = time.time()

    try:
        conn = get_connection()
        cur = conn.cursor()
    except Exception as e:
        return {"error": f"Connection failed: {e}"}

    # Step 1 — Generate SQL using Cortex
    sql_prompt = f"""Given this schema:
FACT_TRANSACTIONS(TRANSACTION_ID, AMOUNT, STATUS[COMPLETED/FAILED/PENDING], MERCHANT_ID, CUSTOMER_ID, TRANSACTION_DATE, PAYMENT_METHOD[CREDIT_CARD/DEBIT_CARD/UPI])
DIM_MERCHANT(MERCHANT_ID, MERCHANT_NAME, CATEGORY, CITY)
Revenue = SUM(AMOUNT) WHERE STATUS = 'COMPLETED' only

Write a Snowflake SQL query to answer: {question}
Return ONLY the SQL. No explanation."""

    try:
        cur.execute(
            "SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large2', %s)",
            (sql_prompt,)
        )
        sql_response = cur.fetchone()[0].strip()
    except Exception as e:
        return {"error": f"Cortex generation failed: {e}"}

    # Clean markdown
    if sql_response.startswith("```"):
        sql_response = sql_response.split("\n", 1)[1]
        sql_response = sql_response.rsplit("```", 1)[0].strip()

    print(f"[Cortex] Generated SQL:\n{sql_response}")

    # Step 2 — Execute SQL
    result = {
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


# ── DISPLAY RESULTS ─────────────────────────────────────────
def display_results(question: str, result: dict):
    print("\n" + "─" * 60)
    print(f"Q: {question}")
    print("─" * 60)

    if result.get("error"):
        print(f"❌ ERROR: {result['error']}")
        return

    print(f"\nSQL:\n{result['sql']}\n")

    if result["columns"]:
        header = " | ".join(result["columns"])
        print(header)
        print("-" * len(header))

        for row in result["rows"][:10]:
            print(" | ".join(str(v) for v in row))

    print(f"\n⏱ Time: {result['elapsed_seconds']:.2f}s")


# ── QUESTIONS ───────────────────────────────────────────────
COMPARISON_QUESTIONS = [
    "How many transactions do we have in total?",
    "How many transactions failed?",
    "Which merchant had the highest revenue?",
    "What is the failure rate for each payment method?",
    "What was the total revenue generated across all merchants?"
]


# ── RUNNER ─────────────────────────────────────────────────
def run_comparison():
    print("\n" + "=" * 60)
    print("  CORTEX ANALYST — RUNNING TEST")
    print("=" * 60)

    comparison_log = []

    for i, question in enumerate(COMPARISON_QUESTIONS, 1):
        print(f"\n[Question {i}/5]")

        result = ask_cortex(question)
        display_results(question, result)

        comparison_log.append({
            "question_num": i,
            "question": question,
            "sql": result.get("sql"),
            "row_count": len(result.get("rows", [])),
            "time": result.get("elapsed_seconds"),
            "error": result.get("error")
        })

        time.sleep(1)

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    for r in comparison_log:
        status = "OK" if not r["error"] else "ERROR"
        print(f"{r['question_num']}. {status} | {r['time']:.2f}s")

    # Save results
    with open("cortex_results.json", "w") as f:
        json.dump(comparison_log, f, indent=2)

    print("\n📁 Saved: cortex_results.json")


# ── MAIN ───────────────────────────────────────────────────
if __name__ == "__main__":
    print("🔥 Script started...")
    try:
        run_comparison()
    except Exception as e:
        print("❌ FATAL ERROR:", e)