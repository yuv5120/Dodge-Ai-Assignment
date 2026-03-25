"""
llm.py — Gemini-powered natural language → SQL → answer pipeline.
"""
import os
import re
import sqlite3
import time as _time
from pathlib import Path
from typing import Optional, List, Dict, Any
from google import genai
from google.genai import types as genai_types  # noqa: F401
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

DB_PATH = Path(__file__).parent.parent / "o2c.db"

SCHEMA_DESCRIPTION = """
You have access to a SQLite database with the following tables representing an SAP Order-to-Cash (O2C) process:

TABLES AND KEY COLUMNS:

1. sales_order_headers
   - salesOrder (PK), salesOrderType, soldToParty (FK→business_partners.businessPartner)
   - totalNetAmount, transactionCurrency, creationDate
   - overallDeliveryStatus ('A'=not started, 'B'=partial, 'C'=complete)
   - overallOrdReltdBillgStatus ('A'=not billed, 'B'=partial, 'C'=fully billed)

2. sales_order_items
   - salesOrder (FK→sales_order_headers), salesOrderItem, material (FK→products)
   - requestedQuantity, netAmount, productionPlant (FK→plants)

3. sales_order_schedule_lines
   - salesOrder, salesOrderItem, scheduleLine, requestedDeliveryDate, scheduledQuantity

4. outbound_delivery_headers
   - deliveryDocument (PK), shippingPoint, creationDate
   - overallGoodsMovementStatus ('A'=not started, 'B'=partial, 'C'=complete)
   - overallPickingStatus ('A'=not picked, 'C'=complete)

5. outbound_delivery_items
   - deliveryDocument (FK→outbound_delivery_headers), deliveryDocumentItem
   - referenceSdDocument (FK→sales_order_headers.salesOrder)
   - referenceSdDocumentItem (FK→sales_order_items.salesOrderItem)
   - plant (FK→plants), storageLocation, actualDeliveryQuantity

6. billing_document_headers
   - billingDocument (PK), billingDocumentType, soldToParty (FK→business_partners)
   - totalNetAmount, transactionCurrency, creationDate, billingDocumentDate
   - accountingDocument (FK→journal_entries), billingDocumentIsCancelled

7. billing_document_items
   - billingDocument (FK→billing_document_headers), billingDocumentItem
   - material (FK→products), billingQuantity, netAmount
   - referenceSdDocument (FK→outbound_delivery_headers.deliveryDocument)
   - referenceSdDocumentItem (FK→outbound_delivery_items.deliveryDocumentItem)

8. billing_document_cancellations
   - billingDocument (PK), cancelledBillingDocument

9. journal_entries  (journal_entry_items_accounts_receivable)
   - accountingDocument (PK with fiscalYear), fiscalYear, companyCode
   - referenceDocument (FK→billing_document_headers.billingDocument)
   - glAccount, amountInTransactionCurrency, transactionCurrency, postingDate
   - clearingAccountingDocument (FK→payments.clearingAccountingDocument)
   - customer (FK→business_partners)

10. payments  (payments_accounts_receivable)
    - accountingDocument, accountingDocumentItem, fiscalYear
    - clearingAccountingDocument, clearingDate, amountInTransactionCurrency
    - transactionCurrency, customer (FK→business_partners)

11. business_partners
    - businessPartner (PK), customer (same as businessPartner)
    - businessPartnerFullName, businessPartnerName

12. business_partner_addresses
    - businessPartner (FK), cityName, country, postalCode, streetName

13. products
    - product (PK), productType, productGroup, baseUnit

14. product_descriptions
    - product (FK), language, productDescription

15. plants
    - plant (PK), plantName, country, cityName

KEY RELATIONSHIPS (O2C Flow):
Sales Order → (has items) → Sales Order Items → (reference) → Delivery Items → (belonging to) → Delivery Headers
Delivery Items → (billed via) → Billing Document Items → (part of) → Billing Document Headers
Billing Document Headers → (creates) → Journal Entries → (cleared by) → Payments
Sales Order → Customer (soldToParty)
Sales Order Items → Products (material)
"""

SYSTEM_PROMPT = f"""You are DodgeAI, an intelligent data analyst for SAP Order-to-Cash process data.

{SCHEMA_DESCRIPTION}

## YOUR ROLE
You help users explore and understand Order-to-Cash business data by:
1. Analyzing queries
2. Generating accurate SQLite SQL
3. Executing it and providing data-backed, insightful answers

## GUARDRAILS — CRITICAL
You MUST strictly reject ANY query that is not related to the Order-to-Cash (O2C) SAP dataset above.
Examples of REJECTED queries:
- General knowledge questions ("What is the capital of France?")
- Creative writing, poems, jokes
- Code generation unrelated to SQL queries on this dataset
- Personal advice, opinions, philosophy
- Weather, news, sports, entertainment

For rejected queries, ALWAYS respond EXACTLY:
"This system is designed to answer questions related to the SAP Order-to-Cash dataset only. I can help you analyze sales orders, deliveries, billing documents, payments, customers, and products."

## HOW TO ANSWER
1. Understand the user's question about the O2C data
2. Write a SQLite SQL query to answer it (use proper JOINs, aggregations)
3. Return results in this exact format:

```sql
<your SQL here>
```

RESULT:
<natural language answer grounded in the data>

IMPORTANT SQL RULES:
- Always use LIMIT clauses (max 100 rows for lists, no limit for aggregations)
- Cast numeric text columns: CAST(column AS REAL)
- Use proper table and column names exactly as defined above
- For text comparisons use LIKE or = with proper quoting
- billingDocumentIsCancelled is stored as text 'false' or 'true'
- Status codes: overallDeliveryStatus 'C' = complete, overallOrdReltdBillgStatus '' = not billed
"""

# Domain-related keyword check (fast pre-filter before calling LLM)
DOMAIN_KEYWORDS = [
    "order", "delivery", "billing", "invoice", "payment", "journal",
    "customer", "product", "material", "plant", "sales", "revenue",
    "amount", "quantity", "document", "dispatch", "shipped", "billed",
    "overdue", "outstanding", "flow", "trace", "status", "supply",
    "partner", "vendor", "finance", "accounting", "o2c", "sap",
    "fiscal", "clearing", "gl", "account", "stock", "warehouse",
]

REJECTION_RESPONSE = (
    "This system is designed to answer questions related to the SAP Order-to-Cash dataset only. "
    "I can help you analyze sales orders, deliveries, billing documents, payments, customers, and products."
)


def is_domain_query(query: str) -> bool:
    q = query.lower()
    return any(kw in q for kw in DOMAIN_KEYWORDS)


def execute_sql(sql: str) -> Dict[str, Any]:
    """Execute a SQL query on the SQLite DB and return results."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        # Safety: only allow SELECT statements
        clean_sql = sql.strip().rstrip(";")
        if not clean_sql.upper().startswith("SELECT"):
            return {"error": "Only SELECT queries are allowed", "rows": []}
        cursor = conn.execute(clean_sql)
        rows = cursor.fetchmany(100)
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        return {
            "columns": columns,
            "rows": [dict(r) for r in rows],
            "count": len(rows),
        }
    except Exception as e:
        return {"error": str(e), "rows": []}
    finally:
        conn.close()


def extract_sql_from_response(text: str) -> Optional[str]:
    """Extract SQL from markdown code block."""
    match = re.search(r"```sql\s*(.*?)\s*```", text, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()
    # Try plain code block
    match = re.search(r"```\s*(SELECT.*?)\s*```", text, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return None


def get_client():
    api_key = os.getenv("GEMINI_API_KEY", "")
    if not api_key or api_key == "your_gemini_api_key_here":
        raise ValueError("GEMINI_API_KEY not configured. Please set it in the .env file.")
    return genai.Client(api_key=api_key)


_client = None


def get_or_create_client():
    global _client
    if _client is None:
        _client = get_client()
    return _client


# Model fallback chain — tries in order if rate-limited
_MODEL_FALLBACK = [
    "gemini-2.0-flash",
    "gemini-2.0-flash-lite",
    "gemini-flash-latest",
]


def _call_gemini(client, contents: list, system: str = SYSTEM_PROMPT) -> str:
    """Call Gemini with retry + model fallback for rate limit errors."""
    last_err = None
    for model in _MODEL_FALLBACK:
        for attempt in range(3):  # up to 3 retries per model
            try:
                response = client.models.generate_content(
                    model=model,
                    contents=contents,
                    config=genai_types.GenerateContentConfig(
                        system_instruction=system,
                        temperature=0.1,
                    ),
                )
                return response.text
            except Exception as e:
                last_err = e
                err_str = str(e)
                if "429" in err_str or "RESOURCE_EXHAUSTED" in err_str:
                    # Rate limited — wait then try next model after retries
                    wait = 5 * (attempt + 1)
                    _time.sleep(wait)
                    continue
                elif "404" in err_str or "NOT_FOUND" in err_str:
                    # Model not available — try next model immediately
                    break
                else:
                    raise  # non-rate-limit error, propagate immediately
    raise RuntimeError(f"All Gemini models failed. Last error: {last_err}")


def chat(
    message: str,
    history: List[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Process a natural language query:
    1. Domain guardrail check
    2. Ask Gemini to generate SQL
    3. Execute SQL
    4. Ask Gemini to summarize results
    Returns: {answer, sql, data, rejected}
    """
    # Fast keyword pre-check
    if not is_domain_query(message):
        return {
            "answer": REJECTION_RESPONSE,
            "sql": None,
            "data": None,
            "rejected": True,
            "highlighted_nodes": [],
        }

    client = get_or_create_client()

    # Build conversation history for context
    contents = []
    if history:
        for turn in history[-6:]:  # Last 3 exchanges
            role = "user" if turn["role"] == "user" else "model"
            contents.append(genai_types.Content(role=role, parts=[genai_types.Part(text=turn["content"])]))

    # Step 1: Generate SQL
    sql_prompt = f"""User question: {message}

Please analyze this question about the O2C dataset and generate a SQL query to answer it.
Remember: if this is NOT about the O2C dataset, respond with the rejection message."""

    contents.append(genai_types.Content(role="user", parts=[genai_types.Part(text=sql_prompt)]))
    response_text = _call_gemini(client, contents)

    # Check if the model rejected the query
    if "designed to answer questions related to the SAP" in response_text:
        return {
            "answer": REJECTION_RESPONSE,
            "sql": None,
            "data": None,
            "rejected": True,
            "highlighted_nodes": [],
        }

    # Extract and execute SQL
    sql = extract_sql_from_response(response_text)
    db_result = None
    highlighted_nodes = []

    if sql:
        db_result = execute_sql(sql)

        if "error" not in db_result and db_result.get("rows"):
            # Step 2: Summarize results — add assistant response then user follow-up
            contents.append(genai_types.Content(role="model", parts=[genai_types.Part(text=response_text)]))
            summary_prompt = f"""The SQL query returned {db_result['count']} rows.
Columns: {db_result['columns']}
Sample data (first 10 rows): {db_result['rows'][:10]}

Please provide a clear, concise natural language summary of these results for the user.
Focus on key insights and business meaning. Be specific with numbers."""
            contents.append(genai_types.Content(role="user", parts=[genai_types.Part(text=summary_prompt)]))
            final_answer = _call_gemini(client, contents)

            # Extract any node IDs mentioned in the response for highlighting
            highlighted_nodes = _extract_node_ids(message + " " + str(db_result.get("rows", [])))
        elif "error" in (db_result or {}):
            final_answer = f"I encountered an issue querying the data: {db_result['error']}\n\nLet me help you rephrase the question."
        else:
            final_answer = "The query returned no results. This could mean the data doesn't match the criteria, or the condition needs adjustment."
    else:
        final_answer = response_text

    return {
        "answer": final_answer,
        "sql": sql,
        "data": db_result,
        "rejected": False,
        "highlighted_nodes": highlighted_nodes,
    }


def _extract_node_ids(text: str) -> List[str]:
    """Extract potential node IDs from text for graph highlighting."""
    nodes = []
    # Sales orders (6+ digit numbers)
    for m in re.finditer(r'\b(7\d{5,6})\b', text):
        nodes.append(f"SO_{m.group(1)}")
    # Billing docs (9-digit starting with 9)
    for m in re.finditer(r'\b(9\d{7,8})\b', text):
        nodes.append(f"BILL_{m.group(1)}")
    # Deliveries (8-digit starting with 8)
    for m in re.finditer(r'\b(8\d{7})\b', text):
        nodes.append(f"DEL_{m.group(1)}")
    # Accounting docs (10-digit starting with 94)
    for m in re.finditer(r'\b(94\d{8})\b', text):
        nodes.append(f"JE_{m.group(1)}_2025")
    return list(set(nodes))[:10]
