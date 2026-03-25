# DodgeAI — SAP Order to Cash Graph Explorer

> An interactive graph visualization + LLM-powered query interface for SAP O2C data.

![Architecture](./docs/architecture.png)

## Quick Start

```bash
# 1. Clone and enter the project
cd /path/to/DodgeAi

# 2. Install dependencies
pip3 install fastapi uvicorn google-genai networkx python-dotenv sse-starlette aiofiles

# 3. Add your Gemini API key (free at https://ai.google.dev)
echo "GEMINI_API_KEY=your_key_here" > .env

# 4. Ingest the dataset (one-time)
python3 backend/ingest.py

# 5. Start the server
python3 -m uvicorn backend.main:app --port 8000 --host 0.0.0.0

# 6. Open http://localhost:8000
```

---

## Architecture

```
sap-o2c-data/ (JSONL)
    │
    ▼ ingest.py
SQLite (o2c.db)  — 21,393 rows, 19 tables
    │
    ▼ graph.py
NetworkX DiGraph  — 1,218 nodes, 1,496 edges
    │
    ▼ FastAPI (backend/main.py)
REST API  ─────────────────────────────────
    │                                      │
    ▼                                      ▼
D3.js Graph (frontend/)         Gemini 1.5 Flash (LLM)
Interactive Visualization       NL → SQL → Answer
```

### Key Decisions

| Component | Choice | Rationale |
|-----------|--------|-----------|
| Database | SQLite | Zero-ops, file-based, fast for read-heavy queries on this dataset size |
| Graph | NetworkX (in-memory) | Perfect for traversal queries; no separate graph DB needed at this scale |
| Visualization | D3.js | Maximum flexibility for force-directed layout + custom node styling |
| LLM | Google Gemini 1.5 Flash | Free tier, fast, excellent SQL generation |
| Backend | FastAPI | Async, typed, SSE streaming built-in |

---

## Graph Model

### Node Types (11)

| Type | Count | Description |
|------|-------|-------------|
| SalesOrder | 100 | Sales order headers |
| SalesOrderItem | 167 | Line items per order |
| Delivery | 86 | Outbound delivery headers |
| DeliveryItem | 137 | Delivery line items |
| BillingDoc | 163 | Billing document headers |
| BillingItem | 245 | Billing line items |
| JournalEntry | 123 | AR journal entries |
| Payment | 76 | Payment clearing documents |
| Customer | 8 | Business partners/customers |
| Product | 69 | Materials/products |
| Plant | 44 | Manufacturing/shipping plants |

### Edge Types (8 relationship types)

```
SalesOrder ──HAS_ITEM──► SalesOrderItem
SalesOrder ──SOLD_TO───► Customer
SalesOrderItem ──FOR_PRODUCT──► Product
SalesOrderItem ──DELIVERED_BY──► DeliveryItem
Delivery ──HAS_ITEM────► DeliveryItem
DeliveryItem ──AT_PLANT──► Plant
BillingDoc ──HAS_ITEM──► BillingItem
BillingDoc ──BILLED_TO──► Customer
BillingDoc ──POSTS_TO──► JournalEntry
JournalEntry ──CLEARED_BY──► Payment
```

---

## LLM Prompting Strategy

The system uses a **two-phase prompting approach**:

### Phase 1: SQL Generation
The LLM receives:
- Full schema description (all 15 tables with PKs, FKs, status codes)
- User's natural language question
- Conversation history (last 3 turns)

Output: A SQLite SQL query in a markdown code block.

### Phase 2: Result Summarization
After SQL execution:
- Column names + row data sent back
- LLM generates a concise business-focused natural language answer

### System Prompt Design
```
You are DodgeAI, an intelligent data analyst for SAP O2C processes.
[Full schema with 15 tables, all key columns and relationships]

GUARDRAILS — CRITICAL:
You MUST strictly reject ANY query not related to the O2C dataset.
For rejected queries, ALWAYS respond with the exact rejection message.
```

---

## Guardrails

**Two-layer defense:**

1. **Fast keyword pre-filter** (Python, no LLM call):  
   Checks if query contains any O2C-related keywords (`order`, `delivery`, `billing`, `payment`, `customer`, etc.)
   
2. **LLM-level guardrail** (in system prompt):  
   Instructs Gemini to reject off-topic queries with a fixed response message

3. **SQL safety**: Only `SELECT` statements allowed — no mutations.

Example rejection:
```
> "Write me a poem about nature"
→ "This system is designed to answer questions related to the SAP 
   Order-to-Cash dataset only. I can help you analyze sales orders, 
   deliveries, billing documents, payments, customers, and products."
```

---

## Example Queries

The system handles all required question types:

```
a. "Which products are associated with the highest number of billing documents?"
b. "Trace the full flow of billing document 90504248"  
c. "Show sales orders that were delivered but never billed"
d. "Which customers have outstanding unpaid billing documents?"
e. "What is the total revenue by customer?"
```

---

## API Reference

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Frontend UI |
| `/api/graph` | GET | All nodes + edges (limit param) |
| `/api/graph/stats` | GET | Node/edge type counts |
| `/api/node/{id}` | GET | Node details + neighbors |
| `/api/search?q=` | GET | Fuzzy search over nodes |
| `/api/chat` | POST | NL → SQL → answer |
| `/api/chat/stream` | POST | Streaming version (SSE) |

---

## Project Structure

```
DodgeAi/
├── backend/
│   ├── __init__.py
│   ├── ingest.py      # JSONL → SQLite ingestion
│   ├── graph.py       # NetworkX graph construction
│   ├── llm.py         # Gemini NL→SQL pipeline + guardrails
│   └── main.py        # FastAPI server
├── frontend/
│   ├── index.html     # Single-page app
│   ├── style.css      # Premium dark/light design
│   └── app.js         # D3.js graph + chat interface
├── sap-o2c-data/      # Raw JSONL dataset (19 directories)
├── o2c.db             # SQLite database (generated)
├── .env               # API keys
├── requirements.txt
└── README.md
```
