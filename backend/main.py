"""
main.py — FastAPI application for the SAP O2C Graph System.
Serves REST API + static frontend files.
Run: uvicorn backend.main:app --reload --port 8000
"""
import asyncio
import json
from pathlib import Path
from typing import List, Optional
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

# Run ingestion at startup if DB doesn't exist
DB_PATH = Path(__file__).parent.parent / "o2c.db"
FRONTEND_DIR = Path(__file__).parent.parent / "frontend"

if not DB_PATH.exists():
    print("Database not found. Running ingestion...")
    from backend.ingest import run_ingestion
    run_ingestion()

from backend.graph import get_graph, graph_to_json, get_node_details, search_nodes
from backend import llm as llm_module

app = FastAPI(
    title="SAP O2C Graph System",
    description="Context graph system with LLM-powered query interface for SAP Order-to-Cash data",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Startup ───────────────────────────────────────────────────────────────────
@app.on_event("startup")
async def startup_event():
    """Pre-build graph on startup."""
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, get_graph)


# ── Pydantic Models ───────────────────────────────────────────────────────────
class ChatMessage(BaseModel):
    role: str  # "user" or "assistant"
    content: str


class ChatRequest(BaseModel):
    message: str
    history: Optional[List[ChatMessage]] = []


# ── API Routes ────────────────────────────────────────────────────────────────
@app.get("/api/graph")
async def get_full_graph(limit: int = Query(default=1500, le=3000)):
    """Returns nodes and edges for the graph visualization."""
    G = get_graph()
    return graph_to_json(G, limit=limit)


@app.get("/api/graph/stats")
async def get_graph_stats():
    """Returns statistics about the graph."""
    G = get_graph()
    from collections import Counter
    type_counts = Counter(data.get("type", "Unknown") for _, data in G.nodes(data=True))
    edge_counts = Counter(data.get("label", "") for _, _, data in G.edges(data=True))
    return {
        "total_nodes": G.number_of_nodes(),
        "total_edges": G.number_of_edges(),
        "node_types": dict(type_counts),
        "edge_types": dict(edge_counts),
    }


@app.get("/api/node/{node_id:path}")
async def get_node(node_id: str):
    """Returns detailed info about a node including its neighbors."""
    details = get_node_details(node_id)
    if not details:
        raise HTTPException(status_code=404, detail=f"Node '{node_id}' not found")
    return details


@app.get("/api/search")
async def search(q: str = Query(..., min_length=1), limit: int = 20):
    """Fuzzy search over node labels and properties."""
    results = search_nodes(q, limit=limit)
    return {"results": results, "count": len(results)}


@app.post("/api/chat")
async def chat_endpoint(request: ChatRequest):
    """Natural language query → SQL → answer."""
    history = [{"role": m.role, "content": m.content} for m in (request.history or [])]
    try:
        result = llm_module.chat(request.message, history=history)
        return result
    except ValueError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"LLM error: {str(e)}")


@app.post("/api/chat/stream")
async def chat_stream(request: ChatRequest):
    """Streaming version of chat endpoint using SSE."""
    history = [{"role": m.role, "content": m.content} for m in (request.history or [])]

    async def generate():
        try:
            # Run in thread executor to not block event loop
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None, llm_module.chat, request.message, history
            )
            # Stream answer word by word for effect
            answer = result["answer"]
            words = answer.split(" ")
            for i, word in enumerate(words):
                chunk = word + (" " if i < len(words) - 1 else "")
                yield f"data: {json.dumps({'type': 'token', 'content': chunk})}\n\n"
                await asyncio.sleep(0.02)

            # Send metadata last
            yield f"data: {json.dumps({'type': 'done', 'sql': result.get('sql'), 'data': result.get('data'), 'highlighted_nodes': result.get('highlighted_nodes', []), 'rejected': result.get('rejected', False)})}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'type': 'error', 'content': str(e)})}\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream")


# ── Serve Frontend ────────────────────────────────────────────────────────────
if FRONTEND_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(FRONTEND_DIR)), name="static")

    @app.get("/", response_class=HTMLResponse)
    async def serve_frontend():
        index_path = FRONTEND_DIR / "index.html"
        return HTMLResponse(content=index_path.read_text(), status_code=200)


# ── Dev entry point ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("backend.main:app", host="0.0.0.0", port=8000, reload=True)
