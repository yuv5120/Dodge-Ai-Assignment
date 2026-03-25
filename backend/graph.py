"""
graph.py — Builds a NetworkX DiGraph from SQLite data.
Provides utilities to export graph data for the API.
"""
import sqlite3
import networkx as nx
from pathlib import Path
from typing import Dict, List, Any, Optional

DB_PATH = Path(__file__).parent.parent / "o2c.db"

# Node type colors (used by frontend)
NODE_COLORS = {
    "SalesOrder": "#4F8EF7",
    "SalesOrderItem": "#82AAFF",
    "Delivery": "#42D392",
    "DeliveryItem": "#7DEFA1",
    "BillingDoc": "#FF6B6B",
    "BillingItem": "#FFA07A",
    "JournalEntry": "#FFD93D",
    "Payment": "#6BCB77",
    "Customer": "#C77DFF",
    "Product": "#FF9F1C",
    "Plant": "#2EC4B6",
    "Address": "#AAAAAA",
}

_graph: Optional[nx.DiGraph] = None


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def build_graph() -> nx.DiGraph:
    global _graph
    print("Building graph from SQLite...")
    G = nx.DiGraph()
    conn = get_db()

    # ── Sales Orders ───────────────────────────────────────────────────────────
    rows = conn.execute(
        "SELECT salesOrder, soldToParty, totalNetAmount, overallDeliveryStatus, "
        "transactionCurrency, creationDate FROM sales_order_headers"
    ).fetchall()
    for r in rows:
        G.add_node(
            f"SO_{r['salesOrder']}",
            label=f"SO {r['salesOrder']}",
            type="SalesOrder",
            color=NODE_COLORS["SalesOrder"],
            properties={
                "SalesOrder": r["salesOrder"],
                "SoldToParty": r["soldToParty"],
                "TotalNetAmount": r["totalNetAmount"],
                "DeliveryStatus": r["overallDeliveryStatus"],
                "Currency": r["transactionCurrency"],
                "CreationDate": r["creationDate"],
            },
        )

    # ── Sales Order Items ──────────────────────────────────────────────────────
    rows = conn.execute(
        "SELECT salesOrder, salesOrderItem, material, requestedQuantity, "
        "requestedQuantityUnit, netAmount FROM sales_order_items"
    ).fetchall()
    for r in rows:
        nid = f"SOI_{r['salesOrder']}_{r['salesOrderItem']}"
        G.add_node(
            nid,
            label=f"Item {r['salesOrderItem']}",
            type="SalesOrderItem",
            color=NODE_COLORS["SalesOrderItem"],
            properties={
                "SalesOrder": r["salesOrder"],
                "Item": r["salesOrderItem"],
                "Material": r["material"],
                "Quantity": r["requestedQuantity"],
                "Unit": r["requestedQuantityUnit"],
                "NetAmount": r["netAmount"],
            },
        )
        so_nid = f"SO_{r['salesOrder']}"
        if G.has_node(so_nid):
            G.add_edge(so_nid, nid, label="HAS_ITEM")

    # ── Customers (Business Partners) ─────────────────────────────────────────
    rows = conn.execute(
        "SELECT businessPartner, businessPartnerFullName, businessPartnerCategory "
        "FROM business_partners"
    ).fetchall()
    for r in rows:
        nid = f"CUST_{r['businessPartner']}"
        G.add_node(
            nid,
            label=r["businessPartnerFullName"] or f"Customer {r['businessPartner']}",
            type="Customer",
            color=NODE_COLORS["Customer"],
            properties={
                "BusinessPartner": r["businessPartner"],
                "Name": r["businessPartnerFullName"],
                "Category": r["businessPartnerCategory"],
            },
        )

    # SO → Customer edges
    rows = conn.execute("SELECT salesOrder, soldToParty FROM sales_order_headers").fetchall()
    for r in rows:
        so_nid = f"SO_{r['salesOrder']}"
        cust_nid = f"CUST_{r['soldToParty']}"
        if G.has_node(so_nid) and G.has_node(cust_nid):
            G.add_edge(so_nid, cust_nid, label="SOLD_TO")

    # ── Products ──────────────────────────────────────────────────────────────
    rows = conn.execute("SELECT product FROM products").fetchall()
    for r in rows:
        nid = f"PROD_{r['product']}"
        G.add_node(
            nid,
            label=f"Product {r['product']}",
            type="Product",
            color=NODE_COLORS["Product"],
            properties={"Product": r["product"]},
        )

    # Product descriptions
    desc_rows = conn.execute(
        "SELECT product, productDescription FROM product_descriptions WHERE language='EN' OR language=''"
    ).fetchall()
    desc_map = {r["product"]: r["productDescription"] for r in desc_rows}
    for nid, data in G.nodes(data=True):
        if data.get("type") == "Product":
            prod_id = data["properties"]["Product"]
            if prod_id in desc_map:
                data["label"] = desc_map[prod_id][:30]
                data["properties"]["Description"] = desc_map[prod_id]

    # SOI → Product edges
    rows = conn.execute(
        "SELECT salesOrder, salesOrderItem, material FROM sales_order_items"
    ).fetchall()
    for r in rows:
        soi_nid = f"SOI_{r['salesOrder']}_{r['salesOrderItem']}"
        prod_nid = f"PROD_{r['material']}"
        if G.has_node(soi_nid) and G.has_node(prod_nid):
            G.add_edge(soi_nid, prod_nid, label="FOR_PRODUCT")

    # ── Deliveries ────────────────────────────────────────────────────────────
    rows = conn.execute(
        "SELECT deliveryDocument, shippingPoint, overallGoodsMovementStatus, "
        "overallPickingStatus, creationDate FROM outbound_delivery_headers"
    ).fetchall()
    for r in rows:
        nid = f"DEL_{r['deliveryDocument']}"
        G.add_node(
            nid,
            label=f"Delivery {r['deliveryDocument']}",
            type="Delivery",
            color=NODE_COLORS["Delivery"],
            properties={
                "DeliveryDocument": r["deliveryDocument"],
                "ShippingPoint": r["shippingPoint"],
                "GoodsMovementStatus": r["overallGoodsMovementStatus"],
                "PickingStatus": r["overallPickingStatus"],
                "CreationDate": r["creationDate"],
            },
        )

    # ── Delivery Items + edges to SO Items ────────────────────────────────────
    rows = conn.execute(
        "SELECT deliveryDocument, deliveryDocumentItem, plant, referenceSdDocument, "
        "referenceSdDocumentItem, actualDeliveryQuantity FROM outbound_delivery_items"
    ).fetchall()
    # Build SOI→Delivery map for linking
    soi_to_del = {}
    for r in rows:
        nid = f"DELI_{r['deliveryDocument']}_{r['deliveryDocumentItem']}"
        G.add_node(
            nid,
            label=f"DelItem {r['deliveryDocumentItem']}",
            type="DeliveryItem",
            color=NODE_COLORS["DeliveryItem"],
            properties={
                "DeliveryDocument": r["deliveryDocument"],
                "Item": r["deliveryDocumentItem"],
                "Plant": r["plant"],
                "ReferenceSO": r["referenceSdDocument"],
                "ReferenceSOItem": r["referenceSdDocumentItem"],
                "Quantity": r["actualDeliveryQuantity"],
            },
        )
        del_nid = f"DEL_{r['deliveryDocument']}"
        if G.has_node(del_nid):
            G.add_edge(del_nid, nid, label="HAS_ITEM")

        # Link delivery item → SO item
        so_ref = r["referenceSdDocument"]
        soi_ref = r["referenceSdDocumentItem"]
        if so_ref and soi_ref:
            soi_ref_clean = str(soi_ref).lstrip("0") or "0"
            soi_nid = f"SOI_{so_ref}_{soi_ref_clean}"
            # Try padded version too
            if G.has_node(soi_nid):
                G.add_edge(soi_nid, del_nid, label="DELIVERED_BY")
            else:
                # Try with leading zeros stripped
                soi_nid2 = f"SOI_{so_ref}_{int(soi_ref):d}" if soi_ref.isdigit() else None
                if soi_nid2 and G.has_node(soi_nid2):
                    G.add_edge(soi_nid2, del_nid, label="DELIVERED_BY")

    # ── Plants ────────────────────────────────────────────────────────────────
    rows = conn.execute("SELECT plant, plantName FROM plants").fetchall()
    for r in rows:
        nid = f"PLANT_{r['plant']}"
        G.add_node(
            nid,
            label=r["plantName"] or f"Plant {r['plant']}",
            type="Plant",
            color=NODE_COLORS["Plant"],
            properties={"Plant": r["plant"], "Name": r["plantName"]},
        )

    # DeliveryItem → Plant
    for nid, data in G.nodes(data=True):
        if data.get("type") == "DeliveryItem":
            plant = data["properties"].get("Plant")
            plant_nid = f"PLANT_{plant}"
            if plant and G.has_node(plant_nid):
                G.add_edge(nid, plant_nid, label="AT_PLANT")

    # ── Billing Documents ─────────────────────────────────────────────────────
    rows = conn.execute(
        "SELECT billingDocument, billingDocumentType, totalNetAmount, "
        "transactionCurrency, creationDate, soldToParty, accountingDocument, "
        "billingDocumentIsCancelled FROM billing_document_headers"
    ).fetchall()
    for r in rows:
        nid = f"BILL_{r['billingDocument']}"
        G.add_node(
            nid,
            label=f"Billing {r['billingDocument']}",
            type="BillingDoc",
            color=NODE_COLORS["BillingDoc"],
            properties={
                "BillingDocument": r["billingDocument"],
                "Type": r["billingDocumentType"],
                "TotalNetAmount": r["totalNetAmount"],
                "Currency": r["transactionCurrency"],
                "CreationDate": r["creationDate"],
                "SoldToParty": r["soldToParty"],
                "AccountingDocument": r["accountingDocument"],
                "IsCancelled": r["billingDocumentIsCancelled"],
            },
        )
        # Link billing → customer
        cust_nid = f"CUST_{r['soldToParty']}"
        if G.has_node(cust_nid):
            G.add_edge(nid, cust_nid, label="BILLED_TO")

    # ── Billing Items + edges to Delivery Items ───────────────────────────────
    rows = conn.execute(
        "SELECT billingDocument, billingDocumentItem, material, billingQuantity, "
        "netAmount, referenceSdDocument, referenceSdDocumentItem FROM billing_document_items"
    ).fetchall()
    for r in rows:
        nid = f"BILLI_{r['billingDocument']}_{r['billingDocumentItem']}"
        G.add_node(
            nid,
            label=f"BillItem {r['billingDocumentItem']}",
            type="BillingItem",
            color=NODE_COLORS["BillingItem"],
            properties={
                "BillingDocument": r["billingDocument"],
                "Item": r["billingDocumentItem"],
                "Material": r["material"],
                "Quantity": r["billingQuantity"],
                "NetAmount": r["netAmount"],
                "ReferenceDelivery": r["referenceSdDocument"],
                "ReferenceDeliveryItem": r["referenceSdDocumentItem"],
            },
        )
        bill_nid = f"BILL_{r['billingDocument']}"
        if G.has_node(bill_nid):
            G.add_edge(bill_nid, nid, label="HAS_ITEM")

        # Link billing item → delivery item
        del_ref = r["referenceSdDocument"]
        del_item_ref = r["referenceSdDocumentItem"]
        if del_ref and del_item_ref:
            del_item_nid = f"DELI_{del_ref}_{del_item_ref}"
            if G.has_node(del_item_nid):
                G.add_edge(del_item_nid, nid, label="BILLED_AS")

    # ── Journal Entries ───────────────────────────────────────────────────────
    rows = conn.execute(
        "SELECT accountingDocument, fiscalYear, companyCode, glAccount, "
        "referenceDocument, amountInTransactionCurrency, transactionCurrency, "
        "postingDate, accountingDocumentType FROM journal_entries LIMIT 5000"
    ).fetchall()
    for r in rows:
        nid = f"JE_{r['accountingDocument']}_{r['fiscalYear']}"
        if not G.has_node(nid):
            G.add_node(
                nid,
                label=f"Journal {r['accountingDocument']}",
                type="JournalEntry",
                color=NODE_COLORS["JournalEntry"],
                properties={
                    "AccountingDocument": r["accountingDocument"],
                    "FiscalYear": r["fiscalYear"],
                    "CompanyCode": r["companyCode"],
                    "GlAccount": r["glAccount"],
                    "ReferenceDocument": r["referenceDocument"],
                    "AmountInTransactionCurrency": r["amountInTransactionCurrency"],
                    "TransactionCurrency": r["transactionCurrency"],
                    "PostingDate": r["postingDate"],
                    "AccountingDocumentType": r["accountingDocumentType"],
                },
            )
        # Link billing → journal entry via accountingDocument
        ref_doc = r["referenceDocument"]
        if ref_doc:
            bill_nid = f"BILL_{ref_doc}"
            if G.has_node(bill_nid):
                G.add_edge(bill_nid, nid, label="POSTS_TO")

    # ── Payments ──────────────────────────────────────────────────────────────
    rows = conn.execute(
        "SELECT accountingDocument, fiscalYear, clearingAccountingDocument, "
        "amountInTransactionCurrency, transactionCurrency, clearingDate, customer "
        "FROM payments LIMIT 5000"
    ).fetchall()
    seen_payments = set()
    for r in rows:
        nid = f"PAY_{r['clearingAccountingDocument']}_{r['fiscalYear']}"
        if nid not in seen_payments:
            seen_payments.add(nid)
            G.add_node(
                nid,
                label=f"Payment {r['clearingAccountingDocument']}",
                type="Payment",
                color=NODE_COLORS["Payment"],
                properties={
                    "ClearingDocument": r["clearingAccountingDocument"],
                    "FiscalYear": r["fiscalYear"],
                    "Amount": r["amountInTransactionCurrency"],
                    "Currency": r["transactionCurrency"],
                    "ClearingDate": r["clearingDate"],
                    "Customer": r["customer"],
                },
            )
        # Link journal entry → payment
        je_nid = f"JE_{r['accountingDocument']}_{r['fiscalYear']}"
        if G.has_node(je_nid):
            G.add_edge(je_nid, nid, label="CLEARED_BY")

    conn.close()
    print(
        f"Graph built: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges"
    )
    _graph = G
    return G


def get_graph() -> nx.DiGraph:
    global _graph
    if _graph is None:
        _graph = build_graph()
    return _graph


def graph_to_json(G: nx.DiGraph, limit: int = 2000):
    """Convert graph to JSON-serializable format for the frontend."""
    nodes = []
    edges = []

    node_list = list(G.nodes(data=True))
    # Prioritize by node type importance order
    type_order = {
        "SalesOrder": 0, "Customer": 1, "Delivery": 2,
        "BillingDoc": 3, "JournalEntry": 4, "Payment": 5,
        "Product": 6, "Plant": 7,
        "SalesOrderItem": 8, "DeliveryItem": 9,
        "BillingItem": 10, "Address": 11
    }
    node_list.sort(key=lambda x: type_order.get(x[1].get("type", ""), 99))
    selected_nodes = {nid for nid, _ in node_list[:limit]}

    for nid, data in node_list[:limit]:
        nodes.append({
            "id": nid,
            "label": data.get("label", nid),
            "type": data.get("type", "Unknown"),
            "color": data.get("color", "#888"),
            "properties": data.get("properties", {}),
            "connections": G.degree(nid),
        })

    for u, v, data in G.edges(data=True):
        if u in selected_nodes and v in selected_nodes:
            edges.append({
                "source": u,
                "target": v,
                "label": data.get("label", ""),
            })

    return {"nodes": nodes, "edges": edges}


def get_node_details(node_id: str):
    G = get_graph()
    if not G.has_node(node_id):
        return None
    data = dict(G.nodes[node_id])
    neighbors = []
    for pred in G.predecessors(node_id):
        edge_data = G.edges[pred, node_id]
        pred_data = G.nodes[pred]
        neighbors.append({
            "id": pred,
            "label": pred_data.get("label", pred),
            "type": pred_data.get("type", ""),
            "color": pred_data.get("color", "#888"),
            "direction": "incoming",
            "edge_label": edge_data.get("label", ""),
        })
    for succ in G.successors(node_id):
        edge_data = G.edges[node_id, succ]
        succ_data = G.nodes[succ]
        neighbors.append({
            "id": succ,
            "label": succ_data.get("label", succ),
            "type": succ_data.get("type", ""),
            "color": succ_data.get("color", "#888"),
            "direction": "outgoing",
            "edge_label": edge_data.get("label", ""),
        })
    return {
        "id": node_id,
        "label": data.get("label", node_id),
        "type": data.get("type", ""),
        "color": data.get("color", "#888"),
        "properties": data.get("properties", {}),
        "connections": G.degree(node_id),
        "neighbors": neighbors,
    }


def search_nodes(query: str, limit: int = 20) -> List[Dict[str, Any]]:
    G = get_graph()
    q = query.lower()
    results = []
    for nid, data in G.nodes(data=True):
        label = data.get("label", "").lower()
        props = str(data.get("properties", {})).lower()
        if q in label or q in nid.lower() or q in props:
            results.append({
                "id": nid,
                "label": data.get("label", nid),
                "type": data.get("type", ""),
                "color": data.get("color", "#888"),
            })
        if len(results) >= limit:
            break
    return results
