"""
ingest.py — Ingests all SAP O2C JSONL files into a SQLite database.
Run once at startup or manually: python backend/ingest.py
"""
import json
import sqlite3
import os
import glob
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "sap-o2c-data"
DB_PATH = Path(__file__).parent.parent / "o2c.db"

# Map of directory name → SQLite table name
TABLE_MAP = {
    "sales_order_headers": "sales_order_headers",
    "sales_order_items": "sales_order_items",
    "sales_order_schedule_lines": "sales_order_schedule_lines",
    "outbound_delivery_headers": "outbound_delivery_headers",
    "outbound_delivery_items": "outbound_delivery_items",
    "billing_document_headers": "billing_document_headers",
    "billing_document_items": "billing_document_items",
    "billing_document_cancellations": "billing_document_cancellations",
    "journal_entry_items_accounts_receivable": "journal_entries",
    "payments_accounts_receivable": "payments",
    "business_partners": "business_partners",
    "business_partner_addresses": "business_partner_addresses",
    "customer_company_assignments": "customer_company_assignments",
    "customer_sales_area_assignments": "customer_sales_area_assignments",
    "products": "products",
    "product_descriptions": "product_descriptions",
    "product_plants": "product_plants",
    "product_storage_locations": "product_storage_locations",
    "plants": "plants",
}


def flatten_value(v):
    """Flatten nested dicts/objects to string."""
    if v is None:
        return None
    if isinstance(v, dict):
        return json.dumps(v)
    return v


def ingest_directory(conn: sqlite3.Connection, dir_name: str, table_name: str):
    dir_path = DATA_DIR / dir_name
    if not dir_path.exists():
        print(f"  [SKIP] {dir_name} not found")
        return 0

    files = list(dir_path.glob("*.jsonl"))
    if not files:
        print(f"  [SKIP] {dir_name} has no .jsonl files")
        return 0

    rows = []
    columns = set()

    for f in files:
        with open(f, "r") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                    flat = {k: flatten_value(v) for k, v in record.items()}
                    rows.append(flat)
                    columns.update(flat.keys())
                except json.JSONDecodeError:
                    continue

    if not rows:
        return 0

    columns = sorted(columns)

    # Create table
    col_defs = ", ".join(f'"{c}" TEXT' for c in columns)
    conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
    conn.execute(f'CREATE TABLE "{table_name}" ({col_defs})')

    # Insert rows
    placeholders = ", ".join("?" for _ in columns)
    col_names = ", ".join(f'"{c}"' for c in columns)
    for row in rows:
        vals = [str(row.get(c)) if row.get(c) is not None else None for c in columns]
        conn.execute(
            f'INSERT INTO "{table_name}" ({col_names}) VALUES ({placeholders})',
            vals,
        )

    conn.commit()
    print(f"  [OK] {table_name}: {len(rows)} rows, {len(columns)} columns")
    return len(rows)


def create_indexes(conn: sqlite3.Connection):
    indexes = [
        ("sales_order_headers", "salesOrder"),
        ("sales_order_items", "salesOrder"),
        ("sales_order_items", "material"),
        ("outbound_delivery_headers", "deliveryDocument"),
        ("outbound_delivery_items", "deliveryDocument"),
        ("outbound_delivery_items", "referenceSdDocument"),
        ("billing_document_headers", "billingDocument"),
        ("billing_document_headers", "soldToParty"),
        ("billing_document_items", "billingDocument"),
        ("billing_document_items", "referenceSdDocument"),
        ("journal_entries", "accountingDocument"),
        ("journal_entries", "referenceDocument"),
        ("payments", "accountingDocument"),
        ("business_partners", "businessPartner"),
        ("products", "product"),
    ]
    for table, col in indexes:
        try:
            conn.execute(
                f'CREATE INDEX IF NOT EXISTS "idx_{table}_{col}" ON "{table}" ("{col}")'
            )
        except Exception as e:
            pass
    conn.commit()
    print("  [OK] Indexes created")


def run_ingestion():
    print(f"Ingesting data from {DATA_DIR} into {DB_PATH}...")
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    total = 0
    for dir_name, table_name in TABLE_MAP.items():
        total += ingest_directory(conn, dir_name, table_name)

    create_indexes(conn)
    conn.close()
    print(f"\nIngestion complete. Total rows: {total}")


if __name__ == "__main__":
    run_ingestion()
