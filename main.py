import os
import json
import sqlite3
import uvicorn
from datetime import datetime
from typing import List, Dict, Any
from fastapi import FastAPI, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

DB_FILE = "pipeline_storage.db"

# ==========================================
# 1. FastAPI APPLICATION CONFIGURATION
# ==========================================
app = FastAPI(
    title="AI Integration Pipeline Engine",
    description="Production-grade asynchronous ingestion gateway exposing local storage systems.",
    version="1.0.0"
)

# Cross-Origin Resource Sharing (CORS) Configuration for Cloud Client Security
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class IngestLogPayload(BaseModel):
    id: int
    timestamp: str
    level: str = Field(..., max_length=10)
    msg: str = Field(..., min_length=3)

# ==========================================
# 2. CORE MICROSERVICE ENDPOINTS
# ==========================================
@app.post("/logs/ingest", status_code=status.HTTP_201_CREATED)
async def ingest_pipeline_gateway(payload: List[Dict[str, Any]]):
    """Asynchronously validates data batches and persists them safely into SQLite tables."""
    clean_count = 0
    quarantine_count = 0
    current_time = datetime.now().isoformat()

    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        for item in payload:
            try:
                # Direct JSON-to-Schema Validation Contract
                validated = IngestLogPayload(**item)
                cursor.execute(
                    """INSERT OR REPLACE INTO processed_logs
                       (log_id, log_timestamp, log_level, log_message, ingested_at)
                       VALUES (?, ?, ?, ?, ?)""",
                    (validated.id, validated.timestamp, validated.level.upper(), validated.msg, current_time)
                )
                clean_count += 1
            except Exception as schema_err:
                # Automated DLQ Quarantine routing pattern
                cursor.execute(
                    """INSERT INTO quarantined_dlq (raw_payload, error_details, quarantined_at)
                       VALUES (?, ?, ?)""",
                    (json.dumps(item), str(schema_err), current_time)
                )
                quarantine_count += 1
        conn.commit()

    return {
        "status": "batch_completed",
        "processed_clean": clean_count,
        "quarantined_dlq": quarantine_count
    }

@app.get("/analytics", status_code=status.HTTP_200_OK)
async def get_system_telemetry():
    """Queries relational engines to pull real-time ingestion monitoring numbers."""
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        clean_total = cursor.execute("SELECT COUNT(*) FROM processed_logs").fetchone()[0]
        dlq_total = cursor.execute("SELECT COUNT(*) FROM quarantined_dlq").fetchone()[0]

    return {
        "engine_state": "HEALTHY",
        "total_records_saved": clean_total,
        "dead_letter_queue_size": dlq_total,
        "timestamp": datetime.now().isoformat()
    }

# ==========================================
# 3. DYNAMIC PRODUCTION RUNNER INITIALIZATION
# ==========================================
if __name__ == "__main__":
    # Dynamically extract bound ports passed by Cloud Provider environment configurations
    production_port = int(os.environ.get("PORT", 8000))

    # Establish strict table schema structures on storage disk if not active
    with sqlite3.connect(DB_FILE) as boot_conn:
        boot_conn.execute("CREATE TABLE IF NOT EXISTS processed_logs (log_id INTEGER PRIMARY KEY, log_timestamp TEXT, log_level TEXT, log_message TEXT, ingested_at TEXT)")
        boot_conn.execute("CREATE TABLE IF NOT EXISTS quarantined_dlq (id INTEGER PRIMARY KEY AUTOINCREMENT, raw_payload TEXT, error_details TEXT, quarantined_at TEXT)")
        boot_conn.execute("CREATE INDEX IF NOT EXISTS idx_log_level ON processed_logs(log_level)")

    print(f"[BOOT] Initializing Multi-Worker Production Web Application Layer on Port: {production_port}")
    uvicorn.run("main:app", host="0.0.0.0", port=production_port, workers=4)
