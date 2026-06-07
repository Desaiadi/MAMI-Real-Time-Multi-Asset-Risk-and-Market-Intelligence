"""
MAMI — Run the local demo server.
  pip install -r requirements.txt
  python run.py
  → http://localhost:8000
"""
import os
import uvicorn

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    print()
    print("=" * 58)
    print("  MAMI  ·  Real-Time Risk & Market Intelligence")
    print("=" * 58)
    print(f"  Dashboard  →  http://localhost:{port}")
    print(f"  API docs   →  http://localhost:{port}/docs")
    print(f"  Trigger a flash crash:")
    print(f"    curl -X POST http://localhost:{port}/api/v1/trigger-crash")
    print("=" * 58)
    print()
    uvicorn.run(
        "src.api:app",
        host="0.0.0.0",
        port=port,
        reload=False,
        log_level="warning",
    )
