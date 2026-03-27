import os

import requests


def main() -> None:
    base_url = os.getenv("APP_BASE_URL", "http://localhost:8000")
    token = os.getenv("INGEST_TOKEN")

    if not token:
        raise RuntimeError("INGEST_TOKEN is required")

    response = requests.post(
        f"{base_url}/ingest/run",
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )
    response.raise_for_status()
    print("Ingestion trigger succeeded")


if __name__ == "__main__":
    main()
