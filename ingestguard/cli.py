from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Sequence

import typer
import uvicorn

from ingestguard.app import create_app
from ingestguard.models import IngestRequest, SchemaSpec
from ingestguard.replayer import IngestService, ReplayValidationError
from ingestguard.store import IngestStore

app = typer.Typer(
    name="ingestguard",
    help="Streaming chunk ingestion, idempotent upsert, and quarantine replay CLI.",
    add_completion=False,
)


@app.command()
def serve(
    host: str = typer.Option("0.0.0.0", "--host", help="Host address (default: 0.0.0.0)"),
    port: int = typer.Option(8000, "--port", help="Port (default: 8000)"),
    db_url: str = typer.Option(
        "sqlite:///:memory:",
        "--db-url",
        help="Database connection URL (default: sqlite:///:memory:)",
    ),
) -> None:
    """Run the IngestGuard FastAPI server."""
    fastapi_app = create_app(db_url=db_url)
    uvicorn.run(fastapi_app, host=host, port=port)


@app.command()
def ingest(
    dataset: str = typer.Option(..., "--dataset", help="Target dataset name"),
    format_type: str = typer.Option(..., "--format", help="Input format: csv | jsonl"),
    spec: Path = typer.Option(..., "--spec", help="Path to JSON SchemaSpec file"),
    input_file: Path = typer.Option(..., "--input", help="Path to input data file"),
    chunk_size: int = typer.Option(200, "--chunk-size", help="Chunk size"),
    db_url: str = typer.Option(
        "sqlite:///ingestguard.db",
        "--db-url",
        help="Database connection URL (default: sqlite:///ingestguard.db)",
    ),
) -> None:
    """Ingest a CSV or JSONL batch directly."""
    schema_spec = SchemaSpec.model_validate_json(spec.read_text(encoding="utf-8"))
    data_content = input_file.read_text(encoding="utf-8")

    store = IngestStore(db_url=db_url)
    store.init_db()
    service = IngestService(store=store)

    req = IngestRequest(
        dataset=dataset,
        format_type=format_type,  # type: ignore[arg-type]
        schema_spec=schema_spec,
        content=data_content,
        chunk_size=chunk_size,
    )
    summary = asyncio.run(service.ingest_batch(req))
    typer.echo(json.dumps(summary.model_dump(), indent=2, ensure_ascii=False))


@app.command()
def replay(
    quarantine_id: str = typer.Option(..., "--id", help="Quarantine ID"),
    spec: Path = typer.Option(..., "--spec", help="Path to JSON SchemaSpec file"),
    payload: Path = typer.Option(..., "--payload", help="Path to patched JSON payload"),
    db_url: str = typer.Option(
        "sqlite:///ingestguard.db",
        "--db-url",
        help="Database connection URL (default: sqlite:///ingestguard.db)",
    ),
) -> None:
    """Replay a quarantined row with a patch."""
    schema_spec = SchemaSpec.model_validate_json(spec.read_text(encoding="utf-8"))
    patched_payload = json.loads(payload.read_text(encoding="utf-8"))

    store = IngestStore(db_url=db_url)
    store.init_db()
    service = IngestService(store=store)

    try:
        record = asyncio.run(
            service.replay_quarantine_row(
                quarantine_id=quarantine_id,
                patched_payload=patched_payload,
                spec=schema_spec,
            )
        )
        typer.echo(
            json.dumps(
                {
                    "status": "REPLAYED",
                    "record_id": record.id,
                    "record_key": record.record_key,
                    "updated_at": record.updated_at.isoformat(),
                },
                indent=2,
            )
        )
    except ReplayValidationError as exc:
        typer.echo(f"Validation failed: {exc.errors}", err=True)
        raise typer.Exit(code=1) from exc


def main(argv: Sequence[str] | None = None) -> None:
    if argv is None:
        app()
    else:
        app(args=list(argv), standalone_mode=False)


if __name__ == "__main__":
    main()
