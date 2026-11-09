from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Sequence

from sqlalchemy import Engine, create_engine, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from ingestguard.models import (
    Base,
    IngestedRecordRow,
    IngestJobRow,
    QuarantineRow,
)
from ingestguard.parser import RejectedRow, ValidatedRow


@dataclass(frozen=True)
class ChunkApplyResult:
    inserted: int
    updated: int
    skipped: int
    quarantined: int


def create_db_engine(db_url: str = "sqlite:///:memory:") -> Engine:
    """Create a configured SQLAlchemy engine supporting SQLite and other dialects."""
    if db_url in (":memory:", "sqlite:///:memory:"):
        return create_engine(
            "sqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
            echo=False,
        )
    if db_url.startswith("sqlite"):
        return create_engine(
            db_url,
            connect_args={"check_same_thread": False},
            echo=False,
        )
    return create_engine(db_url, echo=False)


class IngestStore:
    """Lightweight session-scoped store supporting atomic upserts and async calls."""

    def __init__(self, engine: Engine | None = None, db_url: str = "sqlite:///:memory:") -> None:
        self.engine: Engine = engine or create_db_engine(db_url)
        self.session_factory: sessionmaker[Session] = sessionmaker(
            bind=self.engine,
            expire_on_commit=False,
        )
        self._lock = threading.Lock()

    def init_db(self) -> None:
        """Create all tables if they do not exist."""
        Base.metadata.create_all(bind=self.engine)

    async def create_job(self, job_id: str, dataset: str, format_type: str) -> IngestJobRow:
        with self.session_factory() as session:
            with session.begin():
                job = IngestJobRow(
                    job_id=job_id,
                    dataset=dataset,
                    format_type=format_type,
                    created_at=datetime.now(timezone.utc),
                )
                session.add(job)
            return job

    async def apply_chunk(
        self,
        job_id: str,
        dataset: str,
        validated_rows: list[ValidatedRow],
        rejected_rows: list[RejectedRow],
    ) -> ChunkApplyResult:
        with self.session_factory() as session:
            with session.begin():
                if rejected_rows:
                    now = datetime.now(timezone.utc)
                    session.add_all([
                        QuarantineRow(
                            quarantine_id=str(uuid.uuid4()),
                            job_id=job_id,
                            dataset=dataset,
                            row_number=r.row_number,
                            raw_payload_json=r.raw_payload,
                            error_details_json=r.errors,
                            status="PENDING",
                            created_at=now,
                        )
                        for r in rejected_rows
                    ])

                inserted = 0
                updated = 0
                skipped = 0

                if validated_rows:
                    keys = [v.record_key for v in validated_rows]
                    stmt = select(
                        IngestedRecordRow.record_key,
                        IngestedRecordRow.payload_hash,
                    ).where(
                        IngestedRecordRow.dataset == dataset,
                        IngestedRecordRow.record_key.in_(keys),
                    )
                    existing_hashes: dict[str, str] = {
                        row.record_key: row.payload_hash for row in session.execute(stmt)
                    }
                    now = datetime.now(timezone.utc)

                    to_upsert: list[dict[str, Any]] = []
                    for v in validated_rows:
                        old_hash = existing_hashes.get(v.record_key)
                        if old_hash is None:
                            inserted += 1
                            existing_hashes[v.record_key] = v.payload_hash
                        elif old_hash == v.payload_hash:
                            skipped += 1
                            continue
                        else:
                            updated += 1
                            existing_hashes[v.record_key] = v.payload_hash

                        to_upsert.append({
                            "dataset": dataset,
                            "record_key": v.record_key,
                            "payload_hash": v.payload_hash,
                            "payload_json": v.payload,
                            "source_job_id": job_id,
                            "updated_at": now,
                        })

                    if to_upsert:
                        upsert_stmt = sqlite_insert(IngestedRecordRow).values(to_upsert)
                        upsert_stmt = upsert_stmt.on_conflict_do_update(
                            index_elements=["dataset", "record_key"],
                            set_={
                                "payload_hash": upsert_stmt.excluded.payload_hash,
                                "payload_json": upsert_stmt.excluded.payload_json,
                                "source_job_id": upsert_stmt.excluded.source_job_id,
                                "updated_at": upsert_stmt.excluded.updated_at,
                            },
                        )
                        session.execute(upsert_stmt)

                return ChunkApplyResult(
                    inserted=inserted,
                    updated=updated,
                    skipped=skipped,
                    quarantined=len(rejected_rows),
                )

    async def finalize_job(
        self,
        job_id: str,
        inserted: int,
        updated: int,
        skipped: int,
        quarantined: int,
    ) -> IngestJobRow:
        with self.session_factory() as session:
            with session.begin():
                job = session.get(IngestJobRow, job_id)
                if job is None:
                    raise ValueError(f"Job not found: {job_id}")
                job.total_rows = inserted + updated + skipped + quarantined
                job.inserted_rows = inserted
                job.updated_rows = updated
                job.skipped_rows = skipped
                job.quarantined_rows = quarantined
                job.status = "PARTIAL_QUARANTINE" if quarantined > 0 else "COMPLETED"
            return job

    async def get_job(self, job_id: str) -> IngestJobRow | None:
        with self.session_factory() as session:
            return session.get(IngestJobRow, job_id)

    async def get_quarantine_rows(self, job_id: str) -> list[QuarantineRow]:
        with self.session_factory() as session:
            stmt = (
                select(QuarantineRow)
                .where(QuarantineRow.job_id == job_id)
                .order_by(QuarantineRow.row_number.asc())
            )
            return list(session.scalars(stmt))

    async def get_quarantine_row(self, quarantine_id: str) -> QuarantineRow | None:
        with self.session_factory() as session:
            return session.get(QuarantineRow, quarantine_id)

    async def get_records(
        self, dataset: str, limit: int = 100, offset: int = 0
    ) -> Sequence[IngestedRecordRow]:
        with self.session_factory() as session:
            stmt = (
                select(IngestedRecordRow)
                .where(IngestedRecordRow.dataset == dataset)
                .order_by(IngestedRecordRow.id.asc())
                .limit(limit)
                .offset(offset)
            )
            return list(session.scalars(stmt))
