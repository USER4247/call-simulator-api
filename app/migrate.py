"""
Bare bones migration runner. Applies migrations/*.sql in name order, remembers what ran
in schema_migrations. Didnt want to pull in alembic for two tables.

    python -m app.migrate
"""

from pathlib import Path

import psycopg

from .config import settings

MIGRATIONS = Path(__file__).resolve().parent.parent / "migrations"


def main():
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        # lock so two containers starting together dont both run the same file
        conn.execute("select pg_advisory_lock(727001)")
        conn.execute(
            "create table if not exists schema_migrations ("
            " version text primary key, applied_at timestamptz not null default now())"
        )
        done = {row[0] for row in conn.execute("select version from schema_migrations")}

        for f in sorted(MIGRATIONS.glob("*.sql")):
            if f.name in done:
                continue
            with conn.transaction():
                conn.execute(f.read_text())
                conn.execute("insert into schema_migrations (version) values (%s)", (f.name,))
            print(f"applied {f.name}")

        conn.execute("select pg_advisory_unlock(727001)")
    print("migrations up to date")


if __name__ == "__main__":
    main()
