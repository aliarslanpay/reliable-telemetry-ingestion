"""Initialize only the disposable project database with an administrative DSN."""
import os
from pathlib import Path
import psycopg
from psycopg import sql


def main():
    password = os.environ["WORKER_DB_PASSWORD"]
    if not 16 <= len(password) <= 128:
        raise SystemExit("Worker database password length must be 16..128")
    try:
        with psycopg.connect(os.environ["DATABASE_ADMIN_URL"], connect_timeout=2,
                options="-c statement_timeout=3000 -c lock_timeout=1000") as conn:
            conn.execute((Path(__file__).parents[1] / "migrations/001_events.sql").read_text())
            conn.execute(sql.SQL("ALTER ROLE telemetry_worker WITH LOGIN PASSWORD {}").format(sql.Literal(password)))
    except psycopg.Error as exc:
        raise SystemExit("Migration failed: " + type(exc).__name__) from None
    print("Project schema and restricted worker role initialized.")


if __name__ == "__main__":
    main()
