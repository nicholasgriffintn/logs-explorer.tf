import os
from urllib.parse import quote_plus


def _build_metadata_db_uri() -> str:
    db_user = os.environ.get("SUPERSET_DB_USER", "").strip()
    db_password = os.environ.get("SUPERSET_DB_PASSWORD", "")
    db_host = os.environ.get("SUPERSET_DB_HOST", "").strip()
    db_port = os.environ.get("SUPERSET_DB_PORT", "5432").strip()
    db_name = os.environ.get("SUPERSET_DB_NAME", "").strip()

    if all([db_user, db_host, db_name]):
        return (
            "postgresql+psycopg2://"
            f"{quote_plus(db_user)}:{quote_plus(db_password)}@{db_host}:{db_port}/{db_name}"
        )

    return "sqlite:////app/superset_home/superset.db"


SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "change-me-in-prod")
SQLALCHEMY_DATABASE_URI = os.environ.get("SUPERSET_SQLALCHEMY_DATABASE_URI", _build_metadata_db_uri())
