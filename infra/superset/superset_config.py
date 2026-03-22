import os

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "change-me-in-prod")
