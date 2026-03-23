FROM apache/superset:latest

USER root
RUN uv pip install --python /app/.venv/bin/python --system "trino[sqlalchemy]" "sqlalchemy-trino"
RUN /app/.venv/bin/python -c "import importlib; importlib.import_module('trino.sqlalchemy.dialect')"
USER superset
