FROM apache/superset:latest

USER root
RUN uv pip install --python /app/.venv/bin/python --system "trino[sqlalchemy]" "sqlalchemy-trino" "psycopg2-binary"
RUN /app/.venv/bin/python -c "import importlib; importlib.import_module('trino.sqlalchemy.dialect')"
COPY --chown=superset:superset superset_config.py /app/pythonpath/superset_config.py
USER superset
