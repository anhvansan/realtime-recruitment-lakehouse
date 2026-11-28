# Shared lean container image for Streamlit Dashboard and Alert Worker (Task 5.9).
# Pre-installs DuckDB httpfs + delta extensions so containers restart offline.
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app \
    DUCKDB_EXTENSION_DIRECTORY=/opt/duckdb_extensions \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN pip install --upgrade pip \
    && pip install \
        "streamlit==1.67.0" \
        "duckdb==1.5.6" \
        "pandas>=2.0.0" \
        "pyarrow>=17.0.0" \
        "altair>=5.0.0" \
        "boto3>=1.35.0" \
        "pydantic>=2.0.0" \
        "pydantic-settings>=2.0.0" \
        "python-dotenv>=1.0.0" \
        "pyyaml>=6.0.0"

# Pre-bake DuckDB extensions into DUCKDB_EXTENSION_DIRECTORY for offline container restarts.
RUN mkdir -p "${DUCKDB_EXTENSION_DIRECTORY}" \
    && python -c "import duckdb; con = duckdb.connect(':memory:', config={'extension_directory': '/opt/duckdb_extensions'}); con.execute('INSTALL httpfs'); con.execute('INSTALL delta'); con.close()"

# Copy only application code, configs, and dimension seeds (Rule R6: never copy .env).
COPY src/ /app/src/
COPY config/ /app/config/
COPY dbt_cpc/seeds/ /app/dbt_cpc/seeds/

RUN useradd --create-home --uid 1000 --shell /bin/bash appuser \
    && chown -R appuser:appuser /app "${DUCKDB_EXTENSION_DIRECTORY}"

USER appuser

EXPOSE 8501

HEALTHCHECK --interval=15s --timeout=5s --start-period=20s --retries=3 \
    CMD curl -sf http://127.0.0.1:8501/_stcore/health || exit 1

CMD ["streamlit", "run", "src/dashboard/app.py", "--server.address=0.0.0.0", "--server.port=8501", "--server.headless=true"]
