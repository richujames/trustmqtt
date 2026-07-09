FROM python:3.11-slim

WORKDIR /app

COPY tmq_worker/requirements.txt tmq_worker/requirements.txt
COPY tmq_worker/requirements-dev.txt tmq_worker/requirements-dev.txt
RUN pip install --no-cache-dir -r tmq_worker/requirements.txt -r tmq_worker/requirements-dev.txt

COPY tmq_worker/ tmq_worker/
COPY config/ config/
COPY migrations/ migrations/
COPY alembic.ini alembic.ini

ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

# No ENTRYPOINT: `docker compose up` uses this default CMD to run the
# worker daemon, but `docker compose run --rm tmq-worker <other command>`
# (e.g. `python -m tmq_worker.train`, `pytest ...`) fully replaces it rather
# than being appended after a fixed entrypoint.
CMD ["python", "-m", "tmq_worker"]
