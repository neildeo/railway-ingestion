FROM python:3.14-slim

WORKDIR /app

COPY schedule_load_backfill/pyproject.toml .
COPY schedule_load_backfill/src ./src

RUN pip install .

CMD ["python", "-m", "schedule_load_backfill"]