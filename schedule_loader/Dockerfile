FROM python:3.14-slim

WORKDIR /app

COPY schedule_loader/pyproject.toml .
COPY schedule_loader/src ./src
COPY schemas ./schemas

RUN pip install .

CMD ["python", "-m", "schedule_loader"]