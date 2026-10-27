FROM python:3.14-slim

WORKDIR /app

COPY schedule/pyproject.toml .
COPY schedule/src ./src

RUN pip install .

CMD ["python", "-m", "schedule"]