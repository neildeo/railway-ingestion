FROM python:3.14-slim

WORKDIR /app

COPY dummy/pyproject.toml .
COPY dummy/src ./src

RUN pip install .

CMD ["python", "-m", "dummy"]