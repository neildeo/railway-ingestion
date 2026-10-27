FROM python:3.14-slim

WORKDIR /app

COPY corpus/pyproject.toml .
COPY corpus/src ./src

RUN pip install .

CMD ["python", "-m", "corpus"]