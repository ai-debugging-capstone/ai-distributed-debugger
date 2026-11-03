FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
COPY . /app/rca

RUN useradd --create-home --uid 10001 rca \
    && mkdir -p /app/artifacts \
    && chown -R rca:rca /app
USER rca

HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=5 \
  CMD ["python", "-m", "rca.worker", "--check"]

CMD ["python", "-m", "rca.worker"]
