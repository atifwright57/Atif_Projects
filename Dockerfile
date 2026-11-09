FROM python:3.12-slim

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 app \
    && mkdir -p /app/data/delta \
    && chown -R app:app /app

WORKDIR /app

COPY projects/enterprise-lakehouse-platform/requirements.txt ./requirements.txt
RUN python -m pip install --no-cache-dir -r requirements.txt

COPY --chown=app:app projects/enterprise-lakehouse-platform/src ./src
COPY --chown=app:app projects/enterprise-lakehouse-platform/data/orders.json ./data/orders.json

USER app

ENTRYPOINT ["python", "src/medallion_pipeline.py"]
CMD ["--input", "/app/data/orders.json", "--output-root", "/app/data/delta"]
