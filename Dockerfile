FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
COPY pyproject.toml requirements.lock ./
RUN pip install --no-cache-dir -r requirements.lock
COPY src ./src
RUN pip install --no-cache-dir --no-deps . && useradd --uid 10001 --create-home trader && mkdir /app/state && chown trader:trader /app/state
COPY config ./config
USER trader
ENTRYPOINT ["selective-options"]
CMD ["probe", "--output", "/app/state/probe.json"]
