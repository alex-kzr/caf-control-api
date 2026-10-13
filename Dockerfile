FROM python:3.12-slim

WORKDIR /app
RUN pip install --no-cache-dir uv==0.12.17
COPY pyproject.toml uv.lock ./
COPY src ./src
RUN uv sync --frozen --no-dev

USER 65532:65532
EXPOSE 8000
CMD [".venv/bin/python", "-m", "caf_control_api.start"]
