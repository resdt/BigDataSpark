FROM jupyter/pyspark-notebook:spark-3.3.0
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

RUN wget https://jdbc.postgresql.org/download/postgresql-42.6.0.jar -O /home/jovyan/postgresql-jdbc.jar

WORKDIR /app
COPY . /app

RUN uv sync --frozen --no-cache-dir
