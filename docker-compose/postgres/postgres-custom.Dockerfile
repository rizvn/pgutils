FROM postgres:17

RUN apt update -y \
    && apt upgrade -y \
    && apt install postgresql-17-pgvector -y \
    && apt install postgresql-17-cron -y \
    && apt install postgresql-17-partman -y


# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    postgresql-server-dev-17

# Clone, build, and install pgmq
RUN git clone https://github.com/tembo-io/pgmq.git /tmp/pgmq \
    && cd /tmp/pgmq \
    && git checkout tags/v1.8.0 -b v1.8.0 \
    && cd pgmq-extension \
    && make && make install \
    && rm -rf /tmp/pgmq


# Clean up
RUN apt-get remove -y build-essential git postgresql-server-dev-17 \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

## Add initialization script
COPY postgres-init-db.sql /docker-entrypoint-initdb.d/

CMD ["postgres", "-c", "shared_preload_libraries=pg_cron", "-c", "cron.database_name=app_db"]
