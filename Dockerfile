FROM debezium/postgres:17

USER root

# Cài các gói cần thiết để build
RUN apk add --no-cache \
    git \
    build-base \
    postgresql15-dev \
    && git clone https://github.com/eulerto/wal2json.git /wal2json \
    && cd /wal2json \
    && make \
    && make install \
    && cd / \
    && rm -rf /wal2json \
    && apk del git build-base

USER debezium
