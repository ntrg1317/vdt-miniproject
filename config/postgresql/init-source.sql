CREATE ROLE replication_user WITH LOGIN REPLICATION PASSWORD 'rep_pass';

CREATE SCHEMA IF NOT EXISTS vdt;

GRANT USAGE ON SCHEMA vdt TO replication_user;
GRANT SELECT ON ALL TABLES IN SCHEMA vdt TO replication_user;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA vdt TO replication_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA vdt GRANT SELECT ON TABLES TO replication_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA vdt GRANT SELECT ON SEQUENCES TO replication_user;

CREATE TABLE vdt.users (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(255) NOT NULL,
    last_name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE PUBLICATION debezium_pub FOR ALL TABLES;

INSERT INTO vdt.users (first_name, last_name, email) VALUES
    ('John', 'Doe', 'john.doe@example.com'),
    ('Jane', 'Smith', 'jane.smith@example.com'),
    ('Bob', 'Johnson', 'bob.johnson@example.com');
