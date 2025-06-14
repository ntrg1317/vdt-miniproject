CREATE TABLE vdt_rep.users (
    id SERIAL PRIMARY KEY,
    full_name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    sync_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);