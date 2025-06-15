-- =====================================================
-- METADATA CATALOG DATABASE SCHEMA
-- Lưu trữ metadata của tất cả databases trong cluster
-- =====================================================

-- Tạo schema cho catalog
CREATE SCHEMA IF NOT EXISTS catalog;

-- =====================================================
-- 1. BẢNG TABLE_ORIGIN - Lưu metadata của tables/views
-- =====================================================
DROP TABLE IF EXISTS catalog.table_origin CASCADE;

CREATE TABLE catalog.table_origin (
    id VARCHAR(32) PRIMARY KEY,                    -- MD5 hash unique identifier
    datasource VARCHAR(10) NOT NULL,              -- Loại datasource (PG, MySQL, Oracle...)
    database VARCHAR(100) NOT NULL,               -- Tên database
    schema VARCHAR(100) NOT NULL,                 -- Tên schema
    tablename VARCHAR(100) NOT NULL,              -- Tên table/view
    business_term TEXT,                           -- Mô tả nghiệp vụ của bảng
    frequency VARCHAR(10) DEFAULT '1h',           -- Tần suất cập nhật metadata
    rows BIGINT DEFAULT 0,                        -- Số lượng records
    size DECIMAL(15,2) DEFAULT 0.0,              -- Kích thước (MB)
    type VARCHAR(20) NOT NULL,                    -- Loại: table, view, matview
    update_time TIMESTAMP NOT NULL,              -- Thời gian cập nhật metadata
    skip BOOLEAN DEFAULT FALSE,                   -- Có bỏ qua trong quá trình xử lý không
    expire BOOLEAN DEFAULT FALSE,                 -- Đã hết hạn chưa
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT chk_type CHECK (type IN ('table', 'view', 'matview', 'proc'))
);

-- Indexes cho performance
CREATE INDEX idx_table_origin_datasource_db ON catalog.table_origin(datasource, database);
CREATE INDEX idx_table_origin_schema_table ON catalog.table_origin(schema, tablename);
CREATE INDEX idx_table_origin_type ON catalog.table_origin(type);
CREATE INDEX idx_table_origin_update_time ON catalog.table_origin(update_time);
CREATE UNIQUE INDEX idx_table_origin_unique ON catalog.table_origin(datasource, database, schema, tablename);

-- Comments
COMMENT ON TABLE catalog.table_origin IS 'Bảng lưu trữ metadata của tất cả tables, views, materialized views';
COMMENT ON COLUMN catalog.table_origin.id IS 'ID duy nhất được tạo từ MD5(datasource+database+schema+tablename)';
COMMENT ON COLUMN catalog.table_origin.datasource IS 'Loại nguồn dữ liệu: PG, MYSQL, ORACLE, MSSQL';
COMMENT ON COLUMN catalog.table_origin.business_term IS 'Mô tả nghiệp vụ của bảng (từ COMMENT ON TABLE)';
COMMENT ON COLUMN catalog.table_origin.frequency IS 'Tần suất thu thập metadata: 1h, 6h, 12h, 1d, 1w';
COMMENT ON COLUMN catalog.table_origin.rows IS 'Số lượng records ước tính';
COMMENT ON COLUMN catalog.table_origin.size IS 'Kích thước bảng tính bằng MB';
COMMENT ON COLUMN catalog.table_origin.type IS 'Loại đối tượng: table, view, matview, proc';
COMMENT ON COLUMN catalog.table_origin.skip IS 'Có bỏ qua bảng này trong quá trình xử lý không';
COMMENT ON COLUMN catalog.table_origin.expire IS 'Bảng này đã không còn tồn tại trong source';

-- =====================================================
-- 2. BẢNG FIELD_ORIGIN - Lưu metadata của columns
-- =====================================================
DROP TABLE IF EXISTS catalog.field_origin CASCADE;

CREATE TABLE catalog.field_origin (
    id VARCHAR(32) PRIMARY KEY,                    -- MD5 hash unique identifier
    table_id VARCHAR(32) NOT NULL,                -- Foreign key đến table_origin.id
    field VARCHAR(100) NOT NULL,                  -- Tên column
    fieldtype VARCHAR(50) NOT NULL,               -- Kiểu dữ liệu
    field_length INTEGER DEFAULT 0,               -- Độ dài field
    field_demo TEXT,                              -- Dữ liệu mẫu 1
    field_demo2 TEXT,                             -- Dữ liệu mẫu 2
    field_demo3 TEXT,                             -- Dữ liệu mẫu 3
    business_term TEXT,                           -- Mô tả nghiệp vụ của field
    is_nullable BOOLEAN DEFAULT TRUE,             -- Có cho phép NULL không
    is_primary_key BOOLEAN DEFAULT FALSE,         -- Có phải primary key không
    is_foreign_key BOOLEAN DEFAULT FALSE,         -- Có phải foreign key không
    default_value TEXT,                           -- Giá trị mặc định
    position INTEGER,                             -- Vị trí trong bảng
    update_time TIMESTAMP NOT NULL,              -- Thời gian cập nhật metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Foreign key constraint
    CONSTRAINT fk_field_table FOREIGN KEY (table_id) REFERENCES catalog.table_origin(id) ON DELETE CASCADE
);

-- Indexes cho performance
CREATE INDEX idx_field_origin_table_id ON catalog.field_origin(table_id);
CREATE INDEX idx_field_origin_field ON catalog.field_origin(field);
CREATE INDEX idx_field_origin_fieldtype ON catalog.field_origin(fieldtype);
CREATE UNIQUE INDEX idx_field_origin_unique ON catalog.field_origin(table_id, field);

-- Comments
COMMENT ON TABLE catalog.field_origin IS 'Bảng lưu trữ metadata của tất cả columns trong tables/views';
COMMENT ON COLUMN catalog.field_origin.id IS 'ID duy nhất được tạo từ MD5(table_id+field)';
COMMENT ON COLUMN catalog.field_origin.table_id IS 'Reference đến bảng table_origin';
COMMENT ON COLUMN catalog.field_origin.field IS 'Tên column';
COMMENT ON COLUMN catalog.field_origin.fieldtype IS 'Kiểu dữ liệu PostgreSQL (varchar, integer, timestamp...)';
COMMENT ON COLUMN catalog.field_origin.field_length IS 'Độ dài tối đa của field';
COMMENT ON COLUMN catalog.field_origin.field_demo IS 'Giá trị mẫu thứ 1 từ dữ liệu thực tế';
COMMENT ON COLUMN catalog.field_origin.field_demo2 IS 'Giá trị mẫu thứ 2 từ dữ liệu thực tế';
COMMENT ON COLUMN catalog.field_origin.field_demo3 IS 'Giá trị mẫu thứ 3 từ dữ liệu thực tế';
COMMENT ON COLUMN catalog.field_origin.business_term IS 'Mô tả nghiệp vụ của field (từ COMMENT ON COLUMN)';

-- =====================================================
-- 3. BẢNG DATA_LINEAGE - Theo dõi nguồn gốc dữ liệu
-- =====================================================
DROP TABLE IF EXISTS catalog.data_lineage CASCADE;

CREATE TABLE catalog.data_lineage (
    id SERIAL PRIMARY KEY,
    source_table_id VARCHAR(32) NOT NULL,         -- Bảng nguồn
    target_table_id VARCHAR(32) NOT NULL,         -- Bảng đích
    transformation_type VARCHAR(20),              -- Loại transformation: ETL, ELT, VIEW, PROC
    transformation_logic TEXT,                    -- Logic transformation (SQL)
    created_by VARCHAR(50),                       -- Người tạo
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Foreign keys
    CONSTRAINT fk_lineage_source FOREIGN KEY (source_table_id) REFERENCES catalog.table_origin(id),
    CONSTRAINT fk_lineage_target FOREIGN KEY (target_table_id) REFERENCES catalog.table_origin(id),
    CONSTRAINT chk_transformation_type CHECK (transformation_type IN ('ETL', 'ELT', 'VIEW', 'PROC', 'MANUAL'))
);

-- Indexes
CREATE INDEX idx_lineage_source ON catalog.data_lineage(source_table_id);
CREATE INDEX idx_lineage_target ON catalog.data_lineage(target_table_id);

-- Comments
COMMENT ON TABLE catalog.data_lineage IS 'Bảng theo dõi data lineage - mối quan hệ giữa các bảng';

-- =====================================================
-- 4. BẢNG METADATA_JOBS - Theo dõi các job thu thập metadata
-- =====================================================
DROP TABLE IF EXISTS catalog.metadata_jobs CASCADE;

CREATE TABLE catalog.metadata_jobs (
    id SERIAL PRIMARY KEY,
    job_name VARCHAR(100) NOT NULL,               -- Tên job
    datasource VARCHAR(10) NOT NULL,              -- Datasource được xử lý
    database_name VARCHAR(100),                   -- Database được xử lý (NULL = all)
    status VARCHAR(20) NOT NULL,                  -- SUCCESS, FAILED, RUNNING
    start_time TIMESTAMP NOT NULL,               -- Thời gian bắt đầu
    end_time TIMESTAMP,                          -- Thời gian kết thúc
    tables_processed INTEGER DEFAULT 0,          -- Số bảng đã xử lý
    fields_processed INTEGER DEFAULT 0,          -- Số field đã xử lý
    error_message TEXT,                          -- Lỗi nếu có
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT chk_job_status CHECK (status IN ('SUCCESS', 'FAILED', 'RUNNING', 'CANCELLED'))
);

-- Indexes
CREATE INDEX idx_metadata_jobs_status ON catalog.metadata_jobs(status);
CREATE INDEX idx_metadata_jobs_start_time ON catalog.metadata_jobs(start_time);
CREATE INDEX idx_metadata_jobs_datasource ON catalog.metadata_jobs(datasource);

-- Comments
COMMENT ON TABLE catalog.metadata_jobs IS 'Bảng theo dõi các job thu thập metadata';

-- =====================================================
-- 5. VIEW để query dễ dàng
-- =====================================================

-- View tổng hợp thông tin bảng và số lượng fields
CREATE OR REPLACE VIEW catalog.v_table_summary AS
SELECT
    t.id,
    t.datasource,
    t.database,
    t.schema,
    t.tablename,
    t.type,
    t.rows,
    t.size,
    t.business_term,
    COUNT(f.id) as field_count,
    t.update_time
FROM catalog.table_origin t
LEFT JOIN catalog.field_origin f ON t.id = f.table_id
GROUP BY t.id, t.datasource, t.database, t.schema, t.tablename,
         t.type, t.rows, t.size, t.business_term, t.update_time;

COMMENT ON VIEW catalog.v_table_summary IS 'View tổng hợp thông tin bảng và số lượng fields';

-- View chi tiết table và fields
CREATE OR REPLACE VIEW catalog.v_table_fields AS
SELECT
    t.datasource,
    t.database,
    t.schema,
    t.tablename,
    t.type as table_type,
    t.business_term as table_description,
    f.field as column_name,
    f.fieldtype as column_type,
    f.field_length,
    f.business_term as column_description,
    f.is_nullable,
    f.is_primary_key,
    f.field_demo as sample_value,
    f.position
FROM catalog.table_origin t
JOIN catalog.field_origin f ON t.id = f.table_id
ORDER BY t.datasource, t.database, t.schema, t.tablename, f.position;

COMMENT ON VIEW catalog.v_table_fields IS 'View chi tiết tất cả tables và fields';

-- =====================================================
-- 6. FUNCTIONS hỗ trợ
-- =====================================================

-- Function tìm kiếm bảng theo tên
CREATE OR REPLACE FUNCTION catalog.search_tables(search_term TEXT)
RETURNS TABLE(
    datasource VARCHAR,
    database VARCHAR,
    schema VARCHAR,
    tablename VARCHAR,
    type VARCHAR,
    business_term TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT t.datasource, t.database, t.schema, t.tablename, t.type, t.business_term
    FROM catalog.table_origin t
    WHERE LOWER(t.tablename) LIKE LOWER('%' || search_term || '%')
       OR LOWER(t.business_term) LIKE LOWER('%' || search_term || '%')
    ORDER BY t.datasource, t.database, t.schema, t.tablename;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION catalog.search_tables(TEXT) IS 'Tìm kiếm bảng theo tên hoặc mô tả nghiệp vụ';

-- Function thống kê metadata
CREATE OR REPLACE FUNCTION catalog.get_metadata_stats()
RETURNS TABLE(
    datasource VARCHAR,
    total_databases BIGINT,
    total_tables BIGINT,
    total_views BIGINT,
    total_matviews BIGINT,
    total_fields BIGINT,
    total_size_mb NUMERIC
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        t.datasource,
        COUNT(DISTINCT t.database) as total_databases,
        COUNT(CASE WHEN t.type = 'table' THEN 1 END) as total_tables,
        COUNT(CASE WHEN t.type = 'view' THEN 1 END) as total_views,
        COUNT(CASE WHEN t.type = 'matview' THEN 1 END) as total_matviews,
        COUNT(f.id) as total_fields,
        ROUND(SUM(t.size), 2) as total_size_mb
    FROM catalog.table_origin t
    LEFT JOIN catalog.field_origin f ON t.id = f.table_id
    GROUP BY t.datasource
    ORDER BY t.datasource;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION catalog.get_metadata_stats() IS 'Thống kê tổng quan metadata theo datasource';

-- =====================================================
-- 7. Sample data và test
-- =====================================================

-- Grant permissions (adjust as needed)
-- GRANT USAGE ON SCHEMA catalog TO metadata_user;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA catalog TO metadata_user;
-- GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA catalog TO metadata_user;

-- Test queries
/*
-- Tìm kiếm bảng
SELECT * FROM catalog.search_tables('customer');

-- Thống kê metadata
SELECT * FROM catalog.get_metadata_stats();

-- Xem tổng quan
SELECT * FROM catalog.v_table_summary WHERE datasource = 'PG';

-- Xem chi tiết
SELECT * FROM catalog.v_table_fields
WHERE database = 'ecommerce' AND schema = 'sales'
ORDER BY tablename, position;
*/