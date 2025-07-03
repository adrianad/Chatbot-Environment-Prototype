-- Create bfabric user
CREATE USER bfabric WITH PASSWORD 'bfabric123';

-- Create readonly user for MCP
CREATE USER mcp_readonly WITH PASSWORD 'mcp_readonly_pass';

-- Grant connect privileges
GRANT CONNECT ON DATABASE bfabric TO mcp_readonly;

-- Grant usage on schema
GRANT USAGE ON SCHEMA public TO mcp_readonly;

-- Grant select on all existing tables
GRANT SELECT ON ALL TABLES IN SCHEMA public TO mcp_readonly;

-- Grant select on all future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO mcp_readonly;

-- Load bfabric database dump
\! gunzip -c /bfabric_dump.sql.gz | psql -d bfabric