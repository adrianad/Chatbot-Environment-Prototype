# Open WebUI with MCP Proxy

Docker Compose setup for Open WebUI with MCP (Model Context Protocol) proxy and PostgreSQL database.

## Services

- **Open WebUI**: Web interface at `http://localhost:3000`
- **MCP Proxy**: Handles PostgreSQL MCP server at port `8005`
- **PostgreSQL**: Database server at port `5432`

## Quick Start

```bash
docker-compose up -d
```

## Data Directories

- `open-webui-data/` - WebUI persistent data
- `init-db.sql` - Database initialization (add your dump here)

## Configuration

- WebUI authentication is disabled (`WEBUI_AUTH=False`)
- MCP servers configured in `mcpo-config.json`
- PostgreSQL readonly user: `mcp_readonly` / `mcp_readonly_pass`