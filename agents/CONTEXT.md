# Agents Context

This folder owns VM-side HTTP agents and HTTP clients used by orchestrators.

Current implementation:

- `sqlserver/`: FastAPI SQL Server VM agent plus client used by agent-backed adapters.

Rules:

- Agents expose whitelisted benchmark actions only.
- Do not add generic remote shell endpoints.
- Auth is bearer token based for this slice.
- Keep HTTP DTOs in `dto.py`, local runtime settings in `models.py`, action logic in `service.py`, and FastAPI routes in `app.py`.

