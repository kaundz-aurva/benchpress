from __future__ import annotations

from pathlib import Path

from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.responses import FileResponse
from starlette.concurrency import run_in_threadpool

from agents.sqlserver.dto import (
    ActionResponse,
    ArtifactInfo,
    ArtifactListResponse,
    HealthResponse,
    MetadataResponse,
    RunActionRequest,
)
from agents.sqlserver.service import AgentCommandError, SqlServerAgentService


def create_app(service: SqlServerAgentService, bearer_token: str) -> FastAPI:
    if not bearer_token.strip():
        raise ValueError("bearer_token is required")

    app = FastAPI(title="Benchpress SQL Server Agent")

    def require_token(authorization: str = Header(default="")) -> None:
        expected = f"Bearer {bearer_token}"
        if authorization != expected:
            raise HTTPException(status_code=401, detail="invalid bearer token")

    @app.get("/health", response_model=HealthResponse)
    def health() -> HealthResponse:
        details = service.health()
        return HealthResponse(ok=True, service=details["service"])

    @app.post("/audit/enable", response_model=ActionResponse, dependencies=[Depends(require_token)])
    async def enable_audit() -> ActionResponse:
        return await _action_response(service.enable_audit)

    @app.post("/audit/disable", response_model=ActionResponse, dependencies=[Depends(require_token)])
    async def disable_audit() -> ActionResponse:
        return await _action_response(service.disable_audit)

    @app.post(
        "/database/connectivity",
        response_model=ActionResponse,
        dependencies=[Depends(require_token)],
    )
    async def validate_connectivity() -> ActionResponse:
        return await _action_response(service.validate_connectivity)

    @app.post("/database/sanity", response_model=MetadataResponse, dependencies=[Depends(require_token)])
    async def sanity() -> MetadataResponse:
        return await _metadata_response(service.run_sanity_checks)

    @app.post(
        "/snapshots/{label}",
        response_model=ArtifactListResponse,
        dependencies=[Depends(require_token)],
    )
    async def capture_snapshot(label: str, request: RunActionRequest) -> ArtifactListResponse:
        try:
            artifacts = await run_in_threadpool(service.capture_snapshot, request.run_id, label)
            return ArtifactListResponse.from_agent_artifacts(artifacts)
        except (AgentCommandError, NotImplementedError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/metrics/start", response_model=ActionResponse, dependencies=[Depends(require_token)])
    async def start_metrics(request: RunActionRequest) -> ActionResponse:
        return await _action_response(lambda: service.start_metrics_collection(request.run_id))

    @app.post(
        "/metrics/stop",
        response_model=ArtifactListResponse,
        dependencies=[Depends(require_token)],
    )
    async def stop_metrics(request: RunActionRequest) -> ArtifactListResponse:
        try:
            artifacts = await run_in_threadpool(service.stop_metrics_collection, request.run_id)
            return ArtifactListResponse.from_agent_artifacts(artifacts)
        except (AgentCommandError, NotImplementedError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get(
        "/metadata/database",
        response_model=MetadataResponse,
        dependencies=[Depends(require_token)],
    )
    async def database_metadata() -> MetadataResponse:
        return await _metadata_response(service.collect_database_metadata)

    @app.get(
        "/metadata/filesystem",
        response_model=MetadataResponse,
        dependencies=[Depends(require_token)],
    )
    async def filesystem_metadata() -> MetadataResponse:
        return await _metadata_response(service.collect_filesystem_stats)

    @app.get("/metadata/host", response_model=MetadataResponse, dependencies=[Depends(require_token)])
    async def host_metadata() -> MetadataResponse:
        return await _metadata_response(service.collect_host_metadata)

    @app.get("/artifacts", response_model=ArtifactListResponse, dependencies=[Depends(require_token)])
    def list_artifacts() -> ArtifactListResponse:
        return ArtifactListResponse(
            artifacts=[ArtifactInfo.from_agent_artifact(artifact) for artifact in service.list_artifacts()]
        )

    @app.get("/artifacts/{artifact_id}", dependencies=[Depends(require_token)])
    def download_artifact(artifact_id: int) -> FileResponse:
        artifact = service.get_artifact(artifact_id)
        if artifact is None:
            raise HTTPException(status_code=404, detail="artifact not found")
        path = Path(artifact.path)
        if not path.exists():
            raise HTTPException(status_code=404, detail="artifact file not found")
        return FileResponse(path, filename=path.name)

    return app


async def _action_response(action: object) -> ActionResponse:
    try:
        details = await run_in_threadpool(action)
        return ActionResponse(ok=True, details=details)
    except (AgentCommandError, NotImplementedError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


async def _metadata_response(action: object) -> MetadataResponse:
    try:
        return MetadataResponse(metadata=await run_in_threadpool(action))
    except (AgentCommandError, NotImplementedError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

