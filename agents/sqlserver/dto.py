from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class RunActionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: int = Field(gt=0)


class HealthResponse(BaseModel):
    ok: bool
    service: str


class ActionResponse(BaseModel):
    ok: bool = True
    details: dict[str, Any] = Field(default_factory=dict)


class MetadataResponse(BaseModel):
    metadata: dict[str, Any] = Field(default_factory=dict)


class ArtifactInfo(BaseModel):
    artifact_id: int = Field(gt=0)
    artifact_type: str
    path: str
    description: str = ""

    @field_validator("artifact_type", "path")
    @classmethod
    def _required_text(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("value is required")
        return value

    @classmethod
    def from_agent_artifact(cls, artifact: object) -> "ArtifactInfo":
        return cls(
            artifact_id=artifact.artifact_id,
            artifact_type=artifact.artifact_type,
            path=str(artifact.path),
            description=artifact.description,
        )


class ArtifactListResponse(BaseModel):
    artifacts: list[ArtifactInfo] = Field(default_factory=list)

    @classmethod
    def from_agent_artifacts(cls, artifacts: list[object]) -> "ArtifactListResponse":
        return cls(artifacts=[ArtifactInfo.from_agent_artifact(artifact) for artifact in artifacts])
