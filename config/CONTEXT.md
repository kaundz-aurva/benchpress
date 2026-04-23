# Config Context

This folder owns typed benchmark configuration and translation from high-level settings into executable run specs.

Key files:

- `constants.py`: default VU ladder, timing values, repetitions, output root, and workload tool.
- `dto.py`: Pydantic JSON runtime spec DTOs for real VM runs and asset generation.
- `models.py`: `RunTimingConfig` and `BenchmarkConfig` validation.
- `service.py`: `BenchmarkConfigService.build_run_matrix`, JSON spec loading, and runtime spec conversion.

Keep this layer focused on configuration shape and matrix generation. Do not put repository writes, adapter calls, SQL, or benchmark execution logic here.

Current matrix behavior:

- Iterates audit profiles.
- Iterates virtual user counts.
- Iterates repetitions from `1..repetitions`.
- Produces `RunSpec` objects with output paths grouped by audit mode, VU count, and repetition.

Runtime specs are JSON only in this slice. Do not add YAML unless package/dependency policy is updated.

The optional `assets` section drives generation of SQL Server audit scripts, HammerDB TPROC-C TCL, Windows logman scripts, and a resolved SQL agent config.
