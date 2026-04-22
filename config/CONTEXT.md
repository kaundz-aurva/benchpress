# Config Context

This folder owns typed benchmark configuration and translation from high-level settings into executable run specs.

Key files:

- `constants.py`: default VU ladder, timing values, repetitions, output root, and workload tool.
- `models.py`: `RunTimingConfig` and `BenchmarkConfig` validation.
- `service.py`: `BenchmarkConfigService.build_run_matrix`.

Keep this layer focused on configuration shape and matrix generation. Do not put repository writes, adapter calls, SQL, or benchmark execution logic here.

Current matrix behavior:

- Iterates audit profiles.
- Iterates virtual user counts.
- Iterates repetitions from `1..repetitions`.
- Produces `RunSpec` objects with output paths grouped by audit mode, VU count, and repetition.

