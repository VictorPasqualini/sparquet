from pathlib import Path

from sparquet.core.config import PipelineConfig
from sparquet.core.pipeline import OutputMetrics, Pipeline, PipelineResult
from sparquet.framework import Sparquet

__version__ = "0.12.0"
__all__ = [
    "Sparquet",
    "Pipeline",
    "PipelineResult",
    "OutputMetrics",
    "PipelineConfig",
    "examples_path",
]


def examples_path() -> Path:
    """Where the example pipeline configs are, wherever this package came from.

    An installed wheel carries them as `sparquet/examples` (see the `package-dir`
    mapping in `pyproject.toml`); a git checkout has them at `examples/` next to
    the package, which is also where the documentation points. Both answers are
    the same files, so callers ask here instead of guessing.

    The path is returned whether or not it exists: a caller that needs the configs
    should say so with its own error, which will be a better one than ours.
    """
    packaged = Path(__file__).resolve().parent / "examples"
    if packaged.is_dir():
        return packaged
    return Path(__file__).resolve().parent.parent / "examples"
