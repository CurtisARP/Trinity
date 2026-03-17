#!/usr/bin/env python3
"""
Run a single pipeline for testing.
Usage: python run_pipeline.py <pipeline_name>
Example: python run_pipeline.py redgifs_ai_generated_to_telegram
"""

import sys
import json
import importlib
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.config import load_pipeline_config


class TestPipeline:
    """Minimal pipeline mock for testing."""

    def __init__(self, name: str, log_dir: Path, history_dir: Path):
        self.name = name
        self.log_dir = log_dir
        self.history_dir = history_dir
        self.media = []

    def check_post_history(self, filename: str):
        return False

    def add_media(self, media_type: str, path: str):
        self.media.append({"type": media_type, "path": path})
        print(f"  Added media: {media_type} - {path}")

    def log(self, message: str):
        print(f"  [LOG] {message}")


def run_single_pipeline(pipeline_name: str):
    """Run a single pipeline by name."""
    pipelines_dir = Path(__file__).parent / "pipelines"
    pipeline_file = pipelines_dir / f"{pipeline_name}.json"

    if not pipeline_file.exists():
        print(f"Error: Pipeline '{pipeline_name}' not found")
        print(f"Available pipelines:")
        for f in pipelines_dir.glob("*.json"):
            if f.stem != "global":
                print(f"  - {f.stem}")
        sys.exit(1)

    print(f"Loading pipeline: {pipeline_name}")
    config = load_pipeline_config(pipeline_file)

    log_dir = Path(__file__).parent / "logs"
    history_dir = Path(__file__).parent / "history"
    pipeline = TestPipeline(config.get("name", pipeline_name), log_dir, history_dir)

    print(f"\nRunning pipeline: {pipeline.name}")
    print("=" * 50)

    try:
        source_config = config.get("source", {})
        source_task = source_config.get("task", "")

        if not source_task:
            print("Error: No source task defined in pipeline")
            sys.exit(1)

        print("\n[1] Running source...")
        print(f"  Task: {source_task}")

        module_path, func_name = source_task.rsplit(".", 1)
        module = importlib.import_module(module_path)
        source_func = getattr(module, func_name)

        result = source_func(pipeline, config)

        middlewares = config.get("middleware", [])
        for i, mw_path in enumerate(middlewares, 2):
            if not mw_path:
                continue
            print(f"\n[{i}] Running middleware...")
            print(f"  Task: {mw_path}")

            mw_module_path, mw_func_name = mw_path.rsplit(".", 1)
            mw_module = importlib.import_module(mw_module_path)
            mw_func = getattr(mw_module, mw_func_name)
            result = mw_func(pipeline, result)

        post_config = config.get("post", {})
        post_task = post_config.get("task", "")

        if post_task:
            print(f"\n[{len(middlewares) + 2}] Running poster...")
            print(f"  Task: {post_task}")

            post_module_path, post_func_name = post_task.rsplit(".", 1)
            post_module = importlib.import_module(post_module_path)
            post_func = getattr(post_module, post_func_name)
            result = post_func(pipeline, result)

        print("\n" + "=" * 50)
        print("Pipeline completed successfully!")
        print(f"  Media files: {len(pipeline.media)}")

    except Exception as e:
        print("\n" + "=" * 50)
        print(f"Pipeline failed: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python run_pipeline.py <pipeline_name>")
        print("Example: python run_pipeline.py redgifs_ai_generated_to_telegram")
        print("\nAvailable pipelines:")
        pipelines_dir = Path(__file__).parent / "pipelines"
        for f in sorted(pipelines_dir.glob("*.json")):
            if f.stem != "global":
                print(f"  - {f.stem}")
        sys.exit(1)

    pipeline_name = sys.argv[1]
    run_single_pipeline(pipeline_name)
