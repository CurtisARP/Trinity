#!/usr/bin/env python3
"""
Run a single pipeline for testing using the same classes as production.
Usage: python run_pipeline.py <pipeline_name>
Example: python run_pipeline.py redgifs_ai_generated_to_telegram
"""

import importlib
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

APP_DIR = Path(__file__).parent
sys.path.insert(0, str(APP_DIR))
sys.path.insert(0, str(APP_DIR / "src"))

_imghdr_path = APP_DIR / "src" / "imghdr.py"
if _imghdr_path.exists():
    from importlib.util import module_from_spec, spec_from_file_location

    spec = spec_from_file_location("imghdr", str(_imghdr_path))
    _imghdr = module_from_spec(spec)
    spec.loader.exec_module(_imghdr)
    sys.modules["imghdr"] = _imghdr

from src.config import load_pipeline_config
from src.pipeline_store import MyPipelineStore


def load_function(full_function_path: str):
    """Dynamically loads a function given its full path."""
    module_path, function_name = full_function_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, function_name)


def run_single_pipeline(pipeline_name: str):
    """Run a single pipeline by name using production classes."""
    pipelines_dir = APP_DIR / "pipelines"
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

    # Load notification config from global.json
    global_config_file = pipelines_dir / "global.json"
    notification_config = {}
    if global_config_file.exists():
        global_config = load_pipeline_config(global_config_file)
        notification_config = global_config.get("notifications", {})

    name = config.get("name", "unnamed")
    print(f"\nRunning pipeline: {name}")
    print("=" * 50)

    config.setdefault("instant_launch", False)
    config.setdefault("launch_condition", {"time": "*/15 * * * *"})

    # Load tasks (same as runner.py)
    tasks = []
    try:
        source_task = config.get("source", {}).get("task", "")
        if not source_task:
            print("Error: No source task defined in pipeline")
            sys.exit(1)

        tasks = [load_function(source_task)]
        tasks += [load_function(m) for m in config.get("middleware", []) if m]
        tasks.append(load_function(config.get("post", {}).get("task", "")))
    except Exception as e:
        print(f"Error loading tasks: {e}")
        sys.exit(1)

    # Create pipeline store (same as runner.py)
    store = MyPipelineStore(notification_config=notification_config)

    try:
        store.add_pipeline(config, tasks)
    except Exception as e:
        print(f"Error adding pipeline: {e}")
        sys.exit(1)

    try:
        pipeline = list(store.get_all_pipelines().values())[0]
        pipeline.execute_pipeline(tasks, config)
        print("\n" + "=" * 50)
        print("Pipeline completed successfully!")
    except Exception as e:
        print(f"\n" + "=" * 50)
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
