"""
Pytest configuration for Homer ETL tests.

Adds both the project root and the dags/ directory to sys.path so that:
  - tests can do:  from dags.pipeline.xxx import ...
  - pipeline modules can do:  from pipeline.xxx import ...   (as they do at runtime
    inside Airflow, where dags/ is on the path)
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
DAGS_DIR = PROJECT_ROOT / "dags"

for p in (str(PROJECT_ROOT), str(DAGS_DIR)):
    if p not in sys.path:
        sys.path.insert(0, p)
