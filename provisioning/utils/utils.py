"""
MES Utils v1.0 – Logging + Progress (for all loaders)
"""
from typing import Callable

_progress_hook: Callable[[str], None] = print

def set_progress_hook(hook: Callable[[str], None]):
    global _progress_hook
    _progress_hook = hook

def bump_progress(msg: str):
    _progress_hook(msg)

def log_header(msg: str):
    print(f"\n{'═' * 80}")
    print(f"📦 {msg}")
    print(f"{'═' * 80}\n")

def log_success(msg: str):
    print(f"✅ {msg}")

def log_info(msg: str):
    print(f"ℹ️  {msg}")

def log_warn(msg: str):
    print(f"⚠️  {msg}")

def log_error(msg: str):
    print(f"❌ {msg}")

def log_kpi_summary(kpis: dict):
    """KPI Dashboard Summary for MES"""
    print(f"\n📊 KPI SUMMARY")
    print(f"{'─' * 60}")
    for k, v in kpis.items():
        print(f"{k:20}: {v}")
    print(f"{'─' * 60}\n")
