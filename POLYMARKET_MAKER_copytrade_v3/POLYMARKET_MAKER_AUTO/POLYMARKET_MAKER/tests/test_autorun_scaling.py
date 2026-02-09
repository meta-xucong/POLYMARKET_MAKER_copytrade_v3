import math
import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# 在导入 poly_maker_autorun 之前打桩 requests，避免缺失依赖导致提前退出。
if "requests" not in sys.modules:
    sys.modules["requests"] = types.SimpleNamespace()

from poly_maker_autorun import _scale_order_size_by_volume


def test_scale_order_size_high_volume_is_dampened():
    base_size = 10
    growth = 0.5
    size = _scale_order_size_by_volume(
        base_size=base_size,
        total_volume=3_000_000,
        base_volume=10_000,
        growth_factor=growth,
    )
    expected = base_size * (1 + growth * math.log10(300))
    assert size <= 30
    assert math.isclose(size, expected, rel_tol=1e-3)


def test_scale_order_size_near_base_volume_is_gentle():
    base_size = 10
    growth = 0.5
    size = _scale_order_size_by_volume(
        base_size=base_size,
        total_volume=12_000,
        base_volume=10_000,
        growth_factor=growth,
    )
    expected = base_size * (1 + growth * math.log10(1.2))
    assert base_size < size < base_size * 1.1
    assert math.isclose(size, expected, rel_tol=1e-3)


def test_scale_order_size_respects_custom_growth_factor():
    base_size = 10
    size = _scale_order_size_by_volume(
        base_size=base_size,
        total_volume=3_000_000,
        base_volume=10_000,
        growth_factor=0.25,
    )
    expected = base_size * (1 + 0.25 * math.log10(300))
    assert math.isclose(size, expected, rel_tol=1e-3)
