# chart_json.py — build Chart.js payload for Luna
# Uses plot_engine’s fetch + indicators, returns {labels, datasets} for Chart.js
from typing import Dict, Any, List
from plot_engine import (
    resolve_cg_id,
    _fetch_prices,
    _ols_trendline,
    _fib_extensions,
)

DAYS_MAP = {
    "1d": 1,
    "7d": 7,
    "30d": 30,
    "90d": 90,
    "1y": 365,
    "max": "max",
}

def _constant_series(value: float, n: int) -> List[float]:
    return [float(value)] * n

def build_chartjs_payload(symbol: str = "BTC", time_key: str = "7d", metric: str = "price_trend") -> Dict[str, Any]:
    """
    Returns a dict suited for Chart.js:
    {
      "labels": [ms_epoch,...],
      "datasets": [{label, data, borderColor, ...}, ...]
    }
    """
    symbol = (symbol or "BTC").upper()
    cg_id = resolve_cg_id(symbol)
    days = DAYS_MAP.get(time_key, 7)

    ts, px = _fetch_prices(cg_id, days=days)
    if not px:
        return {"labels": [], "datasets": []}

    # _fetch_prices returns seconds—Chart.js time scale likes ms.
    labels_ms = [int(t * 1000) for t in ts]

    # Trendline + Fib levels from the same window
    trend = _ols_trendline(px)
    A, B = _recent_pivots(px)
    fibs = _fib_extensions(A, B)  # list[(name, level)]

    datasets = []

    if metric in ("price", "price_trend"):
        datasets.append({
            "label": f"{symbol} Price",
            "data": px,
            "borderColor": "#60d4ff",
            "backgroundColor": "rgba(96,212,255,0.08)",
            "pointRadius": 0,
            "tension": 0.25,
        })

    if metric in ("trend", "price_trend"):
        if len(trend) == len(px):
            datasets.append({
                "label": "Trendline (OLS)",
                "data": trend,
                "borderColor": "#9cff9c",
                "borderDash": [6, 4],
                "pointRadius": 0,
                "tension": 0.0,
            })

    # Fib horizontal bands as constant series
    for name, level in fibs:
        if level is None:
            continue
        datasets.append({
            "label": f"Fib {name}",
            "data": _constant_series(level, len(px)),
            "borderColor": "rgba(255,209,102,0.55)",
            "borderDash": [2, 3],
            "pointRadius": 0,
            "tension": 0.0,
        })

    return {"labels": labels_ms, "datasets": datasets}
