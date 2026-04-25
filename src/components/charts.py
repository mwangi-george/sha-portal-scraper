import polars as pl
from nicegui import ui


def empty_state(message: str = "No data available for the selected filters.") -> None:
    """Render an empty-state message."""
    with ui.card().classes("w-full p-8 rounded-2xl border border-dashed border-slate-300"):
        ui.icon("info").classes("text-4xl text-slate-400")
        ui.label(message).classes("text-slate-500")


def horizontal_bar(
    *,
    title: str,
    df: pl.DataFrame,
    category: str,
    value: str,
    height: int = 420,
) -> None:
    """Render a ranked horizontal bar chart."""
    if df.is_empty():
        empty_state()
        return

    chart_df = df.sort(value)
    categories = chart_df[category].cast(pl.String).to_list()
    values = chart_df[value].round(0).to_list()

    ui.echart(
        {
            "title": {"text": title, "left": "center"},
            "tooltip": {
                "trigger": "axis",
                "axisPointer": {"type": "shadow"},
            },
            "grid": {"left": "4%", "right": "8%", "bottom": "4%", "containLabel": True},
            "xAxis": {
                "type": "value",
                "axisLabel": {
                    "formatter": "function(v){return (v/1000000).toFixed(1)+'M';}"
                },
            },
            "yAxis": {"type": "category", "data": categories},
            "series": [
                {
                    "type": "bar",
                    "data": values,
                    "label": {
                        "show": True,
                        "position": "right",
                        "formatter": "function(p){return 'KES '+Number(p.value).toLocaleString();}",
                    },
                }
            ],
        }
    ).classes(f"w-full h-[{height}px]")


def donut(
    *,
    title: str,
    df: pl.DataFrame,
    label: str,
    value: str,
    height: int = 420,
) -> None:
    """Render donut chart for composition analysis."""
    if df.is_empty():
        empty_state()
        return

    data = [
        {"name": row[label], "value": round(row[value] or 0, 0)}
        for row in df.iter_rows(named=True)
    ]

    ui.echart(
        {
            "title": {"text": title, "left": "center"},
            "tooltip": {"trigger": "item"},
            "legend": {"bottom": 0, "type": "scroll"},
            "series": [
                {
                    "type": "pie",
                    "radius": ["45%", "70%"],
                    "center": ["50%", "48%"],
                    "data": data,
                }
            ],
        }
    ).classes(f"w-full h-[{height}px]")


def treemap(
    *,
    title: str,
    df: pl.DataFrame,
    label: str,
    value: str,
    height: int = 460,
) -> None:
    """Render treemap for payment concentration."""
    if df.is_empty():
        empty_state()
        return

    data = [
        {"name": row[label], "value": round(row[value] or 0, 0)}
        for row in df.iter_rows(named=True)
    ]

    ui.echart(
        {
            "title": {"text": title, "left": "center"},
            "tooltip": {"formatter": "{b}<br/>KES {c}"},
            "series": [
                {
                    "type": "treemap",
                    "roam": False,
                    "nodeClick": False,
                    "breadcrumb": {"show": False},
                    "data": data,
                }
            ],
        }
    ).classes(f"w-full h-[{height}px]")