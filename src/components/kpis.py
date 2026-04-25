from nicegui import ui


def kes(value: float | int | None) -> str:
    """Format numeric value as KES."""
    return f"KES {value or 0:,.0f}"


def number(value: float | int | None) -> str:
    """Format numeric count."""
    return f"{value or 0:,.0f}"


def executive_card(title: str, value: str, subtitle: str, icon: str) -> None:
    """Render a polished KPI card."""
    with ui.card().classes(
        "rounded-2xl p-5 shadow-sm border border-slate-200 bg-white"
    ):
        with ui.row().classes("items-center justify-between w-full"):
            with ui.column().classes("gap-1"):
                ui.label(title).classes("text-xs uppercase tracking-wide text-slate-500")
                ui.label(value).classes("text-2xl font-bold text-slate-900")
                ui.label(subtitle).classes("text-xs text-slate-400")
            ui.icon(icon).classes(
                "text-3xl text-blue-600 bg-blue-50 rounded-xl p-2"
            )