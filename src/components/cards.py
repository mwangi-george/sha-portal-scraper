from nicegui import ui


def format_kes(value: float | int | None) -> str:
    """Format a numeric value as Kenyan Shillings."""
    if value is None:
        return "KES 0"

    return f"KES {value:,.0f}"


def metric_card(title: str, value: str, subtitle: str | None = None) -> None:
    """Render a dashboard KPI card."""
    with ui.card().classes(
        "w-full rounded-2xl shadow-sm border border-gray-100 p-4"
    ):
        ui.label(title).classes("text-sm text-gray-500")
        ui.label(value).classes("text-2xl font-bold text-gray-900")

        if subtitle:
            ui.label(subtitle).classes("text-xs text-gray-400 mt-1")