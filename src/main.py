from nicegui import ui

from src.pages.dashboard import ExecutiveDashboard


@ui.page("/")
def index() -> None:
    """Dashboard entrypoint."""
    dashboard = ExecutiveDashboard()
    dashboard.render()


ui.run(
    title="SHA Payments Intelligence Dashboard",
    reload=True,
    show=True,
)