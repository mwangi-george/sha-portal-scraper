import polars as pl
from nicegui import ui


MONEY_COLUMNS = {
    "phc_amount_kes",
    "total_amount",
    "average_amount",
    "median_amount",
    "highest_facility_amount",
}


def prepare_rows(df: pl.DataFrame) -> list[dict]:
    """Convert Polars DataFrame rows into table-friendly dictionaries."""
    rows = []

    for row in df.to_dicts():
        clean_row = {}

        for key, value in row.items():
            if key in MONEY_COLUMNS and value is not None:
                clean_row[key] = f"KES {value:,.0f}"
            elif key.endswith("_percent") and value is not None:
                clean_row[key] = f"{value:,.1f}%"
            else:
                clean_row[key] = value

        rows.append(clean_row)

    return rows


def data_table(
    *,
    df: pl.DataFrame,
    title: str,
    rows_per_page: int = 15,
) -> None:
    """Render a searchable, sortable stakeholder table."""
    with ui.card().classes("w-full rounded-2xl shadow-sm border border-slate-200 p-4"):
        ui.label(title).classes("text-lg font-semibold text-slate-900 mb-3")

        if df.is_empty():
            ui.label("No records match the selected filters.").classes("text-slate-500")
            return

        columns = [
            {
                "name": column,
                "label": column.replace("_", " ").title(),
                "field": column,
                "sortable": True,
                "align": "left",
            }
            for column in df.columns
        ]

        table = ui.table(
            columns=columns,
            rows=prepare_rows(df),
            pagination={"rowsPerPage": rows_per_page},
        ).classes("w-full")

        with table.add_slot("top-right"):
            with ui.input(placeholder="Search table...").props("dense outlined clearable") as search:
                search.bind_value(table, "filter")