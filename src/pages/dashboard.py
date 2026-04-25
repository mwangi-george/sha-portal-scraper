from nicegui import ui

from src.components.charts import donut, horizontal_bar, treemap
from src.components.kpis import executive_card, kes, number
from src.components.tables import data_table
from src.config import EXPORT_DIR, SHA_PAYMENTS_DATA_PATH
from src.services.payments_service import ShaPaymentsService


class ExecutiveDashboard:
    """Interactive SHA payments dashboard page."""

    def __init__(self) -> None:
        self.service = ShaPaymentsService(SHA_PAYMENTS_DATA_PATH)
        self.base_df = self.service.load_clean_data()

        self.filtered_df = self.base_df

        self.county_select = None
        self.sub_county_select = None
        self.facility_type_select = None
        self.ownership_select = None
        self.keph_level_select = None
        self.search_input = None
        self.min_amount_input = None
        self.max_amount_input = None

        self.content_area = None

    def render(self) -> None:
        """Render the dashboard shell and first view."""
        ui.page_title("SHA Payments Intelligence Dashboard")

        ui.add_head_html(
            """
            <style>
                body {
                    background: #f8fafc;
                }
            </style>
            """
        )

        with ui.header().classes(
            "bg-white text-slate-900 border-b border-slate-200 px-6"
        ):
            ui.icon("local_hospital").classes("text-blue-600 text-3xl")
            with ui.column().classes("gap-0"):
                ui.label("SHA Payments Intelligence Dashboard").classes(
                    "text-xl font-bold"
                )
                ui.label("PHC payment visibility for county health teams").classes(
                    "text-xs text-slate-500"
                )
            ui.space()
            ui.button(
                "Export Filtered Data",
                icon="download",
                on_click=self.export_filtered_data,
            ).props("unelevated color=primary")

        with ui.column().classes("w-full p-6 gap-5"):
            self.render_filters()

            self.content_area = ui.column().classes("w-full gap-5")
            self.refresh_dashboard()

    def render_filters(self) -> None:
        """Render dashboard filter controls."""
        with ui.card().classes(
            "w-full rounded-2xl shadow-sm border border-slate-200 p-4"
        ):
            with ui.row().classes("items-center justify-between w-full mb-2"):
                with ui.column().classes("gap-0"):
                    ui.label("Analysis Filters").classes("text-lg font-semibold")
                    ui.label(
                        "Use these controls to narrow analysis by geography, facility profile, or payment range."
                    ).classes("text-sm text-slate-500")

                ui.button("Reset Filters", icon="restart_alt", on_click=self.reset_filters).props(
                    "flat color=primary"
                )

            with ui.grid(columns=4).classes("w-full gap-4"):
                self.county_select = ui.select(
                    self.service.options(self.base_df, "county"),
                    label="County",
                    value="ALL",
                    on_change=self.on_county_change,
                ).props("outlined dense clearable").classes("w-full")

                self.sub_county_select = ui.select(
                    self.service.options(self.base_df, "sub_county"),
                    label="Sub-county",
                    value="ALL",
                    on_change=self.refresh_dashboard,
                ).props("outlined dense clearable").classes("w-full")

                self.facility_type_select = ui.select(
                    self.service.options(self.base_df, "facility_type"),
                    label="Facility Type",
                    value="ALL",
                    on_change=self.refresh_dashboard,
                ).props("outlined dense clearable").classes("w-full")

                self.ownership_select = ui.select(
                    self.service.options(self.base_df, "ownership"),
                    label="Ownership",
                    value="ALL",
                    on_change=self.refresh_dashboard,
                ).props("outlined dense clearable").classes("w-full")

            with ui.grid(columns=4).classes("w-full gap-4 mt-3"):
                self.keph_level_select = ui.select(
                    self.service.options(self.base_df, "keph_level"),
                    label="KEPH Level",
                    value="ALL",
                    on_change=self.refresh_dashboard,
                ).props("outlined dense clearable").classes("w-full")

                self.search_input = ui.input(
                    label="Facility or FID Search",
                    placeholder="e.g. NGARA or FID-47",
                    on_change=self.refresh_dashboard,
                ).props("outlined dense clearable").classes("w-full")

                self.min_amount_input = ui.number(
                    label="Minimum Amount",
                    value=None,
                    on_change=self.refresh_dashboard,
                ).props("outlined dense clearable").classes("w-full")

                self.max_amount_input = ui.number(
                    label="Maximum Amount",
                    value=None,
                    on_change=self.refresh_dashboard,
                ).props("outlined dense clearable").classes("w-full")

    def on_county_change(self) -> None:
        """Update sub-county options after county selection changes."""
        county = self.county_select.value

        if county and county != "ALL":
            county_df = self.base_df.filter(self.base_df["county"] == county)
            self.sub_county_select.options = self.service.options(county_df, "sub_county")
        else:
            self.sub_county_select.options = self.service.options(self.base_df, "sub_county")

        self.sub_county_select.value = "ALL"
        self.refresh_dashboard()

    def refresh_dashboard(self) -> None:
        """Refresh all dashboard visuals based on active filters."""
        self.filtered_df = self.service.filter_data(
            self.base_df,
            county=self.county_select.value if self.county_select else "ALL",
            sub_county=self.sub_county_select.value if self.sub_county_select else "ALL",
            facility_type=self.facility_type_select.value if self.facility_type_select else "ALL",
            ownership=self.ownership_select.value if self.ownership_select else "ALL",
            keph_level=self.keph_level_select.value if self.keph_level_select else "ALL",
            search_text=self.search_input.value if self.search_input else None,
            min_amount=self.min_amount_input.value if self.min_amount_input else None,
            max_amount=self.max_amount_input.value if self.max_amount_input else None,
        )

        self.content_area.clear()

        with self.content_area:
            self.render_kpis()
            self.render_insights()
            self.render_visuals()
            self.render_tables()

    def render_kpis(self) -> None:
        """Render executive KPI cards."""
        metrics = self.service.metrics(self.filtered_df)

        with ui.grid(columns=5).classes("w-full gap-4"):
            executive_card(
                "Total PHC Amount",
                kes(metrics["total_amount"]),
                "Filtered payment value",
                "payments",
            )
            executive_card(
                "Facilities",
                number(metrics["facility_count"]),
                "Unique facility IDs",
                "apartment",
            )
            executive_card(
                "Counties",
                number(metrics["county_count"]),
                "Counties in current view",
                "map",
            )
            executive_card(
                "Sub-counties",
                number(metrics["sub_county_count"]),
                "Sub-counties in current view",
                "hub",
            )
            executive_card(
                "Average Payment",
                kes(metrics["average_amount"]),
                "Mean facility payment",
                "analytics",
            )
            # executive_card(
            #     "Highest Payment",
            #     kes(metrics["max_amount"]),
            #     "Largest facility payment",
            #     "trending_up",
            # )

    def render_insights(self) -> None:
        """Render quick analytical interpretation for stakeholders."""
        if self.filtered_df.is_empty():
            return

        county_summary = self.service.summarize(self.filtered_df, "county")
        top_county = county_summary.row(0, named=True) if not county_summary.is_empty() else None

        facility_summary = self.service.top_facilities(self.filtered_df, limit=1)
        top_facility = facility_summary.row(0, named=True) if not facility_summary.is_empty() else None

        with ui.card().classes(
            "w-full rounded-2xl shadow-sm border border-blue-100 bg-blue-50 p-5"
        ):
            ui.label("Key Readout").classes("text-lg font-semibold text-blue-950")

            if top_county:
                ui.label(
                    f"{top_county['county']} accounts for the largest share of payments "
                    f"in the current view at KES {top_county['total_amount']:,.0f} "
                    f"across {top_county['facilities']:,} facilities."
                ).classes("text-sm text-blue-900")

            if top_facility:
                ui.label(
                    f"The highest-paid facility is {top_facility['facility_name']} "
                    f"with KES {top_facility['phc_amount_kes']:,.0f}."
                ).classes("text-sm text-blue-900")

    def render_visuals(self) -> None:
        """Render main analytical charts."""
        county_summary = self.service.summarize(self.filtered_df, "county", limit=15)
        sub_county_summary = self.service.summarize(self.filtered_df, "sub_county", limit=20)
        ownership_summary = self.service.summarize(self.filtered_df, "ownership")
        facility_type_summary = self.service.summarize(self.filtered_df, "facility_type")
        payment_bands = self.service.payment_bands(self.filtered_df)

        with ui.grid(columns=2).classes("w-full gap-5"):
            with ui.card().classes("rounded-2xl shadow-sm border border-slate-200 p-4"):
                horizontal_bar(
                    title="Top Counties by PHC Payments",
                    df=county_summary,
                    category="county",
                    value="total_amount",
                    height=460,
                )

            with ui.card().classes("rounded-2xl shadow-sm border border-slate-200 p-4"):
                treemap(
                    title="Sub-county Payment Concentration",
                    df=sub_county_summary,
                    label="sub_county",
                    value="total_amount",
                    height=460,
                )

        with ui.grid(columns=3).classes("w-full gap-5"):
            with ui.card().classes("rounded-2xl shadow-sm border border-slate-200 p-4"):
                donut(
                    title="Payments by Ownership",
                    df=ownership_summary,
                    label="ownership",
                    value="total_amount",
                    height=400,
                )

            with ui.card().classes("rounded-2xl shadow-sm border border-slate-200 p-4"):
                donut(
                    title="Payments by Facility Type",
                    df=facility_type_summary,
                    label="facility_type",
                    value="total_amount",
                    height=400,
                )

            with ui.card().classes("rounded-2xl shadow-sm border border-slate-200 p-4"):
                donut(
                    title="Facilities by Payment Band",
                    df=payment_bands,
                    label="payment_band",
                    value="facilities",
                    height=400,
                )

    def render_tables(self) -> None:
        """Render searchable stakeholder tables."""
        county_summary = self.service.summarize(self.filtered_df, "county")
        sub_county_summary = self.service.summarize(self.filtered_df, "sub_county")
        top_facilities = self.service.top_facilities(self.filtered_df, limit=200)

        with ui.tabs().classes("w-full") as tabs:
            county_tab = ui.tab("County Summary")
            subcounty_tab = ui.tab("Sub-county Summary")
            facilities_tab = ui.tab("Facility Explorer")

        with ui.tab_panels(tabs, value=county_tab).classes("w-full"):
            with ui.tab_panel(county_tab):
                data_table(
                    df=county_summary,
                    title="County-Level Payment Summary",
                    rows_per_page=15,
                )

            with ui.tab_panel(subcounty_tab):
                data_table(
                    df=sub_county_summary,
                    title="Sub-county-Level Payment Summary",
                    rows_per_page=20,
                )

            with ui.tab_panel(facilities_tab):
                data_table(
                    df=top_facilities,
                    title="Searchable Facility Payment Explorer",
                    rows_per_page=25,
                )

    def reset_filters(self) -> None:
        """Reset all dashboard filters."""
        self.county_select.value = "ALL"
        self.sub_county_select.value = "ALL"
        self.facility_type_select.value = "ALL"
        self.ownership_select.value = "ALL"
        self.keph_level_select.value = "ALL"
        self.search_input.value = ""
        self.min_amount_input.value = None
        self.max_amount_input.value = None
        self.refresh_dashboard()

    def export_filtered_data(self) -> None:
        """Export currently filtered data to Excel."""
        export_path = EXPORT_DIR / "sha_payments_filtered_export.xlsx"

        self.filtered_df.write_excel(
            export_path,
            worksheet="Filtered Payments",
            autofit=True,
            table_style="Table Style Medium 4",
        )

        ui.notify(
            f"Filtered data exported to {export_path}",
            type="positive",
            position="top-right",
        )