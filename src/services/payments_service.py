from datetime import date
from pathlib import Path

import polars as pl
from loguru import logger


class ShaPaymentsService:
    """Service for loading, cleaning, filtering, and summarizing SHA payment data."""

    def __init__(self, data_path: Path) -> None:
        self.data_path = data_path

    def load_clean_data(self) -> pl.DataFrame:
        """Load SHA payment data and apply dashboard-ready cleaning."""
        if not self.data_path.exists():
            raise FileNotFoundError(f"Dataset not found: {self.data_path}")

        raw_df = pl.read_excel(
            self.data_path,
            infer_schema_length=10_000,
        )

        logger.info(f"Raw SHA payments shape: {raw_df.shape}")

        clean_df = (
            raw_df
            .drop(["search_term", "source_facility_name"], strict=False)
            .unique(["facility_name", "fid"])
            .with_columns(
                pl.col("facility_name").cast(pl.String).str.to_uppercase(),
                pl.col("fid").cast(pl.String),
                pl.col("county").fill_null("UNKNOWN").cast(pl.String).str.to_uppercase(),
                pl.col("sub_county").fill_null("UNKNOWN").cast(pl.String).str.to_uppercase(),
                pl.col("facility_type").fill_null("UNKNOWN").cast(pl.String).str.to_uppercase(),
                pl.col("ownership").fill_null("UNKNOWN").cast(pl.String).str.to_uppercase(),
                pl.col("keph_level").fill_null("UNKNOWN").cast(pl.String).str.to_uppercase(),
                pl.col("phc_amount_kes").cast(pl.Float64),
            )
            .filter(pl.col("phc_amount_kes").is_not_null())
        )

        clean_df.write_excel(
            Path(__file__).parent.parent / "datasets" / f"sha_payments_dataset_as_of_{date.today()}.xlsx",
            worksheet="SHA Payments",
            autofit=True,
            table_style="Table Style Medium 4",
        )

        logger.info(f"Clean SHA payments shape: {clean_df.shape}")

        return clean_df

    @staticmethod
    def filter_data(
        df: pl.DataFrame,
        *,
        county: str | None = None,
        sub_county: str | None = None,
        facility_type: str | None = None,
        ownership: str | None = None,
        keph_level: str | None = None,
        search_text: str | None = None,
        min_amount: float | None = None,
        max_amount: float | None = None,
    ) -> pl.DataFrame:
        """Apply stakeholder dashboard filters."""
        filtered_df = df

        if county and county != "ALL":
            filtered_df = filtered_df.filter(pl.col("county") == county)

        if sub_county and sub_county != "ALL":
            filtered_df = filtered_df.filter(pl.col("sub_county") == sub_county)

        if facility_type and facility_type != "ALL":
            filtered_df = filtered_df.filter(pl.col("facility_type") == facility_type)

        if ownership and ownership != "ALL":
            filtered_df = filtered_df.filter(pl.col("ownership") == ownership)

        if keph_level and keph_level != "ALL":
            filtered_df = filtered_df.filter(pl.col("keph_level") == keph_level)

        if search_text:
            search_value = search_text.strip().upper()
            filtered_df = filtered_df.filter(
                pl.col("facility_name").str.contains(search_value, literal=True)
                | pl.col("fid").str.contains(search_value, literal=True)
            )

        if min_amount is not None:
            filtered_df = filtered_df.filter(pl.col("phc_amount_kes") >= min_amount)

        if max_amount is not None:
            filtered_df = filtered_df.filter(pl.col("phc_amount_kes") <= max_amount)

        return filtered_df

    @staticmethod
    def options(df: pl.DataFrame, column: str) -> list[str]:
        """Return sorted select options for a categorical column."""
        values = (
            df
            .select(pl.col(column).drop_nulls().unique().sort())
            .to_series()
            .to_list()
        )
        return ["ALL", *values]

    @staticmethod
    def metrics(df: pl.DataFrame) -> dict:
        """Calculate executive KPI metrics."""
        if df.is_empty():
            return {
                "total_amount": 0,
                "facility_count": 0,
                "county_count": 0,
                "sub_county_count": 0,
                "average_amount": 0,
                "median_amount": 0,
                "max_amount": 0,
            }

        return {
            "total_amount": df.select(pl.col("phc_amount_kes").sum()).item(),
            "facility_count": df.select(pl.col("fid").n_unique()).item(),
            "county_count": df.select(pl.col("county").n_unique()).item(),
            "sub_county_count": df.select(pl.col("sub_county").n_unique()).item(),
            "average_amount": df.select(pl.col("phc_amount_kes").mean()).item(),
            "median_amount": df.select(pl.col("phc_amount_kes").median()).item(),
            "max_amount": df.select(pl.col("phc_amount_kes").max()).item(),
        }

    @staticmethod
    def summarize(df: pl.DataFrame, dimension: str, limit: int | None = None) -> pl.DataFrame:
        """Aggregate payments by a selected dimension."""
        if df.is_empty():
            return pl.DataFrame()

        summary_df = (
            df
            .group_by(dimension)
            .agg(
                pl.col("fid").n_unique().alias("facilities"),
                pl.col("phc_amount_kes").sum().alias("total_amount"),
                pl.col("phc_amount_kes").mean().alias("average_amount"),
                pl.col("phc_amount_kes").median().alias("median_amount"),
                pl.col("phc_amount_kes").max().alias("highest_facility_amount"),
            )
            .with_columns(
                (
                    pl.col("total_amount") / pl.col("total_amount").sum() * 100
                ).alias("share_of_total_percent")
            )
            .sort("total_amount", descending=True)
        )

        return summary_df.head(limit) if limit else summary_df

    @staticmethod
    def top_facilities(df: pl.DataFrame, limit: int = 50) -> pl.DataFrame:
        """Return highest-paid facilities."""
        return (
            df
            .select(
                "facility_name",
                "fid",
                "county",
                "sub_county",
                "facility_type",
                "ownership",
                "keph_level",
                "phc_amount_kes",
            )
            .sort("phc_amount_kes", descending=True)
            .head(limit)
        )

    @staticmethod
    def payment_bands(df: pl.DataFrame) -> pl.DataFrame:
        """Group facilities into useful payment bands."""
        if df.is_empty():
            return pl.DataFrame()

        return (
            df
            .with_columns(
                pl.when(pl.col("phc_amount_kes") < 100_000)
                .then(pl.lit("<100K"))
                .when(pl.col("phc_amount_kes") < 500_000)
                .then(pl.lit("100K–500K"))
                .when(pl.col("phc_amount_kes") < 1_000_000)
                .then(pl.lit("500K–1M"))
                .when(pl.col("phc_amount_kes") < 5_000_000)
                .then(pl.lit("1M–5M"))
                .otherwise(pl.lit("5M+"))
                .alias("payment_band")
            )
            .group_by("payment_band")
            .agg(
                pl.col("fid").n_unique().alias("facilities"),
                pl.col("phc_amount_kes").sum().alias("total_amount"),
            )
            .sort("total_amount", descending=True)
        )