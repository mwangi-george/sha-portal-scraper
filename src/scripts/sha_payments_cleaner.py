from pathlib import Path

import polars as pl
from loguru import logger

sha_payments_raw_df_path = Path(__file__).parent.parent / "datasets/sha-portal-data/sha_payments_combined.xlsx"
sha_payments_raw_df = pl.read_excel(sha_payments_raw_df_path, infer_schema_length=10_000)

logger.info(sha_payments_raw_df.head())

sha_payments_clean_df = (
    sha_payments_raw_df
    .drop("search_term", "source_facility_name")
    .unique(["facility_name", "fid"])
    .with_columns(
        pl.col("phc_amount_kes").cast(pl.Float64).alias("phc_amount_kes"),
    )
)

logger.info(sha_payments_clean_df.shape)
logger.info(sha_payments_clean_df.head())



