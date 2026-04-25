import asyncio
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Iterable

import polars as pl
from loguru import logger

from src.scrapers.sha_payments_scraper import ShaPaymentsScraper


class ShaFacilityBatchScraper:
    """
    Robust batch runner for scraping SHA payment records.

    Features:
    - Cleans facility names before searching.
    - Runs scraping in controlled batches.
    - Retries failed batches with delay.
    - Saves every successful batch to disk.
    - Saves progress to disk so reruns skip completed batches.
    """

    FACILITY_SUFFIXES = [
        "dispensary",
        "health centre",
        "health center",
        "hospital",
        "medical centre",
        "medical center",
        "clinic",
        "nursing home",
        "maternity",
        "mat clinic",
        "sub county hospital",
        "county referral hospital",
        "level 4 hospital",
        "level 5 hospital",
        "level 6 hospital",
    ]

    def __init__(
        self,
        *,
        scraper: ShaPaymentsScraper,
        output_dir: str | Path = "sha-portal-data",
        batch_size: int = 100,
        pause_between_batches_seconds: float = 5.0,
        max_retries: int = 3,
        retry_delay_seconds: float = 10.0,
    ) -> None:
        self.scraper = scraper
        self.output_dir = Path(output_dir)
        self.batch_size = batch_size
        self.pause_between_batches_seconds = pause_between_batches_seconds
        self.max_retries = max_retries
        self.retry_delay_seconds = retry_delay_seconds

        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.checkpoint_file = self.output_dir / "scraping_checkpoint.json"
        self.failed_batches_file = self.output_dir / "failed_batches.json"

    async def scrape_facilities(self, facility_names: Iterable[str]) -> pl.DataFrame:
        """
        Scrape SHA payments for many facility names.

        Already completed batches are skipped using the checkpoint file.
        """
        search_terms_df = self._prepare_search_terms(facility_names)
        checkpoint = self._load_checkpoint()

        logger.info(f"Prepared {search_terms_df.height:,} unique facility search rows.")
        logger.info(f"Already completed batches: {len(checkpoint['completed_batches']):,}")

        for batch_number, batch_df in enumerate(
            self._iter_batches(search_terms_df, self.batch_size),
            start=1,
        ):
            batch_file = self._get_batch_file(batch_number)

            if self._is_batch_completed(batch_number, batch_file, checkpoint):
                logger.info(f"Skipping completed batch {batch_number:,}")
                continue

            batch_search_terms = batch_df["search_term"].to_list()

            logger.info(
                f"Running batch {batch_number:,} "
                f"with {len(batch_search_terms):,} search terms..."
            )

            try:
                batch_results_df = await self._scrape_batch_with_retries(
                    batch_number=batch_number,
                    batch_search_terms=batch_search_terms,
                )

                if not batch_results_df.is_empty():
                    batch_results_df = batch_results_df.join(
                        batch_df,
                        on="search_term",
                        how="left",
                    )

                self._write_batch_excel(batch_results_df, batch_file)

                self._mark_batch_completed(
                    checkpoint=checkpoint,
                    batch_number=batch_number,
                    batch_file=batch_file,
                    rows_written=batch_results_df.height,
                )

                logger.info(f"Saved completed batch {batch_number:,} to {batch_file}")

            except Exception as exc:
                logger.exception(f"Batch {batch_number:,} failed permanently.")
                self._record_failed_batch(
                    batch_number=batch_number,
                    batch_search_terms=batch_search_terms,
                    error=str(exc),
                )

                # Continue with the next batch instead of killing the whole job.
                continue

            await asyncio.sleep(self.pause_between_batches_seconds)

        return self.combine_completed_batches()

    async def _scrape_batch_with_retries(
        self,
        *,
        batch_number: int,
        batch_search_terms: list[str],
    ) -> pl.DataFrame:
        """Scrape one batch with retry logic."""
        last_error: Exception | None = None

        for attempt in range(1, self.max_retries + 1):
            try:
                logger.info(
                    f"Batch {batch_number:,}: attempt {attempt}/{self.max_retries}"
                )

                return await self.scraper.scrape_many(batch_search_terms)

            except Exception as exc:
                last_error = exc

                logger.warning(
                    f"Batch {batch_number:,} failed on attempt "
                    f"{attempt}/{self.max_retries}: {exc}"
                )

                if attempt < self.max_retries:
                    delay = self.retry_delay_seconds * attempt
                    logger.info(f"Retrying batch {batch_number:,} in {delay} seconds...")
                    await asyncio.sleep(delay)

        raise RuntimeError(
            f"Batch {batch_number} failed after {self.max_retries} attempts."
        ) from last_error

    def combine_completed_batches(self) -> pl.DataFrame:
        """
        Combine all completed batch Excel files into one final Excel file.
        """
        checkpoint = self._load_checkpoint()
        batch_files = [
            Path(item["batch_file"])
            for item in checkpoint["completed_batches"].values()
            if Path(item["batch_file"]).exists()
        ]

        if not batch_files:
            logger.warning("No completed batch files found.")
            return pl.DataFrame()

        dataframes = [pl.read_excel(batch_file) for batch_file in batch_files]
        combined_df = pl.concat(dataframes, how="diagonal_relaxed")

        combined_output_file = self.output_dir / "sha_payments_combined.xlsx"

        combined_df.write_excel(
            combined_output_file,
            worksheet="SHA Payments",
            autofit=True,
            table_style="Table Style Medium 4",
        )

        logger.info(f"Saved combined file to {combined_output_file}")

        return combined_df

    def _prepare_search_terms(self, facility_names: Iterable[str]) -> pl.DataFrame:
        """Convert raw facility names into cleaned search terms."""
        records = []

        for facility_name in facility_names:
            clean_facility_name = self._clean_facility_name(facility_name)
            search_term = self._build_search_term(clean_facility_name)

            if not search_term:
                continue

            records.append(
                {
                    "source_facility_name": clean_facility_name,
                    "search_term": search_term,
                }
            )

        if not records:
            return pl.DataFrame(
                schema={
                    "source_facility_name": pl.String,
                    "search_term": pl.String,
                }
            )

        return (
            pl.DataFrame(records)
            .unique(subset=["source_facility_name", "search_term"])
            .sort("search_term")
        )

    def _build_search_term(self, facility_name: str) -> str:
        """Remove common facility-type words from a facility name."""
        search_term = facility_name.lower()

        for suffix in sorted(self.FACILITY_SUFFIXES, key=len, reverse=True):
            search_term = re.sub(rf"\b{re.escape(suffix)}\b", " ", search_term)

        return re.sub(r"\s+", " ", search_term).strip().title()

    @staticmethod
    def _clean_facility_name(facility_name: str) -> str:
        """Normalize whitespace and remove unnecessary punctuation."""
        facility_name = str(facility_name or "").strip()
        facility_name = re.sub(r"[^\w\s&'-]", " ", facility_name)
        return re.sub(r"\s+", " ", facility_name).strip()

    @staticmethod
    def _iter_batches(dataframe: pl.DataFrame, batch_size: int):
        """Yield DataFrame batches of a fixed size."""
        for offset in range(0, dataframe.height, batch_size):
            yield dataframe.slice(offset, batch_size)

    def _get_batch_file(self, batch_number: int) -> Path:
        """Return the output path for a batch."""
        return self.output_dir / f"sha_payments_batch_{batch_number:04d}.xlsx"

    @staticmethod
    def _write_batch_excel(dataframe: pl.DataFrame, output_file: Path) -> None:
        """Write one batch result to Excel."""
        dataframe.write_excel(
            output_file,
            worksheet="SHA Payments",
            autofit=True,
            table_style="Table Style Medium 4",
        )

    def _load_checkpoint(self) -> dict:
        """Load scraping progress from disk."""
        if not self.checkpoint_file.exists():
            return {
                "completed_batches": {},
                "updated_at": None,
            }

        with self.checkpoint_file.open("r", encoding="utf-8") as file:
            return json.load(file)

    def _save_checkpoint(self, checkpoint: dict) -> None:
        """Persist scraping progress to disk."""
        checkpoint["updated_at"] = datetime.now().isoformat(timespec="seconds")

        with self.checkpoint_file.open("w", encoding="utf-8") as file:
            json.dump(checkpoint, file, indent=2)

    def _is_batch_completed(
        self,
        batch_number: int,
        batch_file: Path,
        checkpoint: dict,
    ) -> bool:
        """Check whether a batch was already completed successfully."""
        batch_key = str(batch_number)

        return (
            batch_key in checkpoint["completed_batches"]
            and batch_file.exists()
            and batch_file.stat().st_size > 0
        )

    def _mark_batch_completed(
        self,
        *,
        checkpoint: dict,
        batch_number: int,
        batch_file: Path,
        rows_written: int,
    ) -> None:
        """Mark a batch as completed in the checkpoint file."""
        checkpoint["completed_batches"][str(batch_number)] = {
            "batch_number": batch_number,
            "batch_file": str(batch_file),
            "rows_written": rows_written,
            "completed_at": datetime.now().isoformat(timespec="seconds"),
        }

        self._save_checkpoint(checkpoint)

    def _record_failed_batch(
        self,
        *,
        batch_number: int,
        batch_search_terms: list[str],
        error: str,
    ) -> None:
        """Save failed batch details for later review or rerun."""
        failed_batches = []

        if self.failed_batches_file.exists():
            with self.failed_batches_file.open("r", encoding="utf-8") as file:
                failed_batches = json.load(file)

        failed_batches.append(
            {
                "batch_number": batch_number,
                "search_terms": batch_search_terms,
                "error": error,
                "failed_at": datetime.now().isoformat(timespec="seconds"),
            }
        )

        with self.failed_batches_file.open("w", encoding="utf-8") as file:
            json.dump(failed_batches, file, indent=2)


if __name__ == "__main__":

    async def main() -> None:
        facilities_df_path = (
            Path(__file__).parent.parent / "datasets/facilities_heirarchy.xlsx"
        )

        facilities_df = pl.read_excel(facilities_df_path)
        facility_names = facilities_df.select("facility_name").to_series().to_list()

        scraper = ShaPaymentsScraper(
            timeout=45,
            concurrency=3,
        )

        batch_runner = ShaFacilityBatchScraper(
            scraper=scraper,
            output_dir="../datasets/sha-portal-data",
            batch_size=100,
            pause_between_batches_seconds=8,
            max_retries=4,
            retry_delay_seconds=15,
        )

        final_df = await batch_runner.scrape_facilities(facility_names)

        logger.info(final_df.head())

    asyncio.run(main())