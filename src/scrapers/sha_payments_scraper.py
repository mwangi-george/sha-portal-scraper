import asyncio
import re
from dataclasses import dataclass
from typing import Iterable

import niquests
import polars as pl
from bs4 import BeautifulSoup


@dataclass(frozen=True)
class ShaPaymentRecord:
    """Structured SHA payment record for one facility."""

    search_term: str
    facility_name: str | None
    fid: str | None
    registration: str | None
    registry_name: str | None
    keph_level: str | None
    ownership: str | None
    facility_type: str | None
    county: str | None
    sub_county: str | None
    latitude: float | None
    longitude: float | None
    phc_amount_kes: float | None


class ShaPaymentsScraper:
    """Scrape SHA facility payment results from the public search page."""

    BASE_URL = "https://sha-payments-deploy.onrender.com/"

    FIELD_LABELS = [
        "Registry name",
        "KEPH level",
        "Ownership",
        "Facility type",
        "County",
        "Sub-county",
        "Latitude",
        "Longitude",
        "PHC Amount",
    ]

    def __init__(self, timeout: int = 30, concurrency: int = 5) -> None:
        self.timeout = timeout
        self.concurrency = concurrency

    async def fetch_html(
        self,
        session: niquests.AsyncSession,
        search_term: str,
    ) -> str:
        """Fetch the HTML search results for one search term."""
        response = await session.get(
            self.BASE_URL,
            params={"q": search_term},
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.text

    def parse_html(self, html: str, search_term: str) -> list[ShaPaymentRecord]:
        """Parse SHA payment records from one HTML response."""
        soup = BeautifulSoup(html, "html.parser")
        records: list[ShaPaymentRecord] = []

        for heading in soup.find_all("h3"):
            facility_name = heading.get_text(" ", strip=True)
            section_text = self._extract_facility_section_text(heading)

            if not section_text:
                continue

            records.append(
                ShaPaymentRecord(
                    search_term=search_term,
                    facility_name=facility_name,
                    fid=self._extract_fid(section_text),
                    registration=self._extract_registration(section_text),
                    registry_name=self._extract_label_value(section_text, "Registry name"),
                    keph_level=self._extract_label_value(section_text, "KEPH level"),
                    ownership=self._extract_label_value(section_text, "Ownership"),
                    facility_type=self._extract_label_value(section_text, "Facility type"),
                    county=self._extract_label_value(section_text, "County"),
                    sub_county=self._extract_label_value(section_text, "Sub-county"),
                    latitude=self._to_float(self._extract_label_value(section_text, "Latitude")),
                    longitude=self._to_float(self._extract_label_value(section_text, "Longitude")),
                    phc_amount_kes=self._extract_amount(section_text),
                )
            )

        return records

    async def scrape_one(
        self,
        session: niquests.AsyncSession,
        search_term: str,
    ) -> list[ShaPaymentRecord]:
        """Fetch and parse payment records for one search term."""
        html = await self.fetch_html(session=session, search_term=search_term)
        return self.parse_html(html=html, search_term=search_term)

    async def scrape_many(self, search_terms: Iterable[str]) -> pl.DataFrame:
        """Scrape multiple search terms concurrently and return a Polars DataFrame."""
        semaphore = asyncio.Semaphore(self.concurrency)

        async with niquests.AsyncSession() as session:

            async def scrape_with_limit(term: str) -> list[ShaPaymentRecord]:
                async with semaphore:
                    return await self.scrape_one(session=session, search_term=term)

            results = await asyncio.gather(
                *(scrape_with_limit(term) for term in search_terms)
            )

        records = [record.__dict__ for result in results for record in result]

        if not records:
            return self._empty_dataframe()

        return (
            pl.DataFrame(records)
            .unique(subset=["fid", "facility_name", "phc_amount_kes"])
            .sort(["search_term", "county", "facility_name"])
        )

    def _extract_facility_section_text(self, heading) -> str:
        """Extract all text belonging to one facility result card."""
        # Prefer the nearest parent block that contains the full facility details.
        for parent in heading.parents:
            text = parent.get_text(" ", strip=True)

            if "PHC Amount" in text and text.count("PHC Amount") == 1:
                return self._clean_text(text)

        # Fallback: collect siblings until the next facility heading.
        section_parts = [heading.get_text(" ", strip=True)]

        for sibling in heading.find_next_siblings():
            if sibling.name == "h3":
                break
            section_parts.append(sibling.get_text(" ", strip=True))

        return self._clean_text(" ".join(section_parts))

    def _extract_label_value(self, text: str, label: str) -> str | None:
        """Extract a field value using known SHA result labels."""
        next_labels = [item for item in self.FIELD_LABELS if item != label]

        pattern = (
            rf"{re.escape(label)}\s+"
            rf"(.+?)"
            rf"(?=\s+(?:{'|'.join(map(re.escape, next_labels))})\s+|$)"
        )

        match = re.search(pattern, text, flags=re.IGNORECASE)

        if not match:
            return None

        value = match.group(1).strip(" ·:-")
        return None if value in {"", "—", "-"} else value

    @staticmethod
    def _extract_fid(text: str) -> str | None:
        """Extract SHA facility identifier."""
        match = re.search(r"\b(FID-\d+-\d+-\d+)\b", text)
        return match.group(1) if match else None

    @staticmethod
    def _extract_registration(text: str) -> str | None:
        """Extract facility registration number."""
        match = re.search(
            r"Registration\s+(.+?)(?=\s+Registry name|\s+KEPH level|$)",
            text,
            flags=re.IGNORECASE,
        )
        return match.group(1).strip(" ·:-") if match else None

    @staticmethod
    def _extract_amount(text: str) -> float | None:
        """Extract PHC amount as a numeric value."""
        match = re.search(
            r"PHC Amount\s+([\d,]+(?:\.\d+)?)\s+KES",
            text,
            flags=re.IGNORECASE,
        )
        return float(match.group(1).replace(",", "")) if match else None

    @staticmethod
    def _to_float(value: str | None) -> float | None:
        """Convert a string value to float safely."""
        if value is None:
            return None

        try:
            return float(value.replace(",", ""))
        except ValueError:
            return None

    @staticmethod
    def _clean_text(value: str) -> str:
        """Normalize whitespace for easier regex parsing."""
        return re.sub(r"\s+", " ", value).strip()

    @staticmethod
    def _empty_dataframe() -> pl.DataFrame:
        """Return an empty DataFrame with the expected schema."""
        return pl.DataFrame(
            schema={
                "search_term": pl.String,
                "facility_name": pl.String,
                "fid": pl.String,
                "registration": pl.String,
                "registry_name": pl.String,
                "keph_level": pl.String,
                "ownership": pl.String,
                "facility_type": pl.String,
                "county": pl.String,
                "sub_county": pl.String,
                "latitude": pl.Float64,
                "longitude": pl.Float64,
                "phc_amount_kes": pl.Float64,
            }
        )



if __name__ == "__main__":
    async def main() -> None:
        """Run the SHA scraper and export results to Excel."""
        scraper = ShaPaymentsScraper(timeout=30, concurrency=5)

        df = await scraper.scrape_many(
            [
                "ngara",
                # Add more facility names/search terms here
                "APU",
                "Afya",
                "25062"
            ]
        )

        print(df)

        df.write_excel(
            "sha_payments_resultsxxx.xlsx",
            worksheet="SHA Payments",
            autofit=True,
            table_style="Table Style Medium 4",
        )

    asyncio.run(main())