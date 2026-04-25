import polars as pl

from src.config import settings

def fetch_facilities():
    facilities_df = pl.read_database_uri(
        query="select * from facility_hierarchy",
        uri=settings.facilities_database_url,
    )

    facilities_df.write_excel("datasets/facilities_heirarchy.xlsx")


if __name__ == "__main__":
    fetch_facilities()