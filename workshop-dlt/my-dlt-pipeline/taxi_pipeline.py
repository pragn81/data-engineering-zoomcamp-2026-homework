from typing import Iterator, List, Dict, Any

import dlt
from dlt.sources.helpers import requests


BASE_URL = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"


@dlt.resource(write_disposition="append")
def nyc_taxi_data(page_size: int = 1000) -> Iterator[List[Dict[str, Any]]]:
    """
    REST API resource for NYC taxi data.

    The API is paginated JSON. We fetch pages of `page_size` records and
    stop when the API returns an empty page.
    """
    page = 1

    while True:
        response = requests.get(
            BASE_URL,
            params={"page": page, "page_size": page_size},
        )
        response.raise_for_status()

        data = response.json()
        # Stop when the API returns an empty page
        if not data:
            break

        yield data
        page += 1


@dlt.source
def taxi_pipeline() -> Any:
    """
    dlt source exposing the NYC taxi REST API resource.

    This is the `taxi_pipeline` source requested in the assignment.
    """
    return nyc_taxi_data()


def run_taxi_pipeline() -> None:
    """
    Create a dlt pipeline that loads NYC taxi data into DuckDB.
    """
    pipeline = dlt.pipeline(
        pipeline_name="taxi_pipeline",
        destination="duckdb",
        dataset_name="nyc_taxi",
    )

    load_info = pipeline.run(taxi_pipeline())
    print(load_info)  # noqa: T201


if __name__ == "__main__":
    run_taxi_pipeline()

