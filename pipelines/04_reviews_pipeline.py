"""Completed independently from the course review starter; preserve raw dirt."""
import json
from pathlib import Path
import dlt

ROOT = Path(__file__).resolve().parent.parent

@dlt.resource(name="reviews", write_disposition="replace")
def reviews():
    yield json.loads((ROOT / "data/raw/reviews.json").read_text(encoding="utf-8"))

def main():
    pipeline = dlt.pipeline(pipeline_name="shop_il_reviews",
        pipelines_dir=str(ROOT / ".dlt/pipelines"),
        destination=dlt.destinations.duckdb(str(ROOT / "warehouse.duckdb")), dataset_name="raw")
    print(pipeline.run(reviews()))

if __name__ == "__main__":
    main()
