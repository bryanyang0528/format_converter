# format_converter
Convert file from csv to parquet through Apache Spark

## Usage

1. `export SAPRK_HOME=/spark/path`
2. `python convert.py --in s3://bucket/folder/from.csv --out s3://bucket/folder/to.parquet --mode [append/error/overwrite]`
