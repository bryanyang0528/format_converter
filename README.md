# format_converter
Convert file from csv to parquet through Apache Spark

## Usage

1. `export SAPRK_MASTER=spark://xxxx` (deafult: local[1])
2. `python convert.py --in s3://bucket/folder/from.csv --out s3://bucket/folder/to.parquet --mode [append/error/overwrite]`
