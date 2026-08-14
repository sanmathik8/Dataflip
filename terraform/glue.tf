resource "aws_glue_catalog_database" "dataflip_db" {
  name        = "${var.project_name}_db"
  description = "Database for DataFlip analytics"
}

resource "aws_glue_catalog_table" "sales_curated" {
  name          = "sales_curated"
  database_name = aws_glue_catalog_database.dataflip_db.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "classification" = "parquet"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.analytics.id}/curated/blue/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "ParquetHiveSerDe"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "order_id"
      type = "bigint"
    }
    columns {
      name = "order_date"
      type = "date"
    }
    columns {
      name = "product"
      type = "string"
    }
    columns {
      name = "category"
      type = "string"
    }
    columns {
      name = "quantity"
      type = "int"
    }
    columns {
      name = "unit_price"
      type = "double"
    }
    columns {
      name = "region"
      type = "string"
    }
    columns {
      name = "revenue"
      type = "double"
    }
  }
}
