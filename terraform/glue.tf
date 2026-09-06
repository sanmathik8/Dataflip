# AWS Glue Data Catalog database for DataFlip.
# Note: External tables within this database are dynamically discovered, created,
# and updated by the DataFlip Lambda engine based on uploaded Parquet datasets.
resource "aws_glue_catalog_database" "dataflip_db" {
  name        = "${var.project_name}_db"
  description = "Domain-agnostic database for DataFlip analytics tables"
}

