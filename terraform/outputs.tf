output "s3_bucket_name" {
  value       = aws_s3_bucket.analytics.id
  description = "Name of the analytics S3 bucket"
}

output "glue_database_name" {
  value       = aws_glue_catalog_database.dataflip_db.name
  description = "Glue Catalog Database Name"
}

output "glue_table_name" {
  value       = aws_glue_catalog_table.sales_curated.name
  description = "Glue Catalog Table Name"
}

output "lambda_function_arn" {
  value       = aws_lambda_function.dataflip_processor.arn
  description = "DataFlip Lambda Function ARN"
}
