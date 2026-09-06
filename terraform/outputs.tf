output "s3_bucket_name" {
  value       = aws_s3_bucket.analytics.id
  description = "Name of the analytics S3 bucket"
}

output "s3_bucket_arn" {
  value       = aws_s3_bucket.analytics.arn
  description = "ARN of the analytics S3 bucket"
}

output "glue_database_name" {
  value       = aws_glue_catalog_database.dataflip_db.name
  description = "Glue Catalog Database Name"
}

output "glue_database_arn" {
  value       = aws_glue_catalog_database.dataflip_db.arn
  description = "Glue Catalog Database ARN"
}


output "lambda_function_arn" {
  value       = aws_lambda_function.dataflip_processor.arn
  description = "DataFlip Lambda Function ARN"
}

output "lambda_iam_role_arn" {
  value       = aws_iam_role.lambda_role.arn
  description = "IAM Role ARN attached to Lambda function"
}

