data "archive_file" "lambda_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../lambda"
  output_path = "${path.module}/lambda_payload.zip"
}

resource "aws_lambda_function" "dataflip_processor" {
  filename         = data.archive_file.lambda_zip.output_path
  function_name    = "${var.project_name}-processor"
  role             = aws_iam_role.lambda_role.arn
  handler          = "handler.lambda_handler"
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256
  runtime          = "python3.11"
  timeout          = 30

  environment {
    variables = {
      S3_BUCKET     = aws_s3_bucket.analytics.id
      GLUE_DATABASE = aws_glue_catalog_database.dataflip_db.name
      GLUE_TABLE    = aws_glue_catalog_table.sales_curated.name
    }
  }

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}
