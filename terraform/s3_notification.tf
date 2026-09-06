# =====================================================================
# S3 Event Notification & Lambda Invocation Permission
# =====================================================================

# 1. Least-privilege permission granting S3 permission to invoke the Lambda function
resource "aws_lambda_permission" "allow_s3_invocation" {
  statement_id  = "AllowExecutionFromS3Bucket"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.dataflip_processor.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.analytics.arn
}

# 2. S3 Bucket Notification targeting candidate GREEN Parquet uploads
resource "aws_s3_bucket_notification" "green_candidate_notification" {
  bucket = aws_s3_bucket.analytics.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.dataflip_processor.arn
    events              = ["s3:ObjectCreated:*"]
    filter_suffix       = ".parquet"
  }

  depends_on = [aws_lambda_permission.allow_s3_invocation]
}
