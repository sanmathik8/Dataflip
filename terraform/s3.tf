resource "aws_s3_bucket" "analytics" {
  bucket = "${var.project_name}-analytics-${var.environment}"

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

resource "aws_s3_object" "raw_prefix" {
  bucket = aws_s3_bucket.analytics.id
  key    = "raw/"
}

resource "aws_s3_object" "curated_blue_prefix" {
  bucket = aws_s3_bucket.analytics.id
  key    = "curated/blue/"
}

resource "aws_s3_object" "curated_green_prefix" {
  bucket = aws_s3_bucket.analytics.id
  key    = "curated/green/"
}
