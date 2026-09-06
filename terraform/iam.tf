resource "aws_iam_role" "lambda_role" {
  name = "${var.project_name}-lambda-execution-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_policy" "lambda_policy" {
  name        = "${var.project_name}-lambda-policy"
  description = "Least privilege access policy for DataFlip Lambda processor"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3Access"
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]
        Resource = [
          aws_s3_bucket.analytics.arn,
          "${aws_s3_bucket.analytics.arn}/*"
        ]
      },
      {
        Sid    = "GlueAccess"
        Effect = "Allow"
        Action = ["glue:GetTable", "glue:UpdateTable", "glue:CreateTable", "glue:GetDatabase"]
        Resource = [
          "arn:aws:glue:*:*:catalog",
          aws_glue_catalog_database.dataflip_db.arn,
          "arn:aws:glue:*:*:table/${aws_glue_catalog_database.dataflip_db.name}/*"
        ]
      },

      {
        Sid      = "CloudWatchLogs"
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = [
          "arn:aws:logs:*:*:log-group:/aws/lambda/${var.project_name}-processor",
          "arn:aws:logs:*:*:log-group:/aws/lambda/${var.project_name}-processor:*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_policy_attach" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.lambda_policy.arn
}
