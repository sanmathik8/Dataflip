variable "aws_region" {
  type        = string
  default     = "us-east-1"
  description = "AWS Region for DataFlip deployment"
}

variable "project_name" {
  type        = string
  default     = "dataflip"
  description = "Project name prefix"
}

variable "environment" {
  type        = string
  default     = "dev"
  description = "Environment deployment tier (dev, staging, prod)"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "The environment variable must be one of: dev, staging, prod."
  }
}

variable "lambda_layer_arns" {
  type        = list(string)
  default     = []
  description = "Optional list of Lambda Layer ARNs (e.g. AWS SDK for pandas/PyArrow and AWS Lambda Powertools)"
}


