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

