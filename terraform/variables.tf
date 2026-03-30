variable "aws_region" {
  default = "us-east-1"
}

variable "bronze_bucket_name" {
  default = "airoinsights-bronze"
}

variable "silver_bucket_name" {
  default = "airoinsights-silver"
}

variable "aviationstack_api_key" {
  description = "AviationStack API key"
  sensitive   = true
}

variable "schedule_expression" {
  description = "EventBridge cron for Step Function trigger"
  default     = "rate(6 hours)"
}


variable "lab_role_arn" {
  description = "Pre-existing LabRole ARN from AWS Academy"
  default     = "arn:aws:iam::058264229109:role/LabRole"
}