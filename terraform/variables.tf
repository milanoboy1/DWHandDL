variable "aws_region" {
  default = "us-east-1"
}

variable "bronze_bucket_name" {
  default = "airoinsights-bronze-588863"
}

variable "silver_bucket_name" {
  default = "airoinsights-silver-588863"
}

variable "airlabs_api_key" {
  description = "AirLabs API key"
  sensitive   = true
}

variable "schedule_expression" {
  description = "EventBridge cron for Step Function trigger"
  default     = "rate(6 hours)"
}


variable "lab_role_arn" {
  description = "Pre-existing LabRole ARN from AWS Academy"
  default     = "arn:aws:iam::843336588863:role/LabRole"
}

variable "quicksight_username" {
  description = "QuickSight user name (find it at https://quicksight.aws.amazon.com/sn/admin under 'Manage users')"
  default     = "Admin"
}

variable "quicksight_notification_email" {
  description = "Email address for QuickSight account notifications"
  default     = "admin@example.com"
}

variable "enable_quicksight" {
  description = "Set to true after completing QuickSight Security & Permissions console setup"
  default     = false
  type        = bool
}