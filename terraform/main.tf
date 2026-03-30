terraform {
  required_providers {
    aws     = { source = "hashicorp/aws",       version = "~> 5.0" }
    archive = { source = "hashicorp/archive",   version = "~> 2.0" }
  }
}

provider "aws" {
  region = var.aws_region
}

# ─── S3 Buckets ────────────────────────────────────────────────────────────────
resource "aws_s3_bucket" "bronze" {
  bucket = var.bronze_bucket_name
  tags   = { Project = "airoinsights", Layer = "bronze" }
}

resource "aws_s3_bucket_versioning" "bronze" {
  bucket = aws_s3_bucket.bronze.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket" "silver" {
  bucket = var.silver_bucket_name
  tags   = { Project = "airoinsights", Layer = "silver" }
}

resource "aws_s3_bucket_versioning" "silver" {
  bucket = aws_s3_bucket.silver.id
  versioning_configuration { status = "Enabled" }
}

# ─── Lambda Layer (requests) ───────────────────────────────────────────────────
resource "aws_lambda_layer_version" "requests" {
  filename            = "${path.module}/../requests_layer.zip"
  layer_name          = "airoinsights-requests"
  compatible_runtimes = ["python3.12"]
  source_code_hash    = filebase64sha256("${path.module}/../requests_layer.zip")
}

# ─── Lambda Package ────────────────────────────────────────────────────────────
data "archive_file" "lambda_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../lambda"
  output_path = "${path.module}/../lambda.zip"
}

# ─── Lambda Function ───────────────────────────────────────────────────────────
resource "aws_lambda_function" "ingestor" {
  function_name    = "airoinsights-ingestor"
  filename         = data.archive_file.lambda_zip.output_path
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256
  handler          = "handler.lambda_handler"
  runtime          = "python3.12"
  role             = var.lab_role_arn        # ← LabRole, no IAM creation needed
  timeout          = 300
  memory_size      = 256
  layers           = [aws_lambda_layer_version.requests.arn]

  environment {
    variables = {
      BRONZE_BUCKET         = var.bronze_bucket_name
      SILVER_BUCKET         = var.silver_bucket_name
      AVIATIONSTACK_API_KEY = var.aviationstack_api_key
    }
  }

  depends_on = [aws_s3_bucket.bronze, aws_s3_bucket.silver]
}

# ─── Step Function ─────────────────────────────────────────────────────────────
resource "aws_sfn_state_machine" "ingestor" {
  name     = "airoinsights-ingestor"
  role_arn = var.lab_role_arn               # ← LabRole

  definition = templatefile(
    "${path.module}/../step_function/definition.json",
    { lambda_arn = aws_lambda_function.ingestor.arn }
  )
}

# ─── EventBridge Scheduler ─────────────────────────────────────────────────────

resource "aws_scheduler_schedule" "ingestor" {
  name = "airoinsights-schedule"

  flexible_time_window { mode = "OFF" }

  schedule_expression = var.schedule_expression

  target {
    arn      = aws_sfn_state_machine.ingestor.arn
    role_arn = var.lab_role_arn
  }
}

#resource "aws_cloudwatch_event_rule" "schedule" {
#  name                = "airoinsights-schedule"
#  schedule_expression = var.schedule_expression
#}

#resource "aws_cloudwatch_event_target" "sfn_trigger" {
#  rule     = aws_cloudwatch_event_rule.schedule.name
#  arn      = aws_sfn_state_machine.ingestor.arn
#  role_arn = var.lab_role_arn               # ← LabRole
#}


# ─── Secrets Manager ─────────────────────────────────────────────────────

resource "aws_secretsmanager_secret" "aviationstack_api_key" {
  name        = "airoinsights-aviationstack-api-key"
  description = "Aviationstack API Key for AeroInsights project"
}

resource "aws_secretsmanager_secret_version" "aviationstack_api_key" {
  secret_id     = aws_secretsmanager_secret.aviationstack_api_key.id
  secret_string = var.aviationstack_api_key
}
