terraform {
  required_providers {
    aws     = { source = "hashicorp/aws",       version = "~> 5.0" }
    archive = { source = "hashicorp/archive",   version = "~> 2.0" }
    time    = { source = "hashicorp/time",      version = "~> 0.9" }
  }
}

provider "aws" {
  region = var.aws_region
}

# ─── IAM Execution Role (Lambda / Step Functions / Scheduler) ─────────────────
resource "aws_iam_role" "exec" {
  name = "airoinsights-exec-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = [
        "lambda.amazonaws.com",
        "states.amazonaws.com",
        "scheduler.amazonaws.com"
      ]}
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "exec_inline" {
  name = "airoinsights-exec-inline"
  role = aws_iam_role.exec.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "Logs"
        Effect = "Allow"
        Action = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "arn:aws:logs:*:*:*"
      },
      {
        Sid    = "S3Access"
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:PutObject", "s3:ListBucket",
                  "s3:GetBucketLocation", "s3:DeleteObject"]
        Resource = [
          "arn:aws:s3:::${var.bronze_bucket_name}",
          "arn:aws:s3:::${var.bronze_bucket_name}/*",
          "arn:aws:s3:::${var.silver_bucket_name}",
          "arn:aws:s3:::${var.silver_bucket_name}/*",
          "arn:aws:s3:::airoinsights-athena-results",
          "arn:aws:s3:::airoinsights-athena-results/*",
        ]
      },
      {
        Sid    = "SecretsManager"
        Effect = "Allow"
        Action = ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"]
        Resource = "arn:aws:secretsmanager:${var.aws_region}:*:secret:airoinsights-*"
      },
      {
        Sid    = "Athena"
        Effect = "Allow"
        Action = ["athena:StartQueryExecution", "athena:GetQueryExecution",
                  "athena:GetQueryResults", "athena:StopQueryExecution"]
        Resource = "*"
      },
      {
        Sid    = "Glue"
        Effect = "Allow"
        Action = ["glue:GetDatabase", "glue:GetTable", "glue:GetPartitions",
                  "glue:GetPartition", "glue:BatchGetPartition"]
        Resource = "*"
      },
      {
        Sid    = "StatesInvoke"
        Effect = "Allow"
        Action = ["states:StartExecution", "lambda:InvokeFunction"]
        Resource = "*"
      }
    ]
  })
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
  role             = aws_iam_role.exec.arn
  timeout          = 300
  memory_size      = 256
  layers           = [aws_lambda_layer_version.requests.arn]

  environment {
    variables = {
      BRONZE_BUCKET         = var.bronze_bucket_name
      SILVER_BUCKET         = var.silver_bucket_name
      AIRLABS_API_KEY       = var.airlabs_api_key
    }
  }

  depends_on = [aws_s3_bucket.bronze, aws_s3_bucket.silver]
}

# ─── Step Function ─────────────────────────────────────────────────────────────
resource "aws_sfn_state_machine" "ingestor" {
  name     = "airoinsights-ingestor"
  role_arn = aws_iam_role.exec.arn

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
    role_arn = aws_iam_role.exec.arn
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

# ─── Secrets Manager ─────────────────────────────────

resource "aws_secretsmanager_secret" "airlabs_api_key" {
  name        = "airoinsights-airlabs-api-key"
  description = "AirLabs API Key for AeroInsights project"
}

resource "aws_secretsmanager_secret_version" "airlabs_api_key_version" {
  secret_id     = aws_secretsmanager_secret.airlabs_api_key.id
  secret_string = var.airlabs_api_key
}

# ─── AWS Account identity (used by QuickSight resources) ──────────────────────
data "aws_caller_identity" "current" {}

# ─── S3: Athena Query Results ────────────────────────────────────────────────
resource "aws_s3_bucket" "athena_results" {
  bucket = "airoinsights-athena-results"
  tags   = { Project = "airoinsights", Layer = "athena" }
}

resource "aws_s3_bucket_versioning" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id
  versioning_configuration { status = "Enabled" }
}

# ─── Athena Workgroup ────────────────────────────────────────────────────────
resource "aws_athena_workgroup" "airoinsights" {
  name  = "airoinsights"
  state = "ENABLED"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.bucket}/query-results/"
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }

  tags = { Project = "airoinsights" }
}

# ─── Glue Data Catalog Database ──────────────────────────────────────────────
resource "aws_glue_catalog_database" "airoinsights" {
  name        = "airoinsights"
  description = "AiroInsights bronze layer — flights and weather"
}

# ─── Glue Table: flights (bronze, JSON + partition projection) ────────────────
resource "aws_glue_catalog_table" "flights" {
  name          = "flights"
  database_name = aws_glue_catalog_database.airoinsights.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL                              = "TRUE"
    "classification"                      = "json"
    "projection.enabled"                  = "true"
    "projection.city.type"                = "enum"
    "projection.city.values"              = "zurich,frankfurt,london,paris,unknown"
    "projection.date.type"                = "date"
    "projection.date.range"               = "2026-01-01,NOW"
    "projection.date.format"              = "yyyy-MM-dd"
    "storage.location.template"           = "s3://${var.bronze_bucket_name}/raw/flights/city=$${city}/date=$${date}"
  }

  partition_keys {
    name    = "city"
    type    = "string"
    comment = "City associated with the flights ingestion"
  }
  partition_keys {
    name    = "date"
    type    = "string"
    comment = "Ingestion date YYYY-MM-DD"
  }

  storage_descriptor {
    location          = "s3://${var.bronze_bucket_name}/raw/flights/"
    input_format      = "org.apache.hadoop.mapred.TextInputFormat"
    output_format     = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    compressed        = false
    number_of_buckets = -1

    ser_de_info {
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
      parameters = {
        "ignore.malformed.json" = "TRUE"
        "dots.in.keys"          = "FALSE"
        "case.insensitive"      = "TRUE"
      }
    }

    columns {


      name = "source"


      type = "string"


    }
    columns {

      name = "ingested_at_utc"

      type = "string"

    }
    columns {
      name = "metadata"
      type = "struct<city:string,flight_count:int,source_iata:string,is_mock:boolean>"
    }
    columns {
      name = "payload"
      type = "array<struct<flight_iata:string,dep_iata:string,arr_iata:string,status:string,dep_time:string,arr_time:string,dep_actual:string,arr_actual:string,dep_delayed:int,arr_delayed:int,airline_iata:string,airline_icao:string,aircraft_icao:string,duration:int,delayed:int>>"
    }
  }
}

# ─── Glue Table: weather (bronze, JSON + partition projection) ────────────────
resource "aws_glue_catalog_table" "weather" {
  name          = "weather"
  database_name = aws_glue_catalog_database.airoinsights.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL                              = "TRUE"
    "classification"                      = "json"
    "projection.enabled"                  = "true"
    "projection.city.type"                = "enum"
    "projection.city.values"              = "zurich,frankfurt,london,paris"
    "projection.date.type"                = "date"
    "projection.date.range"               = "2026-01-01,NOW"
    "projection.date.format"              = "yyyy-MM-dd"
    "storage.location.template"           = "s3://${var.bronze_bucket_name}/raw/weather/city=$${city}/date=$${date}"
  }

  partition_keys {
    name    = "city"
    type    = "string"
    comment = "City name"
  }
  partition_keys {
    name    = "date"
    type    = "string"
    comment = "Ingestion date YYYY-MM-DD"
  }

  storage_descriptor {
    location          = "s3://${var.bronze_bucket_name}/raw/weather/"
    input_format      = "org.apache.hadoop.mapred.TextInputFormat"
    output_format     = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    compressed        = false
    number_of_buckets = -1

    ser_de_info {
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
      parameters = {
        "ignore.malformed.json" = "TRUE"
        "dots.in.keys"          = "FALSE"
        "case.insensitive"      = "TRUE"
      }
    }

    columns {


      name = "source"


      type = "string"


    }
    columns {

      name = "ingested_at_utc"

      type = "string"

    }
    columns {
      name = "metadata"
      type = "struct<location:string,lat:double,lon:double,is_mock:boolean>"
    }
    columns {
      name = "payload"
      type = "struct<latitude:double,longitude:double,hourly:struct<time:array<string>,temperature_2m:array<double>,relative_humidity_2m:array<int>,precipitation:array<double>,wind_speed_10m:array<double>>,daily:struct<time:array<string>,weather_code:array<int>,temperature_2m_max:array<double>,temperature_2m_min:array<double>>>"
    }
  }
}

# ─── Athena Named Queries (saved reference queries) ──────────────────────────
resource "aws_athena_named_query" "flights_flattened" {
  name        = "airoinsights-flights-flattened"
  description = "Flattened individual flights from bronze layer"
  workgroup   = aws_athena_workgroup.airoinsights.name
  database    = aws_glue_catalog_database.airoinsights.name

  query = <<-SQL
    SELECT
      city,
      date,
      ingested_at_utc,
      metadata.source_iata         AS source_iata,
      flight.flight_iata           AS flight_iata,
      flight.dep_iata              AS dep_iata,
      flight.arr_iata              AS arr_iata,
      flight.status                AS status,
      flight.airline_iata          AS airline_iata,
      flight.dep_time              AS dep_time,
      flight.arr_time              AS arr_time,
      flight.duration              AS duration,
      flight.delayed               AS delayed
    FROM airoinsights.flights
    CROSS JOIN UNNEST(payload) AS t(flight)
    WHERE city IS NOT NULL
  SQL
}

resource "aws_athena_named_query" "weather_hourly" {
  name        = "airoinsights-weather-hourly"
  description = "Hourly weather observations flattened from bronze layer"
  workgroup   = aws_athena_workgroup.airoinsights.name
  database    = aws_glue_catalog_database.airoinsights.name

  query = <<-SQL
    SELECT
      city,
      date,
      ingested_at_utc,
      t.time_slot       AS observation_time,
      t.temperature_2m  AS temperature_2m,
      t.humidity        AS relative_humidity,
      t.precipitation   AS precipitation,
      t.wind_speed      AS wind_speed_10m
    FROM airoinsights.weather
    CROSS JOIN UNNEST(
      payload.hourly.time,
      payload.hourly.temperature_2m,
      payload.hourly.relative_humidity_2m,
      payload.hourly.precipitation,
      payload.hourly.wind_speed_10m
    ) AS t(time_slot, temperature_2m, humidity, precipitation, wind_speed)
    WHERE city IS NOT NULL
  SQL
}

# ─── S3 Bucket Policies: allow QuickSight service to read from S3 ─────────────
resource "aws_s3_bucket_policy" "bronze_qs" {
  bucket = aws_s3_bucket.bronze.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "AllowQuickSightRead"
      Effect    = "Allow"
      Principal = { Service = "quicksight.amazonaws.com" }
      Action    = ["s3:GetObject", "s3:ListBucket", "s3:GetBucketLocation"]
      Resource  = [aws_s3_bucket.bronze.arn, "${aws_s3_bucket.bronze.arn}/*"]
    }]
  })
}

resource "aws_s3_bucket_policy" "athena_results_qs" {
  bucket = aws_s3_bucket.athena_results.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "AllowQuickSightRead"
      Effect    = "Allow"
      Principal = { Service = "quicksight.amazonaws.com" }
      Action    = ["s3:GetObject", "s3:ListBucket", "s3:GetBucketLocation"]
      Resource  = [aws_s3_bucket.athena_results.arn, "${aws_s3_bucket.athena_results.arn}/*"]
    }]
  })
}

# ─── QuickSight Account Subscription (idempotent — ignored if already active) ─
resource "aws_quicksight_account_subscription" "main" {
  account_name          = "airoinsights"
  authentication_method = "IAM_AND_QUICKSIGHT"
  edition               = "ENTERPRISE"
  notification_email    = var.quicksight_notification_email

  lifecycle {
    ignore_changes = all
  }
}

# ─── IAM Role: QuickSight service role for Athena access ─────────────────────
resource "aws_iam_role" "quicksight_service" {
  name = "aws-quicksight-service-role-v0"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "quicksight.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })

  depends_on = [aws_quicksight_account_subscription.main]
}

resource "aws_iam_role_policy" "quicksight_service_policy" {
  name = "quicksight-athena-s3-glue"
  role = aws_iam_role.quicksight_service.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AthenaAccess"
        Effect = "Allow"
        Action = [
          "athena:BatchGetNamedQuery", "athena:GetNamedQuery",
          "athena:GetQueryExecution", "athena:GetQueryResults",
          "athena:GetQueryResultsStream", "athena:ListNamedQueries",
          "athena:ListQueryExecutions", "athena:StartQueryExecution",
          "athena:StopQueryExecution", "athena:ListWorkGroups",
          "athena:GetWorkGroup", "athena:ListTableMetadata",
          "athena:GetTableMetadata",
        ]
        Resource = "*"
      },
      {
        Sid    = "GlueAccess"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase", "glue:GetDatabases",
          "glue:GetTable", "glue:GetTables",
          "glue:GetPartition", "glue:GetPartitions",
          "glue:BatchGetPartition", "glue:GetCatalogImportStatus",
        ]
        Resource = "*"
      },
      {
        Sid    = "S3Access"
        Effect = "Allow"
        Action = [
          "s3:GetBucketLocation", "s3:GetObject", "s3:ListBucket",
          "s3:ListBucketMultipartUploads", "s3:ListMultipartUploadParts",
          "s3:AbortMultipartUpload", "s3:PutObject", "s3:GetObjectVersion",
        ]
        Resource = [
          aws_s3_bucket.athena_results.arn,
          "${aws_s3_bucket.athena_results.arn}/*",
          aws_s3_bucket.bronze.arn,
          "${aws_s3_bucket.bronze.arn}/*",
        ]
      },
    ]
  })
}

# Wait for IAM propagation before creating QuickSight data source
resource "time_sleep" "wait_iam_propagation" {
  depends_on      = [aws_iam_role_policy.quicksight_service_policy]
  create_duration = "15s"
}

# ─── QuickSight Data Source: Athena ──────────────────────────────────────────
# Requires QuickSight console setup first:
# Manage QuickSight → Security & Permissions → QuickSight access to AWS services
# → Enable Athena → select airoinsights-athena-results + airoinsights-bronze-588863
# Then set enable_quicksight = true in terraform.tfvars and re-apply.
resource "aws_quicksight_data_source" "athena" {
  count          = var.enable_quicksight ? 1 : 0
  data_source_id = "airoinsights-athena"
  name           = "AiroInsights Athena"
  aws_account_id = data.aws_caller_identity.current.account_id
  type           = "ATHENA"

  parameters {
    athena {
      work_group = aws_athena_workgroup.airoinsights.name
    }
  }

  permission {
    actions   = [
      "quicksight:DescribeDataSource",
      "quicksight:DescribeDataSourcePermissions",
      "quicksight:PassDataSource",
      "quicksight:UpdateDataSource",
      "quicksight:DeleteDataSource",
      "quicksight:UpdateDataSourcePermissions",
    ]
    principal = "arn:aws:quicksight:${var.aws_region}:${data.aws_caller_identity.current.account_id}:user/default/${var.quicksight_username}"
  }

  depends_on = [
    aws_athena_workgroup.airoinsights,
    aws_quicksight_account_subscription.main,
    aws_iam_role_policy.quicksight_service_policy,
    time_sleep.wait_iam_propagation,
  ]
}

# ─── QuickSight Dataset: Flights (flattened via Athena SQL) ──────────────────
resource "aws_quicksight_data_set" "flights" {
  count          = var.enable_quicksight ? 1 : 0
  data_set_id    = "airoinsights-flights"
  name           = "AiroInsights - Flights"
  aws_account_id = data.aws_caller_identity.current.account_id
  import_mode    = "DIRECT_QUERY"

  physical_table_map {
    physical_table_map_id = "flights-sql"
    custom_sql {
      data_source_arn = aws_quicksight_data_source.athena[0].arn
      name            = "flights_flattened"
      sql_query       = <<-SQL
        SELECT
          city,
          date,
          ingested_at_utc,
          metadata.source_iata   AS source_iata,
          flight.flight_iata     AS flight_iata,
          flight.dep_iata        AS dep_iata,
          flight.arr_iata        AS arr_iata,
          flight.status          AS status,
          flight.airline_iata    AS airline_iata,
          flight.dep_time        AS dep_time,
          flight.arr_time        AS arr_time,
          flight.duration        AS duration,
          flight.delayed         AS delayed
        FROM airoinsights.flights
        CROSS JOIN UNNEST(payload) AS t(flight)
        WHERE city IS NOT NULL
      SQL
      columns {

        name = "city"

        type = "STRING"

      }
      columns {

        name = "date"

        type = "STRING"

      }
      columns {

        name = "ingested_at_utc"

        type = "STRING"

      }
      columns {

        name = "source_iata"

        type = "STRING"

      }
      columns {

        name = "flight_iata"

        type = "STRING"

      }
      columns {

        name = "dep_iata"

        type = "STRING"

      }
      columns {

        name = "arr_iata"

        type = "STRING"

      }
      columns {

        name = "status"

        type = "STRING"

      }
      columns {

        name = "airline_iata"

        type = "STRING"

      }
      columns {

        name = "dep_time"

        type = "STRING"

      }
      columns {

        name = "arr_time"

        type = "STRING"

      }
      columns {

        name = "duration"

        type = "INTEGER"

      }
      columns {

        name = "delayed"

        type = "INTEGER"

      }
    }
  }

  logical_table_map {
    logical_table_map_id = "flights-logical"
    alias                = "flights_flattened"
    source { physical_table_id = "flights-sql" }
  }

  permissions {
    actions = [
      "quicksight:DescribeDataSet",
      "quicksight:DescribeDataSetPermissions",
      "quicksight:PassDataSet",
      "quicksight:DescribeIngestion",
      "quicksight:ListIngestions",
      "quicksight:UpdateDataSet",
      "quicksight:DeleteDataSet",
      "quicksight:CreateIngestion",
      "quicksight:CancelIngestion",
      "quicksight:UpdateDataSetPermissions",
    ]
    principal = "arn:aws:quicksight:${var.aws_region}:${data.aws_caller_identity.current.account_id}:user/default/${var.quicksight_username}"
  }

  depends_on = [aws_quicksight_data_source.athena[0]]
}

# ─── QuickSight Dataset: Weather (hourly, unnested via Athena SQL) ────────────
resource "aws_quicksight_data_set" "weather" {
  count          = var.enable_quicksight ? 1 : 0
  data_set_id    = "airoinsights-weather"
  name           = "AiroInsights - Weather"
  aws_account_id = data.aws_caller_identity.current.account_id
  import_mode    = "DIRECT_QUERY"

  physical_table_map {
    physical_table_map_id = "weather-sql"
    custom_sql {
      data_source_arn = aws_quicksight_data_source.athena[0].arn
      name            = "weather_hourly"
      sql_query       = <<-SQL
        SELECT
          city,
          date,
          ingested_at_utc,
          t.time_slot      AS observation_time,
          t.temperature_2m AS temperature_2m,
          t.humidity       AS relative_humidity,
          t.precipitation  AS precipitation,
          t.wind_speed     AS wind_speed_10m
        FROM airoinsights.weather
        CROSS JOIN UNNEST(
          payload.hourly.time,
          payload.hourly.temperature_2m,
          payload.hourly.relative_humidity_2m,
          payload.hourly.precipitation,
          payload.hourly.wind_speed_10m
        ) AS t(time_slot, temperature_2m, humidity, precipitation, wind_speed)
        WHERE city IS NOT NULL
      SQL
      columns {

        name = "city"

        type = "STRING"

      }
      columns {

        name = "date"

        type = "STRING"

      }
      columns {

        name = "ingested_at_utc"

        type = "STRING"

      }
      columns {

        name = "observation_time"

        type = "STRING"

      }
      columns {

        name = "temperature_2m"

        type = "DECIMAL"

      }
      columns {

        name = "relative_humidity"

        type = "INTEGER"

      }
      columns {

        name = "precipitation"

        type = "DECIMAL"

      }
      columns {

        name = "wind_speed_10m"

        type = "DECIMAL"

      }
    }
  }

  logical_table_map {
    logical_table_map_id = "weather-logical"
    alias                = "weather_hourly"
    source { physical_table_id = "weather-sql" }
  }

  permissions {
    actions = [
      "quicksight:DescribeDataSet",
      "quicksight:DescribeDataSetPermissions",
      "quicksight:PassDataSet",
      "quicksight:DescribeIngestion",
      "quicksight:ListIngestions",
      "quicksight:UpdateDataSet",
      "quicksight:DeleteDataSet",
      "quicksight:CreateIngestion",
      "quicksight:CancelIngestion",
      "quicksight:UpdateDataSetPermissions",
    ]
    principal = "arn:aws:quicksight:${var.aws_region}:${data.aws_caller_identity.current.account_id}:user/default/${var.quicksight_username}"
  }

  depends_on = [aws_quicksight_data_source.athena[0]]
}

# ─── QuickSight Dashboard ─────────────────────────────────────────────────────
resource "aws_quicksight_dashboard" "airoinsights" {
  count               = var.enable_quicksight ? 1 : 0
  dashboard_id        = "airoinsights-main"
  name                = "AiroInsights Dashboard"
  aws_account_id      = data.aws_caller_identity.current.account_id
  version_description = "1"

  definition {
    data_set_identifiers_declarations {
      data_set_arn = aws_quicksight_data_set.flights[0].arn
      identifier   = "flights"
    }
    data_set_identifiers_declarations {
      data_set_arn = aws_quicksight_data_set.weather[0].arn
      identifier   = "weather"
    }

    # ── Sheet 1: Flights Overview ───────────────────────────────────────────
    sheets {
      sheet_id = "flights-overview"
      name     = "Flights Overview"

      visuals {
        bar_chart_visual {
          visual_id = "flights-by-city"
          title {
            visibility = "VISIBLE"
            format_text { plain_text = "Flights by City" }
          }
          chart_configuration {
            field_wells {
              bar_chart_aggregated_field_wells {
                category {
                  categorical_dimension_field {
                    field_id = "city-dim-1"
                    column {
                      data_set_identifier = "flights"
                      column_name         = "city"
                    }
                  }
                }
                values {
                  categorical_measure_field {
                    field_id             = "flight-count-1"
                    aggregation_function = "COUNT"
                    column {
                      data_set_identifier = "flights"
                      column_name         = "flight_iata"
                    }
                  }
                }
              }
            }
            orientation = "HORIZONTAL"
          }
        }
      }

      visuals {
        pie_chart_visual {
          visual_id = "flight-status-pie"
          title {
            visibility = "VISIBLE"
            format_text { plain_text = "Flight Status Distribution" }
          }
          chart_configuration {
            field_wells {
              pie_chart_aggregated_field_wells {
                category {
                  categorical_dimension_field {
                    field_id = "status-dim-1"
                    column {
                      data_set_identifier = "flights"
                      column_name         = "status"
                    }
                  }
                }
                values {
                  categorical_measure_field {
                    field_id             = "status-count-1"
                    aggregation_function = "COUNT"
                    column {
                      data_set_identifier = "flights"
                      column_name         = "flight_iata"
                    }
                  }
                }
              }
            }
          }
        }
      }
    }

    # ── Sheet 2: Weather Overview ───────────────────────────────────────────
    sheets {
      sheet_id = "weather-overview"
      name     = "Weather Overview"

      visuals {
        line_chart_visual {
          visual_id = "temp-by-city"
          title {
            visibility = "VISIBLE"
            format_text { plain_text = "Temperature by City (°C)" }
          }
          chart_configuration {
            field_wells {
              line_chart_aggregated_field_wells {
                category {
                  categorical_dimension_field {
                    field_id = "obs-time-dim-1"
                    column {
                      data_set_identifier = "weather"
                      column_name         = "observation_time"
                    }
                  }
                }
                values {
                  numerical_measure_field {
                    field_id = "temp-measure-1"
                    column {
                      data_set_identifier = "weather"
                      column_name         = "temperature_2m"
                    }
                    aggregation_function {
                      simple_numerical_aggregation = "AVERAGE"
                    }
                  }
                }
                colors {
                  categorical_dimension_field {
                    field_id = "city-color-1"
                    column {
                      data_set_identifier = "weather"
                      column_name         = "city"
                    }
                  }
                }
              }
            }
          }
        }
      }

      visuals {
        bar_chart_visual {
          visual_id = "precip-by-city"
          title {
            visibility = "VISIBLE"
            format_text { plain_text = "Average Precipitation by City (mm)" }
          }
          chart_configuration {
            field_wells {
              bar_chart_aggregated_field_wells {
                category {
                  categorical_dimension_field {
                    field_id = "city-precip-1"
                    column {
                      data_set_identifier = "weather"
                      column_name         = "city"
                    }
                  }
                }
                values {
                  numerical_measure_field {
                    field_id = "precip-measure-1"
                    column {
                      data_set_identifier = "weather"
                      column_name         = "precipitation"
                    }
                    aggregation_function {
                      simple_numerical_aggregation = "AVERAGE"
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  permissions {
    actions = [
      "quicksight:DescribeDashboard",
      "quicksight:ListDashboardVersions",
      "quicksight:UpdateDashboardPermissions",
      "quicksight:QueryDashboard",
      "quicksight:UpdateDashboard",
      "quicksight:DeleteDashboard",
      "quicksight:DescribeDashboardPermissions",
      "quicksight:UpdateDashboardPublishedVersion",
    ]
    principal = "arn:aws:quicksight:${var.aws_region}:${data.aws_caller_identity.current.account_id}:user/default/${var.quicksight_username}"
  }

  depends_on = [
    aws_quicksight_data_set.flights[0],
    aws_quicksight_data_set.weather[0],
  ]
}



