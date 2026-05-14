# =============================================================================
# Silver Layer — Lambda Transformer + S3 Trigger
# =============================================================================
# Adds to the existing AeroInsights infrastructure (main.tf):
#   1. Lambda package for the silver transformer
#   2. Lambda function (reuses existing exec IAM role)
#   3. S3 event notification on the bronze bucket → triggers the Lambda
#
# Prerequisites from main.tf:
#   - aws_s3_bucket.bronze    (bronze bucket)
#   - aws_s3_bucket.silver    (silver bucket — already created)
#   - aws_iam_role.exec       (shared execution role with S3 read/write)
#   - aws_lambda_layer_version.requests  (requests layer, not needed here
#                                          but available if we add API calls)
# =============================================================================

# ─── Lambda Package ─────────────────────────────────────────────────────────
data "archive_file" "silver_lambda_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../lambda_silver"
  output_path = "${path.module}/../lambda_silver.zip"
}

# ─── Lambda Function ─────────────────────────────────────────────────────────
resource "aws_lambda_function" "silver_transformer" {
  function_name    = "airoinsights-silver-transformer"
  filename         = data.archive_file.silver_lambda_zip.output_path
  source_code_hash = data.archive_file.silver_lambda_zip.output_base64sha256
  handler          = "handler.lambda_handler"
  runtime          = "python3.12"
  role             = aws_iam_role.exec.arn     # reuse existing role from main.tf
  timeout          = 120
  memory_size      = 256

  environment {
    variables = {
      SILVER_BUCKET = var.silver_bucket_name
      BRONZE_BUCKET = var.bronze_bucket_name
    }
  }

  tags = {
    Project = "airoinsights"
    Layer   = "silver"
  }

  depends_on = [aws_s3_bucket.bronze, aws_s3_bucket.silver]
}

# ─── S3 Event Notification → Lambda Trigger ──────────────────────────────────
# Fires the silver transformer whenever a new .json file is created under raw/
resource "aws_lambda_permission" "allow_bronze_s3" {
  statement_id  = "AllowBronzeS3Invoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.silver_transformer.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.bronze.arn
}

resource "aws_s3_bucket_notification" "bronze_to_silver" {
  bucket = aws_s3_bucket.bronze.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.silver_transformer.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "raw/"
    filter_suffix       = ".json"
  }

  depends_on = [aws_lambda_permission.allow_bronze_s3]
}

# ─── Outputs ─────────────────────────────────────────────────────────────────
output "silver_transformer_arn" {
  value = aws_lambda_function.silver_transformer.arn
}

output "silver_transformer_name" {
  value = aws_lambda_function.silver_transformer.function_name
}

# ─── Glue Table: silver flights (flat JSON Lines, no UNNEST needed) ──────────
resource "aws_glue_catalog_table" "silver_flights" {
  name          = "silver_flights"
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
    "storage.location.template"           = "s3://${var.silver_bucket_name}/silver/flights/city=$${city}/date=$${date}"
  }

  partition_keys {
    name = "city"
    type = "string"
  }
  partition_keys {
    name = "date"
    type = "string"
  }

  storage_descriptor {
    location          = "s3://${var.silver_bucket_name}/silver/flights/"
    input_format      = "org.apache.hadoop.mapred.TextInputFormat"
    output_format     = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    compressed        = false
    number_of_buckets = -1

    ser_de_info {
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
      parameters = {
        "ignore.malformed.json" = "TRUE"
        "case.insensitive"      = "TRUE"
      }
    }

    columns { name = "flight_iata"          type = "string"  }
    columns { name = "departure_iata"       type = "string"  }
    columns { name = "arrival_iata"         type = "string"  }
    columns { name = "airline_iata"         type = "string"  }
    columns { name = "airline_icao"         type = "string"  }
    columns { name = "aircraft_type"        type = "string"  }
    columns { name = "flight_status"        type = "string"  }
    columns { name = "departure_time"       type = "string"  }
    columns { name = "arrival_time"         type = "string"  }
    columns { name = "departure_actual"     type = "string"  }
    columns { name = "arrival_actual"       type = "string"  }
    columns { name = "departure_delay_min"  type = "int"     }
    columns { name = "arrival_delay_min"    type = "int"     }
    columns { name = "duration_minutes"     type = "int"     }
    columns { name = "delayed_flag"         type = "int"     }
    columns { name = "source_iata"          type = "string"  }
    columns { name = "is_mock"              type = "boolean" }
    columns { name = "ingested_at_utc"      type = "string"  }
  }
}

# ─── Glue Table: silver weather (flat JSON Lines) ───────────────────────────
# Contains both hourly and daily records — filter with:
#   WHERE record_type = 'hourly'  or  WHERE record_type = 'daily'
resource "aws_glue_catalog_table" "silver_weather" {
  name          = "silver_weather"
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
    "storage.location.template"           = "s3://${var.silver_bucket_name}/silver/weather/city=$${city}/date=$${date}"
  }

  partition_keys {
    name = "city"
    type = "string"
  }
  partition_keys {
    name = "date"
    type = "string"
  }

  storage_descriptor {
    location          = "s3://${var.silver_bucket_name}/silver/weather/"
    input_format      = "org.apache.hadoop.mapred.TextInputFormat"
    output_format     = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    compressed        = false
    number_of_buckets = -1

    ser_de_info {
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
      parameters = {
        "ignore.malformed.json" = "TRUE"
        "case.insensitive"      = "TRUE"
      }
    }

    # Shared columns
    columns { name = "record_type"          type = "string"  }
    columns { name = "latitude"             type = "double"  }
    columns { name = "longitude"            type = "double"  }
    columns { name = "is_mock"              type = "boolean" }
    columns { name = "ingested_at_utc"      type = "string"  }

    # Hourly columns (null for daily records)
    columns { name = "observation_time"     type = "string"  }
    columns { name = "temperature_c"        type = "double"  }
    columns { name = "humidity_pct"         type = "double"  }
    columns { name = "precipitation_mm"     type = "double"  }
    columns { name = "wind_speed_kmh"       type = "double"  }

    # Daily columns (null for hourly records)
    columns { name = "forecast_date"        type = "string"  }
    columns { name = "weather_code"         type = "int"     }
    columns { name = "weather_description"  type = "string"  }
    columns { name = "temperature_max_c"    type = "double"  }
    columns { name = "temperature_min_c"    type = "double"  }
  }
}

# ─── Athena: sample silver queries ──────────────────────────────────────────
resource "aws_athena_named_query" "silver_flights_query" {
  name        = "airoinsights-silver-flights"
  description = "Query silver flights — already flat, no UNNEST needed"
  workgroup   = aws_athena_workgroup.airoinsights.name
  database    = aws_glue_catalog_database.airoinsights.name

  query = <<-SQL
    SELECT
      city,
      date,
      flight_iata,
      departure_iata,
      arrival_iata,
      flight_status,
      airline_iata,
      departure_time,
      arrival_time,
      duration_minutes,
      departure_delay_min
    FROM airoinsights.silver_flights
    WHERE city IS NOT NULL
    ORDER BY departure_time
  SQL
}

resource "aws_athena_named_query" "silver_weather_hourly_query" {
  name        = "airoinsights-silver-weather-hourly"
  description = "Query silver hourly weather — already flat, no UNNEST needed"
  workgroup   = aws_athena_workgroup.airoinsights.name
  database    = aws_glue_catalog_database.airoinsights.name

  query = <<-SQL
    SELECT
      city,
      date,
      observation_time,
      temperature_c,
      humidity_pct,
      precipitation_mm,
      wind_speed_kmh
    FROM airoinsights.silver_weather
    WHERE record_type = 'hourly'
      AND city IS NOT NULL
    ORDER BY observation_time
  SQL
}
