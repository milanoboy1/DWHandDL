output "bronze_bucket" { value = aws_s3_bucket.bronze.bucket }
output "silver_bucket" { value = aws_s3_bucket.silver.bucket }
output "lambda_arn"    { value = aws_lambda_function.ingestor.arn }
output "step_function_arn" { value = aws_sfn_state_machine.ingestor.arn }
