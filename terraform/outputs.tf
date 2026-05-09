output "bronze_bucket"     { value = aws_s3_bucket.bronze.bucket }
output "silver_bucket"     { value = aws_s3_bucket.silver.bucket }
output "lambda_arn"        { value = aws_lambda_function.ingestor.arn }
output "step_function_arn" { value = aws_sfn_state_machine.ingestor.arn }

output "athena_results_bucket" { value = aws_s3_bucket.athena_results.bucket }
output "athena_workgroup"      { value = aws_athena_workgroup.airoinsights.name }
output "glue_database"         { value = aws_glue_catalog_database.airoinsights.name }

output "quicksight_dashboard_url" {
  value = var.enable_quicksight ? "https://${var.aws_region}.quicksight.aws.amazon.com/sn/dashboards/${aws_quicksight_dashboard.airoinsights[0].dashboard_id}" : "QuickSight not enabled yet. Complete Security & Permissions in the QuickSight console, set enable_quicksight=true, and re-apply."
}
