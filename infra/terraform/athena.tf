resource "aws_athena_workgroup" "movielens" {
  name          = "movielens"
  force_destroy = true

  configuration {
    # Users and clients cannot override the settings below
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    # Cost guardrail: any query scanning more than this is cancelled
    bytes_scanned_cutoff_per_query = var.athena_bytes_scanned_cutoff

    result_configuration {
      output_location = "s3://${var.bucket_name}/movielens/athena-results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}
