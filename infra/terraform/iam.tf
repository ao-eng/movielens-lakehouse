# Pipeline user: Airflow ingestion + dbt (read/write on the lakehouse)
resource "aws_iam_user" "pipeline" {
  name = "engineer"
}

resource "aws_iam_policy" "pipeline" {
  name        = "movielens-dbt"
  description = "Airflow ingestion and dbt: read/write on movielens/ and the Glue database"
  policy = templatefile("${path.module}/policies/movielens-dbt.json.tftpl", {
    account_id = local.account_id
    region     = var.region
    bucket     = var.bucket_name
  })
}

resource "aws_iam_user_policy_attachment" "pipeline" {
  user       = aws_iam_user.pipeline.name
  policy_arn = aws_iam_policy.pipeline.arn
}

# BI user: Superset through Athena (read-only on the warehouse)
resource "aws_iam_user" "superset" {
  name = "movielens-superset"
}

resource "aws_iam_policy" "superset" {
  name        = "movielens-superset-read"
  description = "Superset: read-only on the warehouse, queries through the movielens Athena workgroup"
  policy = templatefile("${path.module}/policies/movielens-superset-read.json.tftpl", {
    account_id = local.account_id
    region     = var.region
    bucket     = var.bucket_name
    workgroup  = aws_athena_workgroup.movielens.name
  })
}

resource "aws_iam_user_policy_attachment" "superset" {
  user       = aws_iam_user.superset.name
  policy_arn = aws_iam_policy.superset.arn
}
