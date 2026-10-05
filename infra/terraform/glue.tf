# The Glue database is managed here. Its tables are managed by dbt, never by Terraform.
resource "aws_glue_catalog_database" "movielens" {
  name         = "movielens"
  location_uri = "s3://${var.bucket_name}/movielens/warehouse/"

  create_table_default_permission {
    permissions = ["ALL"]

    principal {
      data_lake_principal_identifier = "IAM_ALLOWED_PRINCIPALS"
    }
  }
}
