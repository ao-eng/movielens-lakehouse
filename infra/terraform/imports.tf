# Resources that already existed before Terraform.
# After the first successful apply these blocks are no-ops and can be kept as a record.

import {
  to = aws_glue_catalog_database.movielens
  id = "908972957987:movielens"
}

import {
  to = aws_s3_bucket.lakehouse
  id = "ao-eng"
}

import {
  to = aws_iam_user.pipeline
  id = "engineer"
}

import {
  to = aws_iam_user.superset
  id = "movielens-superset"
}
