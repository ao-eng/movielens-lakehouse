provider "aws" {
  region  = var.region
  profile = "terraform"

  default_tags {
    tags = {
      project    = "movielens"
      managed_by = "terraform"
    }
  }
}

data "aws_caller_identity" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
}
