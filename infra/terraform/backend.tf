terraform {
  required_version = ">= 1.11"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }

  backend "s3" {
    bucket       = "ao-eng"
    key          = "terraform-state/movielens/terraform.tfstate"
    region       = "us-east-1"
    profile      = "terraform"
    encrypt      = true
    use_lockfile = true
  }
}
