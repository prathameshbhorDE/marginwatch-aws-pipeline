terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  region  = var.region
  profile = var.aws_profile

  # Every resource Terraform creates gets these labels automatically,
  # so MarginWatch resources are easy to find (and to tell apart from older projects).
  default_tags {
    tags = {
      Project   = "marginwatch"
      Env       = var.env
      ManagedBy = "terraform"
    }
  }
}
