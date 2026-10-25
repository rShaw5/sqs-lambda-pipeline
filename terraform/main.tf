# Providers and settings. The login comes from the environment (AWS_PROFILE=learning); no keys live here.
# The Phase 6 check bucket was applied and destroyed on 2026-10-19; the pipeline is in pipeline.tf.

terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }

    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.0"
    }
  }
}

provider "aws" {
  region = "us-west-2"

  default_tags {
    tags = {
      project = "sqs-lambda-pipeline"
      managed = "terraform"
    }
  }
}
