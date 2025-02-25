terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  backend "s3" {
    bucket = "project-merrow-backend-bucket"
    key    = "tf-state/totes-backend.tfstate"
    region = "eu-west-2"
  }
}

## call account id 
data "aws_caller_identity" "current" {}

data "aws_region" "current" {}



