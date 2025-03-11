#################### Providers and Backend ####################

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

provider "aws" {
  region = "eu-west-2"

  default_tags {
    tags = {
      ProjectName  = "TotesAmazing"
      DeployedFrom = "Terraform"
      Repository   = "TotesFullOfQueries"
    }
  }
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

#################### IAM Roles and Policies ####################

locals {
  lambda_services = ["lambda.amazonaws.com"]
}

resource "aws_iam_role" "lambda_iam_role" {
  for_each = toset(["extract", "transform", "load"])

  name = "${each.key}_lambda_iam"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect = "Allow",
      Principal = { Service = local.lambda_services },
      Action = "sts:AssumeRole"
    }]
  })

  force_detach_policies = true
}

#################### IAM Policies Attachments ####################

resource "aws_iam_policy" "s3_policy" {
  for_each = { extract = aws_s3_bucket.extract_bucket.arn, transform = aws_s3_bucket.transform_bucket.arn }

  name = "${each.key}_s3_policy"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      { Effect = "Allow", actions = ["s3:ListBucket"], resources = [each.value] },
      { Effect = "Allow", actions = ["s3:PutObject", "s3:GetObject"], resources = ["${each.value}/*"] }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_s3_policy_attachment" {
  for_each = aws_iam_policy.s3_policy

  role       = aws_iam_role.lambda_iam_role[each.key].name
  policy_arn = each.value.arn
}

#################### Lambda Functions ####################

resource "aws_lambda_function" "lambda" {
  for_each = toset(["extract", "transform", "load"])

  function_name    = var."${each.key}_lambda"
  filename         = data.archive_file.lambda[each.key].output_path
  source_code_hash = data.archive_file.lambda[each.key].output_base64sha256
  role             = aws_iam_role.lambda_iam_role[each.key].arn
  handler          = "${var."${each.key}_lambda"}.lambda_handler"
  runtime          = "python3.12"
  timeout          = var.default_timeout
  memory_size      = 512
  layers = compact([
    aws_lambda_layer_version.dependencieslayer.arn,
    aws_lambda_layer_version.utilslayerversion.arn,
    each.key != "extract" ? "arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python312:16" : null
  ])
}

#################### State Machine ####################

resource "aws_sfn_state_machine" "totes_step_function" {
  name     = "totes_step_function"
  role_arn = aws_iam_role.step_function_role.arn

  definition = jsonencode({
    Comment = "Execute lambdas in sequence",
    StartAt = "extract_lambda",
    States = {
      extract_lambda = { Type = "Task", Resource = aws_lambda_function.lambda["extract"].arn, Next = "transform_lambda" },
      transform_lambda = { Type = "Task", Resource = aws_lambda_function.lambda["transform"].arn, Next = "load_lambda" },
      load_lambda = { Type = "Task", Resource = aws_lambda_function.lambda["load"].arn, End = true }
    }
  })

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.step_function_logs.arn}:*"
    include_execution_data = true
    level                  = "ERROR"
  }
}

#################### CloudWatch Logs ####################

resource "aws_cloudwatch_log_group" "step_function_logs" {
  name              = "/aws/vendedlogs/totes_step_function"
  retention_in_days = 60
}

#################### Variables ####################

variable "extract_lambda" { default = "extract_lambda" }
variable "transform_lambda" { default = "transform_lambda" }
variable "load_lambda" { default = "load_lambda" }

variable "default_timeout" { default = 600 }

variable "email_address" {
  default   = "totefullofqueries@gmail.com"
  sensitive = true
}
