
#########################################  IAM Role and IAM Lambda Policy document   ###################################################
resource "aws_iam_role" "lambda_iam" {
  name                  = "lambda_iam"
  assume_role_policy    = data.aws_iam_policy_document.assume_role.json
  force_detach_policies = true
}

data "aws_iam_policy_document" "assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}



#######################################################              IAM Policy for S3 Read/Write   #######################################################

data "aws_iam_policy_document" "s3_policy" {
  statement {
    effect    = "Allow"
    actions   = ["s3:PutObject", "s3:GetObject", "s3:ListBucket"]
    resources = ["${resource.aws_s3_bucket.extract_bucket.arn}/*"]
  }
}

resource "aws_iam_policy" "s3_policy" {
  name   = "s3_bucket_policy"
  policy = data.aws_iam_policy_document.s3_policy.json

}

resource "aws_iam_policy_attachment" "s3_attach_policy" {
  name       = "s3_attach_policy"
  roles      = [aws_iam_role.lambda_iam.name]
  policy_arn = aws_iam_policy.s3_policy.arn
}


########################################################## IAM Policy for SNS Notification   ###########################################################


data "aws_iam_policy_document" "sns_policy" {
  statement {
    effect    = "Allow"
    actions   = ["sns:Publish"]
    resources = ["arn:aws:sns:eu-west-2:${data.aws_caller_identity.current.account_id}:extraction-updates"]
  }
  

}
resource "aws_iam_policy" "sns_policy" {
  name   = "sns_policy"
  policy = data.aws_iam_policy_document.sns_policy.json

}

resource "aws_iam_policy_attachment" "sns_attach_policy" {
  name       = "sns_attach_policy"
  roles      = [aws_iam_role.lambda_iam.name]
  policy_arn = aws_iam_policy.sns_policy.arn
}

#########################################  IAM Policy for Secrets Manager Get Secret  ###################################################

data "aws_iam_policy_document" "secret_manager_policy" {
  statement {
    effect    = "Allow"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = ["arn:aws:secretsmanager:eu-west-2:${data.aws_caller_identity.current.account_id}:secret:*"]
  }
##The above policy is currently using the * wildcard, where all secrets can be accessed
}
resource "aws_iam_policy" "secret_manager_policy" {
  name   = "secret_manager_policy"
  policy = data.aws_iam_policy_document.secret_manager_policy.json

}

resource "aws_iam_policy_attachment" "secret_manager_attach_policy" {
  name       = "secret_manager_attach_policy"
  roles      = [aws_iam_role.lambda_iam.name]
  policy_arn = aws_iam_policy.secret_manager_policy.arn
}

#########################################  IAM Policy for Cloud Watch  ###################################################


data "aws_iam_policy_document" "cloudwatch_policy" {

  #this adds the new document which defines what our lambda is allowed to do with cloudwatch - ie log groups, streams or write log events

  statement {
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/*:*"
    ]
    # resources (above) limits the policy to lambda related log streams
  }
}

#creates document
resource "aws_iam_policy" "cloudwatch_policy" {
  name   = "cloudwatch_lambda_policy"
  policy = data.aws_iam_policy_document.cloudwatch_policy.json
}

# attaches to Lambda
resource "aws_iam_policy_attachment" "cloudwatch_attach_policy" {
  name       = "cloudwatch_attach_policy"
  roles      = [aws_iam_role.lambda_iam.name]
  policy_arn = aws_iam_policy.cloudwatch_policy.arn
}


#########################################  IAM Policy for Database Access/RDS  #################################################

data "aws_iam_policy_document" "postgres_policy" {
  statement {
    effect = "Allow"
    actions = [
      "rds:DescribeDBInstances",
      "rds:Connect"
    ]
    resources = [
      "arn:aws:rds-db:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:dbuser:${var.database_id}/${var.database_user}"
    ]
  }

  statement {
    effect  = "Allow"
    actions = ["secretsmanager:GetSecretValue"]
    resources = [
      "arn:aws:secretsmanager:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:secret:${var.database_secret_name}"
    ]
  }
}

resource "aws_iam_policy" "postgres_policy" {
  name   = "postgres_lambda_policy"
  policy = data.aws_iam_policy_document.postgres_policy.json
}

resource "aws_iam_role_policy_attachment" "lambda_postgres_policy_attachment" {
  role       = aws_iam_role.lambda_iam.name
  policy_arn = aws_iam_policy.postgres_policy.arn
}


#######################################################  IAM Policy for Step Function #################################################
#The role
resource "aws_iam_role" "step_function_role" {
  name = "StepFunctionRole"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "states.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

#The policy Document
data "aws_iam_policy_document" "step_function_policy" {
  statement {
    effect  = "Allow"
    actions = ["lambda:InvokeFunction"]
    resources = [
      "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:your-lambda-function-name"
    ]
  }
}

#Create the policy
resource "aws_iam_policy" "step_function_policy" {
  name   = "step_function_lambda_invoke_policy"
  policy = data.aws_iam_policy_document.step_function_policy.json
}

#Attach the policy
resource "aws_iam_role_policy_attachment" "step_function_lambda_policy_attachment" {
  role       = aws_iam_role.step_function_role.name
  policy_arn = aws_iam_policy.step_function_policy.arn
}
