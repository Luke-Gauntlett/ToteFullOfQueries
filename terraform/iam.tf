######################################### IAM Roles and Role Policy Documents #########################################

# Extract Lambda IAM Role
data "aws_iam_policy_document" "extract_iam_policy" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "extract_lambda_iam_role" {
  name                  = "extract_lambda_iam"
  assume_role_policy    = data.aws_iam_policy_document.extract_iam_policy.json
  force_detach_policies = true
}

# Transform Lambda IAM Role
data "aws_iam_policy_document" "transform_iam_policy" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "transform_lambda_iam_role" {
  name                  = "transform_lambda_iam"
  assume_role_policy    = data.aws_iam_policy_document.transform_iam_policy.json
  force_detach_policies = true
}

# Load Lambda IAM Role
data "aws_iam_policy_document" "load_iam_policy" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "load_lambda_iam_role" {
  name                  = "load_lambda_iam"
  assume_role_policy    = data.aws_iam_policy_document.load_iam_policy.json
  force_detach_policies = true
}

######################################### IAM Policy for S3 Read/Write #########################################

# Extract Bucket Permissions
data "aws_iam_policy_document" "extract_s3_policy" {
  statement {
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.extract_bucket.arn]
  }

  statement {
    effect    = "Allow"
    actions   = ["s3:PutObject", "s3:GetObject"]
    resources = ["${aws_s3_bucket.extract_bucket.arn}/*"]
  }
}

resource "aws_iam_policy" "extract_s3_policy_document" {
  name   = "extract_s3_policy_document"
  policy = data.aws_iam_policy_document.extract_s3_policy.json
}

resource "aws_iam_policy_attachment" "extract_s3_attach_policy" {
  name       = "extract_s3_attach_policy"
  roles      = [aws_iam_role.extract_lambda_iam_role.name, aws_iam_role.load_lambda_iam_role.name]
  policy_arn = aws_iam_policy.extract_s3_policy_document.arn
}

# Transform Bucket Permissions
data "aws_iam_policy_document" "transform_s3_policy" {
  statement {
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.transform_bucket.arn]
  }

  statement {
    effect    = "Allow"
    actions   = ["s3:PutObject", "s3:GetObject"]
    resources = ["${aws_s3_bucket.transform_bucket.arn}/*"]
  }
}

resource "aws_iam_policy" "transform_s3_policy_document" {
  name   = "transform_s3_policy_document"
  policy = data.aws_iam_policy_document.transform_s3_policy.json
}

resource "aws_iam_policy_attachment" "transform_s3_attach_policy" {
  name       = "transform_s3_attach_policy"
  roles      = [aws_iam_role.transform_lambda_iam_role.name, aws_iam_role.load_lambda_iam_role.name]
  policy_arn = aws_iam_policy.transform_s3_policy_document.arn
}

######################################### IAM Policy for SNS Notification #########################################

data "aws_iam_policy_document" "sns_policy" {
  statement {
    effect    = "Allow"
    actions   = ["sns:Publish"]
    resources = ["arn:aws:sns:eu-west-2:${data.aws_caller_identity.current.account_id}:*"]
  }
}

resource "aws_iam_policy" "sns_policy_document" {
  name   = "sns_policy_document"
  policy = data.aws_iam_policy_document.sns_policy.json
}

resource "aws_iam_policy_attachment" "sns_attach_policy" {
  name       = "sns_attach_policy"
  roles      = [aws_iam_role.extract_lambda_iam_role.name, aws_iam_role.transform_lambda_iam_role.name, aws_iam_role.load_lambda_iam_role.name]
  policy_arn = aws_iam_policy.sns_policy_document.arn
}

######################################### IAM Policy for Secrets Manager #########################################

data "aws_iam_policy_document" "secret_manager_policy" {
  statement {
    effect    = "Allow"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = ["arn:aws:secretsmanager:eu-west-2:${data.aws_caller_identity.current.account_id}:secret:*"]
  }
}

resource "aws_iam_policy" "secret_manager_policy_document" {
  name   = "secret_manager_policy_document"
  policy = data.aws_iam_policy_document.secret_manager_policy.json
}

resource "aws_iam_policy_attachment" "secret_manager_attach_policy" {
  name       = "secret_manager_attach_policy"
  roles      = [aws_iam_role.extract_lambda_iam_role.name, aws_iam_role.load_lambda_iam_role.name]
  policy_arn = aws_iam_policy.secret_manager_policy_document.arn
}
