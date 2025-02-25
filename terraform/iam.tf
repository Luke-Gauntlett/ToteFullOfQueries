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

resource "aws_iam_role" "lambda_iam" {
  name                  = "lambda_iam"
  assume_role_policy    = data.aws_iam_policy_document.assume_role.json
  force_detach_policies = true
}

data "aws_iam_policy_document" "s3_policy" {
  statement {
    effect    = "Allow"
    actions   = ["s3:PutObject", "s3:GetObject"]
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





