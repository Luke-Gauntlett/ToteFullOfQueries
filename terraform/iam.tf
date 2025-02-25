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

#attach cloudwatch policy to lambda_iam role