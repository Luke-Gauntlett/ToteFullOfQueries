resource "aws_sns_topic" "lambda_error_topic" {
  name = "lambda-error-notifications"
}

resource "aws_sns_topic_subscription" "email_subscription" {
  topic_arn = aws_sns_topic.lambda_error_topic.arn
  protocol  = "email"
  endpoint  = var.email
