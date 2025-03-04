resource "aws_cloudwatch_event_rule" "scheduler" {
  name                = "lambda-scheduler"
  description         = "Trigger Lambda every 5 minutes"
  schedule_expression = "rate(5 minutes)"
  state               = "DISABLED"
  tags = {
    Environment = "dev"
  }
}
## Create the rule
resource "aws_cloudwatch_event_target" "scheduler_target" {
  rule      = aws_cloudwatch_event_rule.scheduler.name
  target_id = "lambda-target"
  arn       = aws_lambda_function.totes_extract_lambda.arn
}

#### Allow permission to lambda
resource "aws_lambda_permission" "allow_cloudwatch" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.totes_extract_lambda.arn
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.scheduler.arn
}
