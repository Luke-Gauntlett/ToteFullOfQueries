# resource "aws_cloudwatch_event_rule" "scheduler" {
#   name                = "lambda-scheduler"
#   description         = "Trigger Lambda every 5 minutes"
#   schedule_expression = "rate(5 minutes)"
#   state               = "DISABLED"
#   tags = {
#     Environment = "dev"
#   }
# }
# ## Create the rule
# resource "aws_cloudwatch_event_target" "scheduler_target" {
#   rule      = aws_cloudwatch_event_rule.scheduler.name
#   target_id = "lambda-target"
#   arn       = aws_lambda_function.totes_extract_lambda.arn
# }
# resource "aws_scheduler_schedule" "scheduler" {
#   name       = "my_first_terraform_scheduler"
#   flexible_time_window {
#     mode = "OFF"
#   }
#   schedule_expression = "rate(5 minutes)"
#   target {
#     arn      = aws_sfn_state_machine.totes_step_function.arn
#     role_arn = aws_iam_role.step_function_role.arn
#   }
# }
# #### Allow permission to lambda
# resource "aws_lambda_permission" "allow_cloudwatch" {
#   statement_id  = "AllowExecutionFromCloudWatch"
#   action        = "lambda:InvokeFunction"
#   function_name = aws_lambda_function.totes_extract_lambda.arn
#   principal     = "events.amazonaws.com"
#   source_arn    = aws_cloudwatch_event_rule.scheduler.arn
# }


resource "aws_sfn_state_machine" "totes_step_function" {
  name     = "totes_step_function"
  role_arn = aws_iam_role.step_function_role.arn


  definition = <<EOF
{
  "Comment": "Execute ETL lambdas in sequence",
  "StartAt": "extract_lambda",
  "States": {
    "extract_lambda": {
      "Type": "Task",
      "Resource": "${aws_lambda_function.totes_extract_lambda.arn}",
      "Next": "transform_lambda"
    },
    "transform_lambda": {
      "Type": "Task",
      "Resource": "${aws_lambda_function.totes_transform_lambda.arn}",
      "End": true
    }
  }
}
EOF

  depends_on = [
    aws_cloudwatch_log_group.step_function_logs,
    aws_iam_role.step_function_role,
    aws_iam_policy.step_function_policy,
    aws_iam_policy.step_function_logging_policy
  ]


}


# logging_configuration {
#     log_destination        = "${aws_cloudwatch_log_group.step_function_logs.arn}:*"
#     include_execution_data = true
#     level                  = "ALL"
#   }