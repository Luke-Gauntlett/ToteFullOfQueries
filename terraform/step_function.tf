# resource "aws_sfn_state_machine" "totes_step_function" {
#   name     = "totes_step_function"
#   role_arn = aws_iam_role.step_function_role.arn

#   logging_configuration {
#     log_destination = aws_cloudwatch_log_group.step_function_logs.arn
#     include_execution_data = true
#     level                  = "ERROR"
#   }

#   definition = <<EOF
# {
#   "Comment": "Execute ETL lambdas in sequence",
#   "StartAt": "extract_lambda",
#   "States": {
#     "extract_lambda": {
#       "Type": "Task",
#       "Resource": "${aws_lambda_function.totes_extract_lambda.arn}",
#       "Next": "transform_lambda"
#     },
#     "transform_lambda": {
#       "Type": "Task",
#       "Resource": "${aws_lambda_function.totes_transform_lambda.arn}",
#       "End": true
#     }
#   }
# }
# EOF

#   depends_on = [
#     aws_cloudwatch_log_group.step_function_logs,
#     aws_cloudwatch_log_resource_policy.step_function_logs_policy
#   ]
# }


# ,
#     "load_lambda": {
#       "Type": "Task",
#       "Resource": "${var.load_lambda.arn}",
#       "End": true
#     }