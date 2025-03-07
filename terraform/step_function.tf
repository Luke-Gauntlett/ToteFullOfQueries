resource "aws_sfn_state_machine" "totes_step_function" {
  name     = "totes_step_function"
  role_arn = aws_iam_role.step_function_role.arn


    
  definition = <<EOF
{
  "Comment": "Execute lambdas in sequence",
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
      "Next": "load_lambda"
    },
    "load_lambda": {
      "Type": "Task",
      "Resource": "${aws_lambda_function.totes_load_lambda.arn}", 
      "End": true
    }
  }
}
EOF


  depends_on = [
    aws_cloudwatch_log_group.step_function_logs,
    aws_cloudwatch_log_resource_policy.step_function_logs_policy
  ]
}


  # logging_configuration {
  #   log_destination = "${aws_cloudwatch_log_group.step_function_logs.arn}:*"
  #   include_execution_data = true
  #   level                  = "ERROR"
  # }
