# resource "aws_cloudwatch_event_rule" "scheduler" {
#   name                = "lambda-scheduler"
#   description         = "Trigger Lambda every 5 minutes"
#   schedule_expression = "rate(5 minutes)"
#   state               = "ENABLED"
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
#     role_arn = aws_iam_role.eventbridge_role.arn
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


# resource "aws_iam_role" "eventbridge_role" {
#   name = "eventbridge_step_function_role"

#   assume_role_policy = jsonencode({
#     Version = "2012-10-17",
#     Statement = [
#       {
#         Effect = "Allow",
#         Principal = {
#           Service = "scheduler.amazonaws.com"
#         },
#         Action = "sts:AssumeRole"
#       }
#     ]
#   })
# }

# resource "aws_iam_policy" "eventbridge_sf_policy" {
#   name   = "eventbridge_sf_policy"
#   policy = jsonencode({
#     Version = "2012-10-17",
#     Statement = [
#       {
#         Effect = "Allow",
#         Action = "states:StartExecution",
#         Resource = aws_sfn_state_machine.totes_step_function.arn
#       }
#     ]
#   })
# }

# resource "aws_iam_role_policy_attachment" "eventbridge_sf_policy_attachment" {
#   role       = aws_iam_role.eventbridge_role.name
#   policy_arn = aws_iam_policy.eventbridge_sf_policy.arn
# }


# logging_configuration {
#     log_destination        = "${aws_cloudwatch_log_group.step_function_logs.arn}:*"
#     include_execution_data = true
#     level                  = "ALL"
#   }





# EventBridge Rule

resource "aws_cloudwatch_event_rule" "scheduler" {
  name                = "lambda-scheduler"
  description         = "Trigger Step Function every 5 minutes"
  schedule_expression = "rate(5 minutes)"
  state               = "ENABLED"
  tags = {
    Environment = "dev"
  }
}


# EventBridge Target

resource "aws_cloudwatch_event_target" "scheduler_target" {
  rule      = aws_cloudwatch_event_rule.scheduler.name
  target_id = "step-function-target"
  arn       = aws_sfn_state_machine.totes_step_function.arn
  role_arn  = aws_iam_role.eventbridge_role.arn
}

# EventBridge IAM role with permissons
resource "aws_iam_role" "eventbridge_role" {
  name = "eventbridge_step_function_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "events.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_policy" "eventbridge_sf_policy" {
  name   = "eventbridge_sf_policy"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow"
        Action = "states:StartExecution"
        Resource = aws_sfn_state_machine.totes_step_function.arn
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "eventbridge_sf_policy_attachment" {
  role       = aws_iam_role.eventbridge_role.name
  policy_arn = aws_iam_policy.eventbridge_sf_policy.arn
}


resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.totes_extract_lambda.arn
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.scheduler.arn
}
