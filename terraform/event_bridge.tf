# CloudWatch Event Rule
resource "aws_cloudwatch_event_rule" "scheduler" {
  name                = "lambda-scheduler"
  description         = "Trigger Lambda every 5 minutes"
  schedule_expression = "rate(5 minutes)"
  state               = "ENABLED" 
  tags = {
    Environment = "dev"
  }
}


resource "aws_cloudwatch_event_target" "scheduler_target" {
  rule      = aws_cloudwatch_event_rule.scheduler.name
  target_id = "state-machine-target"
  arn       = aws_sfn_state_machine.totes_step_function.arn
  role_arn  = aws_iam_role.step_function_role.arn
}


# Create Scheduler Schedule 
resource "aws_scheduler_schedule" "scheduler" {
  name               = "my_first_terraform_scheduler"
  schedule_expression = "rate(5 minutes)"
  
  flexible_time_window {
    mode = "OFF"
  }
  
  target {
    arn      = aws_sfn_state_machine.totes_step_function.arn  
    role_arn = aws_iam_role.step_function_role.arn  
  }
}


# Allow permission for Lambda to be triggered by CloudWatch Event
resource "aws_lambda_permission" "allow_cloudwatch" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.totes_extract_lambda.arn 
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.scheduler.arn
}


data "aws_iam_policy_document" "scheduler_policy" {
  statement {
    effect = "Allow"
    actions = [
      "states:StartExecution"
    ]
    resources = [
      aws_sfn_state_machine.totes_step_function.arn
    ]
  }
}

resource "aws_iam_policy" "scheduler_policy_document" {
  name   = "SchedulerStartStateMachinePolicy"
  policy = data.aws_iam_policy_document.scheduler_policy.json
}

resource "aws_iam_role_policy_attachment" "scheduler_policy_attachment" {
  role       = aws_iam_role.step_function_role.name
  policy_arn = aws_iam_policy.scheduler_policy_document.arn
}
