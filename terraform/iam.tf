
#########################################  IAM Roles and role policy documents   ######################################### 

# Extract Lambda
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


# Transform Lambda

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


# Load Lambda
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


#########################################  IAM Policy for S3 Read/Write   #########################################

# Extract Bucket Permisions
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

######################################### IAM Policy for SNS Notification   #########################################


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

#########################################  IAM Policy for Secrets Manager Get Secret  #########################################

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




#########################################  IAM Policy for Cloud Watch  #########################################


data "aws_iam_policy_document" "cloudwatch_policy" {

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
    
  }
}


resource "aws_iam_policy" "cloudwatch_policy_document" {
  name   = "cloudwatch_policy_document"
  policy = data.aws_iam_policy_document.cloudwatch_policy.json
}


resource "aws_iam_policy_attachment" "cloudwatch_attach_policy" {
  name       = "cloudwatch_attach_policy"
  roles      = [aws_iam_role.extract_lambda_iam_role.name, aws_iam_role.transform_lambda_iam_role.name, aws_iam_role.load_lambda_iam_role.name]
  policy_arn = aws_iam_policy.cloudwatch_policy_document.arn
}


# Extract Lambda
resource "aws_cloudwatch_log_metric_filter" "extract_metric_filter" {
  name           = "extract_metric_filter"
  pattern        = "?ERROR ?Failed ?Exception"
  log_group_name = "/aws/lambda/${var.extract_lambda}"

  metric_transformation {
    name      = "EventCount"
    namespace = "cw_metrics"
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "extract_lambda_alert" {
  alarm_name          = "extract_lambda_alert"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = aws_cloudwatch_log_metric_filter.extract_metric_filter.name
  namespace           = "cw_metrics"
  period              = 300
  statistic           = "Sum"
  threshold           = 1
  alarm_description   = "This metric monitors errors for extract lambda"
  alarm_actions       = [aws_sns_topic.extraction_updates.arn]


}

# Transform Lambda
resource "aws_cloudwatch_log_metric_filter" "transform_metric_filter" {
  name           = "transform_metric_filter"
  pattern        = "?ERROR ?Failed ?Exception"
  log_group_name = "/aws/lambda/${var.transform_lambda}"

  metric_transformation {
    name      = "EventCount"
    namespace = "cw_metrics"
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "transform_lambda_alert" {
  alarm_name          = "transform_lambda_alert"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = aws_cloudwatch_log_metric_filter.transform_metric_filter.name
  namespace           = "cw_metrics"
  period              = 300
  statistic           = "Sum"
  threshold           = 1
  alarm_description   = "This metric monitors errors for transform lambda"
  alarm_actions       = [aws_sns_topic.transform_updates.arn]
}

resource "aws_cloudwatch_log_metric_filter" "load_metric_filter" {
  name           = "load_metric_filter"
  pattern        = "?ERROR ?Failed ?Exception"
  log_group_name = "/aws/lambda/${var.load_lambda}"

  metric_transformation {
    name      = "EventCount"
    namespace = "cw_metrics"
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "load_lambda_alert" {
  alarm_name          = "load_lambda_alert"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = aws_cloudwatch_log_metric_filter.load_metric_filter.name
  namespace           = "cw_metrics"
  period              = 300
  statistic           = "Sum"
  threshold           = 1
  alarm_description   = "This metric monitors errors for load lambda"
  alarm_actions       = [aws_sns_topic.load_updates.arn]
}

#######################################################  IAM Policy for Step Function #################################################
# Step Function Role
resource "aws_iam_role" "step_function_role" {
  name = "StepFunctionRole"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "states.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

# Lambda permissions

data "aws_iam_policy_document" "step_function_lambda_policy" {
  statement {
    effect  = "Allow"
    actions = ["lambda:InvokeFunction"]
    resources = [
      "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:${var.extract_lambda}",
      "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:${var.transform_lambda}",
      "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:${var.load_lambda}"
    ]
  }
}

resource "aws_iam_policy" "step_function_lambda_policy_document" {
  name   = "StepFunctionLambdaInvokePolicyDocument"
  policy = data.aws_iam_policy_document.step_function_lambda_policy.json
}

resource "aws_iam_role_policy_attachment" "step_function_lambda_policy_attachment" {
  role       = aws_iam_role.step_function_role.name
  policy_arn = aws_iam_policy.step_function_lambda_policy_document.arn
}

# CloudWatch Permissions
resource "aws_cloudwatch_log_group" "step_function_logs" {
  name              = "/aws/vendedlogs/totes_step_function"
  retention_in_days = 60
}


data "aws_iam_policy_document" "step_function_logging_policy" {
  statement {
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
      "logs:DescribeLogStreams"
    ]
    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/vendedlogs/totes_step_function:*"
    ]
  }
}

# CloudWatch Log Resource Policy


resource "aws_cloudwatch_log_resource_policy" "cw_sf_policy" {
  policy_name = "StepFunctionLogsPolicy"
  policy_document = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Principal = {
          Service = "states.amazonaws.com"
        },
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ],
        Resource = [
          aws_cloudwatch_log_group.step_function_logs.arn,
          "${aws_cloudwatch_log_group.step_function_logs.arn}:*"
        ]
      }
    ]
  })
}

# resource "aws_iam_policy" "step_function_logging_policy_document" {
#   name   = "StepFunctionLoggingPolicyDocument"
#   policy = data.aws_iam_policy_document.step_function_logging_policy.json
# }


# resource "aws_iam_role_policy_attachment" "step_function_logging_attach_policy" {
#   role       = aws_iam_role.step_function_role.name
#   policy_arn = aws_iam_policy.step_function_logging_policy_document.arn
# }





# resource "aws_cloudwatch_log_resource_policy" "cw_sf_policy" {
#   policy_name = "StepFunctionLogsPolicy"
#   policy_document = jsonencode({
#     Version = "2012-10-17",
#     Statement = [
#       {
#         Effect = "Allow",
#         Principal = {
#           Service = "states.amazonaws.com"
#         },
#         Action = [
#           "logs:CreateLogStream",
#           "logs:PutLogEvents"
#         ],
#         Resource = [
#           aws_cloudwatch_log_group.step_function_logs.arn,
#           "${aws_cloudwatch_log_group.step_function_logs.arn}:*"
#         ]
#       }
#     ]
#   })
# }

# # resource "aws_iam_role_policy_attachment" "step_function_cw_attachment" {
# #   role       = aws_iam_role.step_function_role.name
# #   policy_arn = aws_cloudwatch_log_resource_policy.step_function_logs_policy.arn
# # }





