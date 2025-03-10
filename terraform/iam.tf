
#########################################  IAM Roles and role policy documents   ###################################################
# Extract Lambda IAM role
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


#Load Lambda
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


#########################################  IAM Policy for S3 Read/Write   ###################################################
# Extract


data "aws_iam_policy_document" "s3_policy" {
  statement {
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.extract_bucket.arn]
  }

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
  roles      = [aws_iam_role.extract_lambda_iam_role.name, aws_iam_role.transform_lambda_iam_role.name, aws_iam_role.load_lambda_iam_role.name] # Fixed to attach to both roles
  policy_arn = aws_iam_policy.s3_policy.arn


}
# Transform

data "aws_iam_policy_document" "s3_transform_load_policy" {
  statement {
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.transform_bucket.arn]
  }

  statement {
    effect    = "Allow"
    actions   = ["s3:PutObject", "s3:GetObject"]
    resources = ["${resource.aws_s3_bucket.transform_bucket.arn}/*"]
  }
}

resource "aws_iam_policy" "s3_transform_policy_json" {
  name   = "s3_transform_bucket_policy"
  policy = data.aws_iam_policy_document.s3_transform_load_policy.json
}

resource "aws_iam_policy_attachment" "s3_transform_attach_policy" {
  name       = "s3_transform_attach_policy"
  roles      = [aws_iam_role.transform_lambda_iam_role.name, aws_iam_role.load_lambda_iam_role.name]
  policy_arn = aws_iam_policy.s3_transform_policy_json.arn
}
###Load Transform


# resource "aws_iam_policy" "s3_load_policy" {
#   name   = "s3_load_bucket_policy"
#   policy = data.aws_iam_policy_document.s3_transform_policy.json
# }

# resource "aws_iam_policy_attachment" "s3_load_attach_policy" {
#   name       = "s3_load_attach_policy"
#   roles      = [aws_iam_role.lambda_iam3.name]
#   policy_arn = aws_iam_policy.s3_transform_policy.arn
# }

########################################################## IAM Policy for SNS Notification   ###########################################################


data "aws_iam_policy_document" "sns_policy" {
  statement {
    effect    = "Allow"
    actions   = ["sns:Publish"]
    resources = ["arn:aws:sns:eu-west-2:${data.aws_caller_identity.current.account_id}:"]
  }

}
resource "aws_iam_policy" "sns_policy" {
  name   = "sns_policy"
  policy = data.aws_iam_policy_document.sns_policy.json

}

resource "aws_iam_policy_attachment" "sns_attach_policy" {
  name       = "sns_attach_policy"
  roles      = [aws_iam_role.extract_lambda_iam_role.name]
  policy_arn = aws_iam_policy.sns_policy.arn
}

resource "aws_iam_policy_attachment" "sns_attach_policy2" {
  name       = "sns_attach_policy2"
  roles      = [aws_iam_role.transform_lambda_iam_role.name]
  policy_arn = aws_iam_policy.sns_policy.arn  
}


#########################################  IAM Policy for Secrets Manager Get Secret  ###################################################

data "aws_iam_policy_document" "secret_manager_policy" {
  statement {
    effect    = "Allow"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = ["arn:aws:secretsmanager:eu-west-2:${data.aws_caller_identity.current.account_id}:secret:*"]
  }
  ##The above policy is currently using the * wildcard, where all secrets can be accessed
}
resource "aws_iam_policy" "secret_manager_policy_json" {
  name   = "secret_manager_policy_json"
  policy = data.aws_iam_policy_document.secret_manager_policy.json

}

resource "aws_iam_policy_attachment" "secret_manager_attach_policy" {
  name       = "secret_manager_attach_policy"
  roles      = [aws_iam_role.extract_lambda_iam_role.name, aws_iam_role.load_lambda_iam_role.name]
  policy_arn = aws_iam_policy.secret_manager_policy_json.arn
}




#########################################  IAM Policy for Cloud Watch  ###################################################


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
  roles      = [aws_iam_role.extract_lambda_iam_role.name]
  policy_arn = aws_iam_policy.cloudwatch_policy.arn
}

resource "aws_iam_policy_attachment" "cloudwatch_attach_policy2" {
  name       = "cloudwatch_attach_policy2"
  roles      = [aws_iam_role.transform_lambda_iam_role.name]
  policy_arn = aws_iam_policy.cloudwatch_policy.arn  # Reusing the existing S3 policy
}


#attach cloudwatch policy to lambda_iam role


# resource "aws_iam_policy" "postgres_policy" {
#   name   = "postgres_lambda_policy"
#   policy = data.aws_iam_policy_document.postgres_policy.json
# }

# resource "aws_iam_role_policy_attachment" "lambda_postgres_policy_attachment" {
#   role       = aws_iam_role.lambda_iam.name
#   policy_arn = aws_iam_policy.postgres_policy.arn
# }

# might need seperate log groups etc for each lambda

# resource "aws_cloudwatch_log_group" "extract_lambda_logs" {
#   name = "/aws/lambda/${var.extract_lambda}"
# }

# aws_cloudwatch_log_group.extract_lambda_logs.name for log_group_name

resource "aws_cloudwatch_log_metric_filter" "cw_metric_filter" {
  name           = "cw_metric_filter"
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
  metric_name         = aws_cloudwatch_log_metric_filter.cw_metric_filter.name
  namespace           = "cw_metrics"
  period              = 300
  statistic           = "Sum"
  threshold           = 1
  alarm_description   = "This metric monitors errors for extract lambda"
  alarm_actions       = [aws_sns_topic.extraction_updates.arn]


}

resource "aws_cloudwatch_log_metric_filter" "transform_cw_metric_filter" {
  name           = "transform_cw_metric_filter"
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
  metric_name         = aws_cloudwatch_log_metric_filter.transform_cw_metric_filter.name
  namespace           = "cw_metrics"
  period              = 300
  statistic           = "Sum"
  threshold           = 1
  alarm_description   = "This metric monitors errors for transform lambda"
  alarm_actions       = [aws_sns_topic.transform_updates.arn]
}

# resource "aws_cloudwatch_log_metric_filter" "load_cw_metric_filter" {
#   name           = "transform_cw_metric_filter"
#   pattern        = "?ERROR ?Failed ?Exception"
#   log_group_name = "/aws/lambda/${var.load_lambda}"

#   metric_transformation {
#     name      = "EventCount"
#     namespace = "cw_metrics"
#     value     = "1"
#   }
# }

# resource "aws_cloudwatch_metric_alarm" "load_lambda_alert" {
#   alarm_name          = "load_lambda_alert"
#   comparison_operator = "GreaterThanOrEqualToThreshold"
#   evaluation_periods  = 1
#   metric_name         = aws_cloudwatch_log_metric_filter.load_cw_metric_filter.name
#   namespace           = "cw_metrics"
#   period              = 300
#   statistic           = "Sum"
#   threshold           = 1
#   alarm_description   = "This metric monitors errors for load lambda"
#   alarm_actions       = [aws_sns_topic.load_updates.arn]
# }

#######################################################  IAM Policy for Step Function #################################################
#The role
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

# creates lambda permissions

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

resource "aws_iam_policy" "step_function_lambda_policy" {
  name   = "StepFunctionLambdaInvokePolicy"
  policy = data.aws_iam_policy_document.step_function_lambda_policy.json
}

resource "aws_iam_role_policy_attachment" "step_function_lambda_policy_attachment" {
  role       = aws_iam_role.step_function_role.name
  policy_arn = aws_iam_policy.step_function_lambda_policy.arn
}


# create cloudwatch permissions

#creates cloudwatch logs
resource "aws_cloudwatch_log_group" "step_function_logs" {
  name              = "/aws/vendedlogs/totes_step_function"
  retention_in_days = 30
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
      aws_cloudwatch_log_group.step_function_logs.arn,
      "${aws_cloudwatch_log_group.step_function_logs.arn}:*"
    ]
  }
}

resource "aws_iam_policy" "step_logging_policy" {
  name   = "StepFunctionLoggingPolicy"
  policy = data.aws_iam_policy_document.step_function_logging_policy.json
}

resource "aws_iam_role_policy_attachment" "step_function_logging_attachment" {
  role       = aws_iam_role.step_function_role.name
  policy_arn = aws_iam_policy.step_logging_policy.arn
}


resource "aws_cloudwatch_log_resource_policy" "step_function_logs_policy" {
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

# resource "aws_iam_role_policy_attachment" "step_function_cw_attachment" {
#   role       = aws_iam_role.step_function_role.name
#   policy_arn = aws_cloudwatch_log_resource_policy.step_function_logs_policy.arn
# }





#########################################  IAM Policy for Database Access/RDS  #################################################

# data "aws_iam_policy_document" "postgres_policy" {
#   statement {
#     effect = "Allow"
#     actions = [
#       "rds:DescribeDBInstances",
#       "rds:Connect"
#     ]
#     resources = [
#       "arn:aws:rds-db:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:dbuser:${var.database_id}/${var.database_user}"
#     ]
#   }

#   statement {
#     effect  = "Allow"
#     actions = ["secretsmanager:GetSecretValue"]
#     resources = [
#       "arn:aws:secretsmanager:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:secret:*"
#     ]
#   }
# }

# resource "aws_iam_policy" "postgres_policy" {
#   name   = "postgres_lambda_policy"
#   policy = data.aws_iam_policy_document.postgres_policy.json
# }

# resource "aws_iam_role_policy_attachment" "lambda_postgres_policy_attachment" {
#   role       = aws_iam_role.extract_lambda_iam_role.arn
#   policy_arn = aws_iam_policy.postgres_policy.arn
# }



