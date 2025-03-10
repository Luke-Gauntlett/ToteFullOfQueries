resource "aws_lambda_function" "totes_extract_lambda" {
  function_name    = var.extract_lambda
  source_code_hash = data.archive_file.extract_lambda.output_base64sha256
  filename         = data.archive_file.extract_lambda.output_path
  role             = aws_iam_role.extract_lambda_iam_role.arn
  handler          = "${var.extract_lambda}.lambda_handler"
  runtime          = "python3.12"
  timeout          = var.default_timeout
  memory_size      = 512  
  layers           = [
    aws_lambda_layer_version.dependencieslayer.arn,
    aws_lambda_layer_version.utilslayerversion.arn
  ]
}

resource "aws_lambda_function" "totes_transform_lambda" {
  function_name    = var.transform_lambda
  source_code_hash = data.archive_file.transform_lambda.output_base64sha256
  filename         = data.archive_file.transform_lambda.output_path
  role             = aws_iam_role.transform_lambda_iam_role.arn
  handler          = "${var.transform_lambda}.lambda_handler"
  runtime          = "python3.12"
  timeout          = var.default_timeout
  memory_size      = 512
  layers           = [
    aws_lambda_layer_version.dependencieslayer.arn,
    aws_lambda_layer_version.utilslayerversion.arn,
    "arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python312:16"]
}

resource "aws_lambda_function" "totes_load_lambda" {
  function_name    = var.load_lambda
  source_code_hash = data.archive_file.load_lambda.output_base64sha256
  filename         = data.archive_file.load_lambda.output_path
  role             = aws_iam_role.load_lambda_iam_role.arn
  handler          = "${var.load_lambda}.lambda_handler"
  runtime          = "python3.12"
  timeout          = var.default_timeout
  memory_size      = 512  
  layers           = [
    aws_lambda_layer_version.dependencieslayer.arn,
    aws_lambda_layer_version.utilslayerversion.arn,
    "arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python312:16"]
}





