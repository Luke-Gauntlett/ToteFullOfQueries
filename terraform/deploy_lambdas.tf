# making the main extract lambda function, bringing everything together
resource "aws_lambda_function" "totes_extract_lambda" {
  function_name    = var.extract_lambda
  source_code_hash = data.archive_file.extract_lambda.output_base64sha256
  s3_bucket        = aws_s3_bucket.code_bucket.bucket
  s3_key           = "${var.extract_lambda}/function.zip"
  role = aws_iam_role.lambda_iam.arn
  handler    = "${var.extract_lambda}.lambda_handler"
  runtime    = "python3.12"
  timeout    = var.default_timeout
  layers     = [aws_lambda_layer_version.dependencies.arn, aws_lambda_layer_version.connection_resource_layer.arn]
  depends_on = [aws_s3_object.lambda_zip_code, aws_s3_object.lambda_layer]
}








