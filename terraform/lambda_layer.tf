#making the zip file for the extract lambda function
data "archive_file" "extract_lambda" {
  type        = "zip"
  source_file = "${path.module}/../src/extract_lambda.py"
  output_path = "${path.module}/../packages/extract_lambda/function.zip"
}

#making the zip file for the extract lambda function
data "archive_file" "transform_lambda" {
  type        = "zip"
  source_file = "${path.module}/../src/transform_lambda.py"
  output_path = "${path.module}/../packages/transform_lambda/function.zip"
}

# making the zip file for lambda layer
data "archive_file" "layer_code" {
  type        = "zip"
  output_path = "${path.module}/../packages/extract_lambda/layer.zip"
  source_dir  = "${path.module}/../dependencies"
}

data "archive_file" "connection_resource" {
  type        = "zip"
  source_dir = "${path.module}/../GetSecrets"
  output_path = "${path.module}/../connection_resource/connections.zip"
}

# lambda zip code being put in the above bucket
resource "aws_s3_object" "lambda_zip_code" {
  for_each = toset([var.extract_lambda,var.transform_lambda])
  # add var.transform_lambda, var.load_lambda above when ready
  # currently waiting for vars file to be completed
  bucket = aws_s3_bucket.code_bucket.bucket
  key    = "${each.key}/function.zip"
  source = "${path.module}/../packages/${each.key}/function.zip"
  # each.key loops over items in for_each at the top
  etag = filemd5("${path.module}/../packages/${each.key}/function.zip")
}

# lambda layer code uploading into the bucket
resource "aws_s3_object" "lambda_layer" {
  bucket     = aws_s3_bucket.code_bucket.bucket
  key        = "layer/layer.zip"
  source     = data.archive_file.layer_code.output_path
  etag       = filemd5(data.archive_file.layer_code.output_path)
  depends_on = [data.archive_file.layer_code]
}

# Layer to zip connection to secrets manager and db
resource "aws_lambda_layer_version" "connection_resource_layer"{
  layer_name = "connection_resource_layer"
  description = "Layer to add connect to secrets manager and db"
  filename = data.archive_file.connection_resource.output_path
  compatible_runtimes = ["python3.12"]
}

# references the layer zip code in the bucket, for the main lambda function
# to point to under the key of 'layers'
resource "aws_lambda_layer_version" "dependencies" {
  layer_name = "requirements_layer"
  s3_bucket  = aws_s3_object.lambda_layer.bucket
  s3_key     = aws_s3_object.lambda_layer.key
}

