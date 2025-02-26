
# bucket to store the lambda zip code and layer zip
resource "aws_s3_bucket" "code_bucket" {
  bucket_prefix = "project-lambda-layer-and-functions-"
}


#making the zip file for the extract lambda function
data "archive_file" "extract_lambda" {
  type        = "zip"
  output_path = "${path.module}/../packages/extract_lambda/function.zip"
  source_file = "${path.module}/../src/extract_lambda.py"
  depends_on  = [null_resource.trigger_lambda_update]
}
resource "null_resource" "trigger_lambda_update" {
  triggers = {
    always_run = "${timestamp()}"
  }
}
# lambda zip code being put in the above bucket
resource "aws_s3_object" "lambda_zip_code" {
  for_each = toset([var.extract_lambda])
  # add var.transform_lambda, var.load_lambda above when ready
  # currently waiting for vars file to be completed
  bucket = aws_s3_bucket.code_bucket.bucket
  key    = "${each.key}/function.zip"
  source = "${path.module}/../packages/${each.key}/function.zip"
  # each.key loops over items in for_each at the top
  etag = filemd5("${path.module}/../packages/${each.key}/function.zip")
}


# making the zip file for lambda layer
data "archive_file" "layer_code" {
  type        = "zip"
  output_path = "${path.module}/../packages/extract_layer/layer.zip"
  source_dir  = "${path.module}/../dependencies"
}

# lambda layer code uploading into the bucket
resource "aws_s3_object" "lambda_layer" {
  bucket     = aws_s3_bucket.code_bucket.bucket
  key        = "layer/layer.zip"
  source     = data.archive_file.layer_code.output_path
  etag       = filemd5(data.archive_file.layer_code.output_path)
  depends_on = [data.archive_file.layer_code]
}


