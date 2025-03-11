resource "aws_s3_bucket" "extract_bucket" {
  bucket_prefix = var.extract_bucket_prefix

  tags = {
    Name = "Extract Bucket"
  }

}

resource "aws_s3_bucket" "transform_bucket" {
  bucket_prefix = var.transform_bucket_prefix

  tags = {
    Name = "Transform Bucket"
  }
}


# # bucket to store the lambda zip code and layer zip
# resource "aws_s3_bucket" "code_bucket" {
#   bucket_prefix = "project-lambda-layer-and-functions-"
# }



