
resource "aws_s3_bucket" "transform_bucket" {
  bucket_prefix = "totes-extract"

  tags = {
    Name        = "Extract Bucket"
  }
}