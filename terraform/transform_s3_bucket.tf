
resource "aws_s3_bucket" "transform_bucket" {
  bucket_prefix = "totes-transform"

  tags = {
    Name        = "Transform Bucket"
  }
}