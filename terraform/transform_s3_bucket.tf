
resource "aws_s3_bucket" "transform_bucket" {
  bucket_prefix = var.transform_bucket_prefix

  tags = {
    Name = "Transform Bucket"
  }
}
