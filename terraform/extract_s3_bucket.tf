resource "aws_s3_bucket" "extract_bucket" {
  bucket_prefix = var.extract_bucket_prefix

  tags = {
    Name = "Extract Bucket"
  }

}

