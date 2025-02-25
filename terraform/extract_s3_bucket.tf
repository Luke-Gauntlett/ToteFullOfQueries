resource "aws_s3_bucket" "extract_bucket" {
  bucket_prefix = "totes-extract"
  
    tags = {
    Name        = "Extract Bucket"
  }

}

