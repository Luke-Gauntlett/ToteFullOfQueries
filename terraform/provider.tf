provider "aws" {
  region = "eu-west-2"
  default_tags {
    tags = {
      ProjectName  = "TotesAmazing"
      DeployedFrom = "Terraform"
      Repository   = "TotesFullOfQueries"
    }
  }
}