variable "extract_lambda" {
  type    = string
  default = "extract_lambda"
}

variable "extract_bucket_prefix" {
  type    = string
  default = "totes-extract-bucket-"
}

variable "transform_bucket_prefix" {
  type    = string
  default = "totes-transform-bucket-"
}

variable "database_secret_name" {
  type    = string
  default = "project_database_credentials"
}

variable "database_id" {
  type    = string
  default = "de_2024_12_02"
}

variable "database_user" {
  type    = string
  default = "project_team_3"
}

variable "default_timeout" {
  type    = number
  default = 600
}

variable "transform_lambda" {
  type    = string
  default = "transform_lambda"
}

# variable "load_lambda" {
#   type    = string
#   default = "load_lambda"
# }

# variable "state_machine_name" {
#   type    = string
#   default = "totes-amazing-workflow-"
# }

#commented out future steps

variable "email_address" {
  type = string
  default = "totefullofqueries@gmail.com"
  sensitive = true
}
