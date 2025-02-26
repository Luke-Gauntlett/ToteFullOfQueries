variable "extract_lambda" {
  type    = string
  default = "extract"
}

variable "extract_bucket_prefix" {
  type    = string
  default = "totes-extract-bucket-"
}

variable "transform_bucket_prefix" {
  type    = string
  default = "totes-transform-bucket-"
}

# variable "transform_lambda" {
#   type    = string
#   default = "transform"
# }

# variable "load_lambda" {
#   type    = string
#   default = "load"
# }

variable "default_timeout" {
  type    = number
  default = 5
}

# variable "state_machine_name" {
#   type    = string
#   default = "totes-amazing-workflow-"
# }

#commented out future steps

variable "email_address" {
  type = string
  default = "testemail@gmail.com"
  sensitive = true
}
