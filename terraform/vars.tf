variable "extract_lambda" {
  type    = string
  default = "extract"
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