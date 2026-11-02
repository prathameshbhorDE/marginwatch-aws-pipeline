variable "region" {
  description = "AWS region for every MarginWatch resource"
  type        = string
  default     = "ap-south-1" # Mumbai
}

variable "aws_profile" {
  description = "AWS CLI profile Terraform uses to log in"
  type        = string
  default     = "marginwatch"
}

variable "env" {
  description = "Environment name, part of every resource name"
  type        = string
  default     = "dev"
}

variable "allow_destroy_with_data" {
  description = "true = 'terraform destroy' also deletes buckets that still hold files (handy for a practice account, never in production)"
  type        = bool
  default     = true
}

variable "alert_email" {
  description = "Email address that receives the daily cut-off summary (confirm the subscription email AWS sends)"
  type        = string
}
