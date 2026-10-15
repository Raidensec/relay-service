variable "aws_region" {
  description = "AWS region for the relay resources."
  type        = string
  default     = "us-east-1"
}

variable "aws_account_id" {
  description = "12-digit AWS account ID used to build IAM and secret ARNs."
  type        = string
  default     = ""
  validation {
    condition     = var.offline_plan || can(regex("^[0-9]{12}$", var.aws_account_id))
    error_message = "Set aws_account_id to a 12-digit AWS account ID before deployment."
  }
}

variable "offline_plan" {
  description = "Use fake provider credentials and skip AWS identity checks for a local plan only."
  type        = bool
  default     = false
}

variable "ingress_secret_arn" {
  description = "Secrets Manager ARN containing the shared ingress bearer token."
  type        = string
  validation {
    condition     = can(regex("^arn:aws:secretsmanager:[^:]+:[0-9]{12}:secret:.+$", var.ingress_secret_arn))
    error_message = "ingress_secret_arn must be a Secrets Manager ARN, not a secret value."
  }
}

variable "operator_secret_arn" {
  description = "Distinct Secrets Manager ARN containing the bearer token for cloud replay operations."
  type        = string
  validation {
    condition = (
      can(regex("^arn:aws:secretsmanager:[^:]+:[0-9]{12}:secret:.+$", var.operator_secret_arn)) &&
      var.operator_secret_arn != var.ingress_secret_arn
    )
    error_message = "operator_secret_arn must be a Secrets Manager ARN distinct from ingress_secret_arn."
  }
}

variable "destinations" {
  description = "Named HTTPS delivery destinations and Secrets Manager ARNs holding their HMAC secrets."
  type = map(object({
    url        = string
    secret_arn = string
  }))
  validation {
    condition = length(var.destinations) > 0 && alltrue([
      for destination in values(var.destinations) :
      can(regex("^https://[^/]+", destination.url)) &&
      can(regex("^arn:aws:secretsmanager:[^:]+:[0-9]{12}:secret:.+$", destination.secret_arn))
    ])
    error_message = "Configure at least one HTTPS destination with its Secrets Manager ARN."
  }
}

variable "package_path" {
  description = "Lambda ZIP produced by scripts/package_lambda.py."
  type        = string
  default     = "../../dist/relay-service.zip"
}

variable "queue_visibility_seconds" {
  description = "SQS visibility must exceed six worker timeouts plus the batching window."
  type        = number
  default     = 210
  validation {
    condition     = var.queue_visibility_seconds >= 210 && var.queue_visibility_seconds == floor(var.queue_visibility_seconds)
    error_message = "Queue visibility must be at least 210 seconds for a 30-second Lambda and zero-second batching window."
  }
}
