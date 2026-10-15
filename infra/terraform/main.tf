locals {
  account_id = var.aws_account_id != "" ? var.aws_account_id : "000000000000"
  name       = "relay-service"
  functions = {
    api        = { handler = "relay_service.aws_handlers.api_handler", timeout = 15 }
    worker     = { handler = "relay_service.aws_handlers.worker_handler", timeout = 30 }
    reconciler = { handler = "relay_service.aws_handlers.reconciler_handler", timeout = 30 }
  }
  destinations_json = jsonencode({
    for destination_name, destination in var.destinations : destination_name => {
      url        = destination.url
      secret_ref = destination.secret_arn
    }
  })
  log_arn = "arn:aws:logs:${var.aws_region}:${local.account_id}:log-group:/aws/lambda/${local.name}-*:log-stream:*"
}

resource "aws_dynamodb_table" "events" {
  name         = "${local.name}-events"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "event_key"
  range_key    = "record_key"

  attribute {
    name = "event_key"
    type = "S"
  }
  attribute {
    name = "record_key"
    type = "S"
  }
  attribute {
    name = "state"
    type = "S"
  }
  attribute {
    name = "available_at"
    type = "N"
  }
  attribute {
    name = "event_id"
    type = "S"
  }

  global_secondary_index {
    name            = "state-available-index"
    hash_key        = "state"
    range_key       = "available_at"
    projection_type = "ALL"
  }

  global_secondary_index {
    name            = "event-id-index"
    hash_key        = "event_id"
    range_key       = "record_key"
    projection_type = "ALL"
  }

  point_in_time_recovery {
    enabled = true
  }
  server_side_encryption {
    enabled = true
  }
}

resource "aws_sqs_queue" "dead_letter" {
  name                    = "${local.name}-dead-letter"
  sqs_managed_sse_enabled = true
}

resource "aws_sqs_queue" "events" {
  name                       = "${local.name}-events"
  visibility_timeout_seconds = var.queue_visibility_seconds
  message_retention_seconds  = 1209600
  sqs_managed_sse_enabled    = true
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dead_letter.arn
    maxReceiveCount     = 5
  })
}

resource "aws_cloudwatch_log_group" "lambda" {
  for_each          = local.functions
  name              = "/aws/lambda/${local.name}-${each.key}"
  retention_in_days = 30
}

resource "aws_iam_role" "lambda" {
  for_each = local.functions
  name     = "${local.name}-${each.key}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "lambda" {
  for_each = local.functions
  name     = "${local.name}-${each.key}-runtime"
  role     = aws_iam_role.lambda[each.key].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat(
      [{
        Effect   = "Allow"
        Action   = ["logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = [local.log_arn]
      }],
      each.key == "api" ? [
        {
          Effect   = "Allow"
          Action   = ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:TransactWriteItems", "dynamodb:Query"]
          Resource = [aws_dynamodb_table.events.arn, "${aws_dynamodb_table.events.arn}/index/*"]
        },
        { Effect = "Allow", Action = ["sqs:SendMessage"], Resource = [aws_sqs_queue.events.arn] },
        {
          Effect   = "Allow"
          Action   = ["secretsmanager:GetSecretValue"]
          Resource = [var.ingress_secret_arn, var.operator_secret_arn]
        }
      ] : [],
      each.key == "worker" ? [
        {
          Effect   = "Allow"
          Action   = ["dynamodb:GetItem", "dynamodb:TransactWriteItems"]
          Resource = [aws_dynamodb_table.events.arn]
        },
        { Effect = "Allow", Action = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"], Resource = [aws_sqs_queue.events.arn] },
        { Effect = "Allow", Action = ["secretsmanager:GetSecretValue"], Resource = [for entry in values(var.destinations) : entry.secret_arn] }
      ] : [],
      each.key == "reconciler" ? [
        {
          Effect   = "Allow"
          Action   = ["dynamodb:Query", "dynamodb:UpdateItem"]
          Resource = [aws_dynamodb_table.events.arn, "${aws_dynamodb_table.events.arn}/index/state-available-index"]
        },
        { Effect = "Allow", Action = ["sqs:SendMessage"], Resource = [aws_sqs_queue.events.arn] }
      ] : []
    )
  })
}

resource "aws_lambda_function" "service" {
  for_each         = local.functions
  function_name    = "${local.name}-${each.key}"
  role             = aws_iam_role.lambda[each.key].arn
  handler          = each.value.handler
  runtime          = "python3.12"
  architectures    = ["arm64"]
  timeout          = each.value.timeout
  memory_size      = 256
  filename         = "${path.module}/${var.package_path}"
  source_code_hash = filebase64sha256("${path.module}/${var.package_path}")

  environment {
    variables = merge(
      {
        RELAY_TABLE_NAME               = aws_dynamodb_table.events.name
        RELAY_QUEUE_URL                = aws_sqs_queue.events.url
        RELAY_DESTINATIONS_JSON        = local.destinations_json
        RELAY_QUEUE_VISIBILITY_SECONDS = tostring(var.queue_visibility_seconds)
        RELAY_MAX_ATTEMPTS             = "5"
        RELAY_RETRY_BASE_SECONDS       = "1"
        RELAY_RETRY_MAX_SECONDS        = "300"
        RELAY_REQUEST_TIMEOUT_SECONDS  = "10"
      },
      each.key == "api" ? {
        RELAY_INGRESS_SECRET_ARN  = var.ingress_secret_arn
        RELAY_OPERATOR_SECRET_ARN = var.operator_secret_arn
      } : {}
    )
  }

  depends_on = [aws_cloudwatch_log_group.lambda, aws_iam_role_policy.lambda]
}

resource "aws_sqs_queue_redrive_allow_policy" "dead_letter" {
  queue_url = aws_sqs_queue.dead_letter.url
  redrive_allow_policy = jsonencode({
    redrivePermission = "byQueue"
    sourceQueueArns   = [aws_sqs_queue.events.arn]
  })
}

resource "aws_lambda_event_source_mapping" "queue" {
  event_source_arn        = aws_sqs_queue.events.arn
  function_name           = aws_lambda_function.service["worker"].arn
  batch_size              = 10
  function_response_types = ["ReportBatchItemFailures"]
  enabled                 = true
}

resource "aws_apigatewayv2_api" "http" {
  name          = local.name
  protocol_type = "HTTP"
}

resource "aws_apigatewayv2_integration" "api" {
  api_id                 = aws_apigatewayv2_api.http.id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.service["api"].invoke_arn
  payload_format_version = "2.0"
}

resource "aws_apigatewayv2_route" "ingress" {
  api_id    = aws_apigatewayv2_api.http.id
  route_key = "POST /events"
  target    = "integrations/${aws_apigatewayv2_integration.api.id}"
}

resource "aws_apigatewayv2_route" "replay" {
  api_id    = aws_apigatewayv2_api.http.id
  route_key = "POST /events/{event_id}/replay"
  target    = "integrations/${aws_apigatewayv2_integration.api.id}"
}

resource "aws_apigatewayv2_stage" "default" {
  api_id      = aws_apigatewayv2_api.http.id
  name        = "$default"
  auto_deploy = true
}

resource "aws_lambda_permission" "api_gateway" {
  statement_id  = "AllowHttpApiInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.service["api"].function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.http.execution_arn}/*/POST/*"
}

resource "aws_cloudwatch_event_rule" "reconcile" {
  name                = "${local.name}-reconcile-pending"
  schedule_expression = "rate(1 minute)"
}

resource "aws_cloudwatch_event_target" "reconcile" {
  rule      = aws_cloudwatch_event_rule.reconcile.name
  target_id = "reconcile-pending"
  arn       = aws_lambda_function.service["reconciler"].arn
}

resource "aws_lambda_permission" "eventbridge" {
  statement_id  = "AllowEventBridgeReconcile"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.service["reconciler"].function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.reconcile.arn
}

resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  for_each            = local.functions
  alarm_name          = "${local.name}-${each.key}-errors"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Sum"
  threshold           = 1
  dimensions          = { FunctionName = aws_lambda_function.service[each.key].function_name }
}

resource "aws_cloudwatch_metric_alarm" "queue_age" {
  alarm_name          = "${local.name}-oldest-message"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "ApproximateAgeOfOldestMessage"
  namespace           = "AWS/SQS"
  period              = 300
  statistic           = "Maximum"
  threshold           = 300
  dimensions          = { QueueName = aws_sqs_queue.events.name }
}

resource "aws_cloudwatch_metric_alarm" "dead_letter_messages" {
  alarm_name          = "${local.name}-dead-letter-visible"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 300
  statistic           = "Maximum"
  threshold           = 1
  dimensions          = { QueueName = aws_sqs_queue.dead_letter.name }
}
