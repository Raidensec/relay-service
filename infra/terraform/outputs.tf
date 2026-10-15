output "api_endpoint" {
  description = "HTTP API base URL."
  value       = aws_apigatewayv2_api.http.api_endpoint
}

output "event_table_name" {
  description = "DynamoDB table holding events, attempt records, and replay audit records."
  value       = aws_dynamodb_table.events.name
}

output "dead_letter_queue_url" {
  description = "SQS dead-letter queue for messages that exceed SQS redrive attempts."
  value       = aws_sqs_queue.dead_letter.url
}
