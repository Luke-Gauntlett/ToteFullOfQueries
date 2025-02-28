resource "aws_sns_topic" "extraction_updates"{
    name="extraction-updates"
}

resource "aws_sns_topic_subscription" "email_subscription"{
    topic_arn=aws_sns_topic.extraction_updates.arn
    protocol = "email"
    endpoint = var.email_address
}