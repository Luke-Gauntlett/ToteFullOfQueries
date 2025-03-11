resource "aws_sns_topic" "extraction_updates"{
    name="extraction-updates"
}

resource "aws_sns_topic_subscription" "extract_email_subscription"{
    topic_arn=aws_sns_topic.extraction_updates.arn
    protocol = "email"
    endpoint = var.email_address
}


resource "aws_sns_topic" "transform_updates"{
    name="transformation-updates"
}

resource "aws_sns_topic_subscription" "transform_email_subscription"{
    topic_arn=aws_sns_topic.transform_updates.arn
    protocol = "email"
    endpoint = var.email_address
}

resource "aws_sns_topic" "load_updates"{
    name="load-updates"
}

resource "aws_sns_topic_subscription" "load_email_subscription"{
    topic_arn=aws_sns_topic.load_updates.arn
    protocol = "email"
    endpoint = var.email_address
}