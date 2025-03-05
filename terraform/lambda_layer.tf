#################### zips the lambdas ##############################################

data "archive_file" "extract_lambda" {
  type        = "zip"
  source_file = "${path.module}/../src/extract_lambda.py"
  output_path = "${path.module}/../deploy/lambdas/extract_lambda.zip"
}

data "archive_file" "transform_lambda" {
  type        = "zip"
  source_file = "${path.module}/../src/transform_lambda.py"
  output_path = "${path.module}/../deploy/lambdas/transform_lambda.zip"
}

####################### zips the layer folders #########################################

data "archive_file" "dependencieslayer1" {
  type        = "zip"
  source_dir  = "${path.module}/../deploy/layers/unziped/dependencies1"
  output_path = "${path.module}/../deploy/layers/zipped/dependencieslayer1/dependencieslayer1.zip"
}

data "archive_file" "dependencieslayer2" {
  type        = "zip"
  source_dir  = "${path.module}/../deploy/layers/unziped/dependencies2"
  output_path = "${path.module}/../deploy/layers/zipped/dependencieslayer2/dependencieslayer2.zip"
}

# data "archive_file" "dependencieslayer3" {
#   type        = "zip"
#   source_dir  = "${path.module}/../deploy/layers/unziped/dependencies3"
#   output_path = "${path.module}/../deploy/layers/zipped/dependencieslayer3/dependencieslayer3.zip"
# }

data "archive_file" "utilslayer" {
  type        = "zip"
  source_dir = "${path.module}/../utils"
  output_path = "${path.module}/../deploy/layers/zipped/utilslayer/utilslayer.zip"
}

###################################### make layers ######################################################

resource "aws_lambda_layer_version" "utilslayerversion"{
  layer_name = "utilslayer"
  filename = data.archive_file.utilslayer.output_path
  compatible_runtimes = ["python3.12"]
  depends_on = [null_resource.force_redeploy]
}

resource "aws_lambda_layer_version" "dependencieslayer1version" {
  layer_name = "dependencieslayer1"
  filename = data.archive_file.dependencieslayer1.output_path
  compatible_runtimes = ["python3.12"]
  depends_on = [null_resource.force_redeploy]
}

resource "aws_lambda_layer_version" "dependencieslayer2version" {
  layer_name = "dependencieslayer2"
  filename = data.archive_file.dependencieslayer2.output_path
  compatible_runtimes = ["python3.12"]
  depends_on = [null_resource.force_redeploy]
}

# resource "aws_lambda_layer_version" "dependencieslayer3version" {
#   layer_name = "dependencieslayer3"
#   filename = data.archive_file.dependencieslayer3.output_path
#   compatible_runtimes = ["python3.12"]
#   depends_on = [null_resource.force_redeploy]
# }

############### triggers to remake  lambdas and layers ###########################################
resource "null_resource" "force_redeploy" {
  triggers = {
    always_run = timestamp()
  }
}