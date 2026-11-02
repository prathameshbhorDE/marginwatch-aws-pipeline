# The agent bank's OAuth login (client_id + client_secret), locked with our KMS key.
#
# Terraform creates the EMPTY secret only. The value is put in afterwards with the AWS CLI,
# because anything written in Terraform is also stored in plain text in the Terraform state file.
resource "aws_secretsmanager_secret" "agent_bank_oauth" {
  name                    = "marginwatch/${var.env}/agent-bank/oauth"
  description             = "Agent bank API OAuth2 client credentials"
  kms_key_id              = aws_kms_key.lake.arn
  recovery_window_in_days = 0 # practice account: delete at once on destroy (production: 7-30 days)
}
