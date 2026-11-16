#!/usr/bin/env bash
# Update the recorder's code on the running instance: pull the configured branch from GitHub and
# re-run setup (which restarts the recorder). Push your commits to GitHub first.
#
#   infra/update.sh
#
# Optional environment: REGION (default us-east-2), STACK (default prediction-market-arb).
set -euo pipefail
REGION="${REGION:-us-east-2}"
STACK="${STACK:-prediction-market-arb}"
INSTANCE="$(aws cloudformation describe-stacks --region "$REGION" --stack-name "$STACK" \
  --query "Stacks[0].Outputs[?OutputKey=='RecorderInstanceId'].OutputValue" --output text)"

echo "== Running pma-update on $INSTANCE"
CMD_ID="$(aws ssm send-command --region "$REGION" --instance-ids "$INSTANCE" \
  --document-name AWS-RunShellScript --comment "pma-update" \
  --parameters 'commands=["/usr/local/bin/pma-update"]' --query Command.CommandId --output text)"
aws ssm wait command-executed --region "$REGION" --command-id "$CMD_ID" --instance-id "$INSTANCE" || true
aws ssm get-command-invocation --region "$REGION" --command-id "$CMD_ID" --instance-id "$INSTANCE" \
  --query "[Status, StandardOutputContent, StandardErrorContent]" --output text | tail -25
