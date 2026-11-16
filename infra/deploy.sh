#!/usr/bin/env bash
# Deploy (or update) the AWS stack, then upload the web pages.
#
#   ALERT_EMAILS="first@example.com,second@example.com" infra/deploy.sh
#
# Optional environment: BRANCH (default arch-redesign), REGION (default us-east-2), STACK (default
# prediction-market-arb). Needs the AWS CLI with credentials for the target account.
#
# The web password is created once in SSM Parameter Store (/prediction-market-arb/web/password) and
# never printed or stored in the repository; only its SHA-256 goes to CloudFormation. To read it:
#   aws ssm get-parameter --region us-east-2 --name /prediction-market-arb/web/password --with-decryption \
#     --query Parameter.Value --output text
set -euo pipefail

REGION="${REGION:-us-east-2}"
STACK="${STACK:-prediction-market-arb}"
BRANCH="${BRANCH:-arch-redesign}"
PW_PARAM=/prediction-market-arb/web/password
TAGS=(Project=prediction-market-arb environment=dev owner=binoy)
HERE="$(cd "$(dirname "$0")" && pwd)"
: "${ALERT_EMAILS:?Set ALERT_EMAILS to one or two comma-separated addresses}"
IFS=, read -r EMAIL1 EMAIL2 <<< "$ALERT_EMAILS"

echo "== Checking the Kalshi key parameters in $REGION"
for p in /prediction-market-arb/kalshi/key-id /prediction-market-arb/kalshi/private-key; do
  aws ssm describe-parameters --region "$REGION" --parameter-filters "Key=Name,Values=$p" \
    --query "Parameters[0].Name" --output text | grep -q "$p" || { echo "Missing $p in $REGION"; exit 1; }
done

echo "== Web password"
if ! aws ssm get-parameter --region "$REGION" --name "$PW_PARAM" >/dev/null 2>&1; then
  aws ssm put-parameter --region "$REGION" --name "$PW_PARAM" --type SecureString \
    --value "$(python3 -c 'import secrets; print(secrets.token_urlsafe(18))')" \
    --tags Key=Project,Value=prediction-market-arb Key=environment,Value=dev Key=owner,Value=binoy >/dev/null
  echo "   created a new password in $PW_PARAM"
else
  echo "   using the existing password in $PW_PARAM"
fi
HASH="$(aws ssm get-parameter --region "$REGION" --name "$PW_PARAM" --with-decryption --query Parameter.Value \
        --output text | tr -d '\n' | shasum -a 256 | cut -d' ' -f1)"

echo "== Deploying stack $STACK in $REGION (branch $BRANCH); a first deploy takes about 10 minutes"
aws cloudformation deploy --region "$REGION" --stack-name "$STACK" \
  --template-file "$HERE/stack.yaml" \
  --capabilities CAPABILITY_IAM \
  --no-fail-on-empty-changeset \
  --tags "${TAGS[@]}" \
  --parameter-overrides "AlertEmail1=$EMAIL1" "AlertEmail2=${EMAIL2:-}" "WebPasswordHash=$HASH" "RepoBranch=$BRANCH"

out() { aws cloudformation describe-stacks --region "$REGION" --stack-name "$STACK" \
          --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue" --output text; }
BUCKET="$(out DataBucketName)"
URL="$(out WebUrl)"

echo "== Uploading web pages to s3://$BUCKET/"
for f in index.html login.html; do
  aws s3 cp --region "$REGION" "$HERE/site/$f" "s3://$BUCKET/$f" --content-type "text/html; charset=utf-8" \
    --cache-control "no-cache" >/dev/null
done

echo
echo "Done."
echo "  Web address:   $URL"
echo "  Data bucket:   s3://$BUCKET"
echo "  Instance:      $(out RecorderInstanceId)  (shell: aws ssm start-session --region $REGION --target <id>)"
echo "  Alert emails:  each address must click the confirmation link AWS just sent."
