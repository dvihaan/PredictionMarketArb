#!/usr/bin/env bash
# Day-to-day operations for the recorder, from your own computer (AWS CLI with credentials).
#
#   infra/ops.sh status                     recorder state, timers, recent log, alarms, last heartbeat
#   infra/ops.sh logs [unit] [lines]        recent log lines; unit = recorder (default), nightly or refresh
#   infra/ops.sh stop | start | restart     control the recorder service (stop = no order-book data until start)
#   infra/ops.sh refresh                    rebuild today's clean tables now (normally every 3 hours)
#   infra/ops.sh alarms                     state of every project alarm
#   infra/ops.sh test-alarm [name]          force an alarm to ALARM to test the email path, no outage needed;
#                                           it returns to OK on its next evaluation (default: recorder-down)
#   infra/ops.sh shell                      interactive shell on the instance (needs the Session Manager plugin)
#
# Optional environment: REGION (default us-east-2), STACK (default prediction-market-arb).
set -euo pipefail
REGION="${REGION:-us-east-2}"
STACK="${STACK:-prediction-market-arb}"
PREFIX=prediction-market-arb

instance() {
  aws cloudformation describe-stacks --region "$REGION" --stack-name "$STACK" \
    --query "Stacks[0].Outputs[?OutputKey=='RecorderInstanceId'].OutputValue" --output text
}

# Run a shell command on the instance through SSM Run Command and print its output.
remote() {
  local id cmd_id params
  id="$(instance)"
  params="$(python3 -c 'import json, sys; print(json.dumps({"commands": [sys.argv[1]]}))' "$1")"
  cmd_id="$(aws ssm send-command --region "$REGION" --instance-ids "$id" --document-name AWS-RunShellScript \
    --comment "ops.sh: ${2:-command}" --parameters "$params" --query Command.CommandId --output text)"
  aws ssm wait command-executed --region "$REGION" --command-id "$cmd_id" --instance-id "$id" 2>/dev/null || true
  aws ssm get-command-invocation --region "$REGION" --command-id "$cmd_id" --instance-id "$id" \
    --query "[StandardOutputContent, StandardErrorContent]" --output text
}

alarms() {
  aws cloudwatch describe-alarms --region "$REGION" --alarm-name-prefix "$PREFIX" \
    --query "MetricAlarms[].[StateValue, AlarmName]" --output text | sed "s/$PREFIX-//"
}

unit_name() {
  case "${1:-recorder}" in
    recorder) echo pma-recorder ;;
    nightly)  echo pma-compact ;;
    refresh)  echo pma-refresh ;;
    *) echo "unknown unit '$1' (recorder, nightly or refresh)" >&2; exit 2 ;;
  esac
}

case "${1:-status}" in
  status)
    remote 'echo "recorder: $(systemctl is-active pma-recorder) since $(systemctl show pma-recorder -p ActiveEnterTimestamp --value)"
            echo "code:     $(git -C /opt/pma/app log -1 --format="%h %s" | cut -c1-70)"
            systemctl list-timers "pma-*" --no-pager | head -3
            echo; journalctl -u pma-recorder -n 3 --no-pager -o cat' status
    echo "alarms:"; alarms | sed 's/^/  /'
    echo "last heartbeat: $(aws cloudwatch get-metric-statistics --region "$REGION" --namespace PredictionMarketArb \
      --metric-name Heartbeat --start-time "$(python3 -c 'import datetime as d; print((d.datetime.now(d.timezone.utc) - d.timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%SZ"))')" \
      --end-time "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --period 60 --statistics Sum \
      --query 'sort_by(Datapoints,&Timestamp)[-1].Timestamp' --output text)"
    ;;
  logs)
    u="$(unit_name "${2:-recorder}")"
    remote "journalctl -u $u -n ${3:-30} --no-pager -o cat" "logs $u"
    ;;
  stop|start|restart)
    remote "systemctl $1 pma-recorder; sleep 3; echo recorder: \$(systemctl is-active pma-recorder)" "$1 recorder"
    ;;
  refresh)
    remote 'systemctl start pma-refresh; echo "refresh: $(systemctl show pma-refresh -p Result --value)"; journalctl -u pma-refresh -n 3 --no-pager -o cat' refresh
    ;;
  alarms)
    alarms
    ;;
  test-alarm)
    name="$PREFIX-${2:-recorder-down}"
    aws cloudwatch set-alarm-state --region "$REGION" --alarm-name "$name" --state-value ALARM \
      --state-reason "Manual test from infra/ops.sh"
    echo "$name set to ALARM; an email goes to confirmed subscribers. It returns to OK on its next evaluation."
    ;;
  shell)
    aws ssm start-session --region "$REGION" --target "$(instance)"
    ;;
  *)
    sed -n '2,13p' "$0"; exit 2
    ;;
esac
