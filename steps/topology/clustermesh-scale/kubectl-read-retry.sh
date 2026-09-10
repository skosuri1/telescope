#!/usr/bin/env bash
set -uo pipefail

budget="${1:-}"
attempts="${CLUSTERMESH_KUBECTL_READ_ATTEMPTS:-5}"
retry_seconds="${CLUSTERMESH_KUBECTL_READ_RETRY_SECONDS:-5}"
request_timeout="${CLUSTERMESH_KUBECTL_READ_REQUEST_TIMEOUT_SECONDS:-30}"
command_timeout="${CLUSTERMESH_KUBECTL_READ_COMMAND_TIMEOUT_SECONDS:-45}"
if [ "$#" -lt 2 ] ||
   ! [[ "$budget" =~ ^[1-9][0-9]*$ ]] ||
   ! [[ "$attempts" =~ ^[1-9][0-9]*$ ]] ||
   ! [[ "$retry_seconds" =~ ^[0-9]+$ ]] ||
   ! [[ "$request_timeout" =~ ^[1-9][0-9]*$ ]] ||
   ! [[ "$command_timeout" =~ ^[1-9][0-9]*$ ]]; then
  echo "kubectl-read-retry: expected a positive budget and valid attempt/request/command timeouts and retry interval" >&2
  exit 2
fi
shift
retry_seconds=$((10#$retry_seconds))
case "$1" in
  get) ;;
  rollout)
    if [ "${2:-}" != "status" ]; then
      echo "kubectl-read-retry: only get and rollout status are allowed" >&2
      exit 2
    fi
    ;;
  *)
    echo "kubectl-read-retry: only get and rollout status are allowed" >&2
    exit 2
    ;;
esac

output_dir=$(mktemp -d) || exit 1
trap 'rm -f "$output_dir/stdout" "$output_dir/stderr"; rmdir "$output_dir"' EXIT
deadline=$((SECONDS + budget))

is_transient() {
  # Structural/auth failures and actual rollout non-readiness are not retries.
  if grep -Eiq \
      'Forbidden|Unauthorized|You must be logged in|x509:|invalid configuration|unknown flag|NotFound|not found|timed out waiting for the condition|exceeded its progress deadline' \
      "$output_dir/stdout" "$output_dir/stderr"; then
    return 1
  fi
  if [ "$rc" -eq 124 ]; then
    return 0
  fi
  grep -Eiq \
    'connection refused|the connection to the server .* was refused|connection reset by peer|i/o timeout|TLS handshake timeout|context deadline exceeded|Client.Timeout exceeded|net/http: request canceled|http2: client connection lost|server sent GOAWAY|unexpected EOF|(^|: )EOF$|ServiceUnavailable|TooManyRequests|ServerTimeout|BadGateway|GatewayTimeout|the server is currently unable to handle the request' \
    "$output_dir/stdout" "$output_dir/stderr"
}

for ((attempt=1; attempt<=attempts; attempt++)); do
  remaining=$((deadline - SECONDS))
  if [ "$remaining" -le 0 ]; then
    echo "kubectl-read-retry: ${KUBECONFIG:-current-context}: ${budget}s deadline exhausted" >&2
    exit 124
  fi
  request="$request_timeout"
  if [ "$request" -gt "$remaining" ]; then
    request="$remaining"
  fi
  limit="$remaining"
  rollout_args=()
  if [ "$1" = "rollout" ]; then
    # Every watch retry consumes the original deadline, never another 5m.
    request="$remaining"
    rollout_args=("--timeout=${remaining}s")
  elif [ "$limit" -gt "$command_timeout" ]; then
    limit="$command_timeout"
  fi
  rc=0
  timeout --signal=TERM --kill-after=5s "${limit}s" \
    kubectl --request-timeout="${request}s" "$@" "${rollout_args[@]}" \
      >"$output_dir/stdout" 2>"$output_dir/stderr" || rc=$?
  if [ "$rc" -eq 0 ] && [ "$SECONDS" -lt "$deadline" ]; then
    cat "$output_dir/stdout"
    cat "$output_dir/stderr" >&2
    exit 0
  fi
  # Failed attempts must not contaminate JSON captured by the caller.
  cat "$output_dir/stdout" "$output_dir/stderr" >&2
  if [ "$SECONDS" -ge "$deadline" ]; then
    echo "kubectl-read-retry: ${KUBECONFIG:-current-context}: ${budget}s deadline exhausted" >&2
    exit 124
  fi
  if [ "$attempt" -eq "$attempts" ] || ! is_transient; then
    echo "kubectl-read-retry: ${KUBECONFIG:-current-context}: failed rc=${rc} after ${attempt} attempt(s)" >&2
    exit "$rc"
  fi
  remaining=$((deadline - SECONDS))
  pause="$retry_seconds"
  if [ "$pause" -gt "$remaining" ]; then
    pause="$remaining"
  fi
  echo "kubectl-read-retry: ${KUBECONFIG:-current-context}: transient API error on attempt ${attempt}/${attempts}; retrying in ${pause}s" >&2
  if [ "$pause" -gt 0 ]; then
    sleep "$pause"
  fi
done
