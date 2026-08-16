#!/usr/bin/env bash
set +x
set -euo pipefail

: "${servicePrincipalId:?service principal ID is unavailable}"
: "${idToken:?service principal ID token is unavailable}"
: "${tenantId:?tenant ID is unavailable}"
: "${AZURESUBSCRIPTION_SERVICE_CONNECTION_ID:?service connection ID is unavailable}"
: "${SYSTEM_OIDCREQUESTURI:?OIDC request URI is unavailable}"
: "${SYSTEM_ACCESSTOKEN:?pipeline access token is unavailable}"

oidc_context=$(
  jq -cn \
    --arg request_uri "$SYSTEM_OIDCREQUESTURI" \
    --arg request_token "$SYSTEM_ACCESSTOKEN" \
    --arg service_connection_id "$AZURESUBSCRIPTION_SERVICE_CONNECTION_ID" \
    --arg client_id "$servicePrincipalId" \
    --arg tenant_id "$tenantId" \
    '{
      request_uri: $request_uri,
      request_token: $request_token,
      service_connection_id: $service_connection_id,
      client_id: $client_id,
      tenant_id: $tenant_id
    }' |
    base64 --wrap=0
)

printf '##vso[task.setvariable variable=SP_CLIENT_ID;issecret=true]%s\n' \
  "$servicePrincipalId"
printf '##vso[task.setvariable variable=SP_ID_TOKEN;issecret=true]%s\n' \
  "$idToken"
printf '##vso[task.setvariable variable=TENANT_ID;issecret=true]%s\n' \
  "$tenantId"
printf '##vso[task.setvariable variable=AZURE_OIDC_CONTEXT_B64;issecret=true]%s\n' \
  "$oidc_context"
