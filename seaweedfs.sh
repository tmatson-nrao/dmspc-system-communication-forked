#!/bin/sh
set -e

cat > /tmp/identity.json <<EOF
{
  "identities": [
    {
      "name": "demo_user",
      "credentials": [
        {
          "accessKey": "${WEED_S3_ACCESS_KEY}",
          "secretKey": "${WEED_S3_SECRET_KEY}"
        }
      ],
      "actions": [
        "Admin",
        "Read",
        "Write",
        "List"
      ]
    }
  ]
}
EOF

# Detect if running on Render vs Local
if [ -n "$RENDER" ] || [ "$APP_ENV" = "demo" ]; then
    echo "--- Demo Cloud Environment Detected ---"
    # Fallback syntax uses :- instead of ://
    S3_DOMAIN="${WEED_S3_DOMAIN}" 
else
    echo "--- Local Development Environment Detected ---"
    S3_DOMAIN="${WEED_S3_DOMAIN}" 
fi

BIND_IP="0.0.0.0"
FILER_PORT=${WEED_FILER_PORT:-8888}

echo "Binding IP  : $BIND_IP"
echo "S3 Domain   : $S3_DOMAIN"
echo "S3 Port     : $S3_PORT"
echo "Filer Port  : $FILER_PORT"

# Boot SeaweedFS
# Changed -dir to /tmp for Render Free Tier to avoid permission errors without volumes
exec weed server \
  -ip="$BIND_IP" \
  -dir="/tmp" \
  -s3 \
  -s3.domainName="$S3_DOMAIN" \
  -s3.port="$S3_PORT" \
  -filer=true \
  -filer.port="$FILER_PORT" \
  -s3.config=/tmp/identity.json
