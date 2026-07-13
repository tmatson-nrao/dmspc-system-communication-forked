#!/bin/sh

# Detect if running on Render vs Local
if [ -n "$RENDER" ] || [ "$APP_ENV" = "demo" ]; then
    echo "--- Demo Cloud Environment Detected ---"
    S3_DOMAIN="${WEED_S3_DOMAIN:-system-communication-prototype.onrender.com}"

    # CRITICAL: On Render, force the S3 Gateway to use Render's dynamic $PORT
    if [ -n "$PORT" ]; then
        echo "Render $PORT detected: Overriding S3 port to $PORT"
        S3_PORT=$PORT
    else
        S3_PORT=${WEED_S3_PORT:-8333}
    fi
else
    echo "--- Local Development Environment Detected ---"
    S3_DOMAIN="localhost"
    S3_PORT=${WEED_S3_PORT:-8333} # Uses local .env variable or 8333 fallback
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
  -filer.port="$FILER_PORT"