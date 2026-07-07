#!/bin/sh

# Default fallback values for local development
BIND_IP=${WEED_BIND_IP:-"0.0.0.0"}
S3_DOMAIN=${WEED_S3_DOMAIN:-"localhost"}

echo "Starting SeaweedFS Demo Cluster..."
echo "S3 Domain set to: $S3_DOMAIN"

# Run all-in-one SeaweedFS server
exec weed server \
  -ip=$BIND_IP \
  -dir=/data \
  -s3 \
  -s3.domainName=$S3_DOMAIN \
  -s3.port=8333 \
  -filer=true
