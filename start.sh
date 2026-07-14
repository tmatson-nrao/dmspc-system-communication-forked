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

weed s3 -config=/tmp/identity.json
