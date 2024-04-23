#!/bin/bash --login
# The --login ensures the bash configuration is loaded,
# enabling Conda.

# Enable strict mode.
set -euo pipefail
# ... Run whatever commands ...

# Temporarily disable strict mode and activate conda:
set +euo pipefail
conda activate dicom-router

# Replace the variables in the router.conf file
sed -i "s/\$ORGANIZATION_ID/$ORG_ID/g" router.conf
sed -i "s/\$CLIENT_KEY/$CLIENT/g" router.conf
sed -i "s/\$SECRET_KEY/$SECRET/g" router.conf
sed -i "s|\\\$API_URL|$URL|g" router.conf

# Re-enable strict mode:
set -euo pipefail

# exec the final command:
exec python /app/main.py
