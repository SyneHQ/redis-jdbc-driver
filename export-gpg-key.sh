#!/bin/bash

# GPG Key Export Script for GitHub Actions Setup
# This script helps export your GPG private key for GitHub Actions secrets

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}GPG Key Export Script for GitHub Actions${NC}"
echo "=============================================="

# Check if GPG is installed
if ! command -v gpg &> /dev/null; then
    echo -e "${RED}Error: GPG is not installed. Please install GPG first.${NC}"
    exit 1
fi

# Get the key ID from Maven settings or prompt user
KEY_ID="9EE631E110B9B429"  # Default from your settings.xml

echo -e "${YELLOW}Using GPG Key ID: $KEY_ID${NC}"
echo "If this is incorrect, please modify the script or export manually."

# Check if the key exists
if ! gpg --list-secret-keys --keyid-format LONG | grep -q "$KEY_ID"; then
    echo -e "${RED}Error: GPG key $KEY_ID not found in your keyring.${NC}"
    echo "Available secret keys:"
    gpg --list-secret-keys --keyid-format LONG
    exit 1
fi

echo -e "${GREEN}✓ GPG key $KEY_ID found${NC}"

# Export the private key
echo -e "${YELLOW}Exporting GPG private key...${NC}"
gpg --armor --export-secret-keys "$KEY_ID" > gpg-private-key.asc

if [ -f "gpg-private-key.asc" ]; then
    echo -e "${GREEN}✓ Private key exported to gpg-private-key.asc${NC}"
    echo ""
    echo -e "${YELLOW}Next steps:${NC}"
    echo "1. Copy the content of gpg-private-key.asc (including BEGIN and END lines)"
    echo "2. Go to your GitHub repository → Settings → Secrets and variables → Actions"
    echo "3. Add a new secret named 'GPG_PRIVATE_KEY' with the copied content"
    echo "4. Add another secret named 'GPG_PASSPHRASE' with your GPG passphrase"
    echo "5. Add secrets for Maven Central credentials:"
    echo "   - MAVEN_CENTRAL_USERNAME: gPQby2"
    echo "   - MAVEN_CENTRAL_PASSWORD: rneorR4NZZT2eBgi2q859iwFFfbcabg14"
    echo ""
    echo -e "${GREEN}Private key content:${NC}"
    echo "----------------------------------------"
    cat gpg-private-key.asc
    echo "----------------------------------------"
    echo ""
    echo -e "${RED}⚠️  SECURITY WARNING:${NC}"
    echo "This file contains your private key. Delete it after copying to GitHub Secrets:"
    echo "rm gpg-private-key.asc"
else
    echo -e "${RED}Error: Failed to export private key${NC}"
    exit 1
fi
