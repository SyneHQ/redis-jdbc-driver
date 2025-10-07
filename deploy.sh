#!/bin/bash

# Redis JDBC Driver Deployment Script
# Make sure you have configured ~/.m2/settings.xml with your Sonatype credentials

set -e

echo "🚀 Redis JDBC Driver Deployment Script"
echo "======================================"

# Check if Maven settings exist
if [ ! -f ~/.m2/settings.xml ]; then
    echo "❌ Maven settings file not found at ~/.m2/settings.xml"
    echo "Please configure your Sonatype credentials first. See SETUP_INSTRUCTIONS.md"
    exit 1
fi

# Check if GPG is available
if ! command -v gpg &> /dev/null; then
    echo "❌ GPG not found. Please install GPG for artifact signing."
    exit 1
fi

# Check if GPG keys exist
if ! gpg --list-secret-keys --keyid-format LONG | grep -q sec; then
    echo "❌ No GPG keys found. Please generate a GPG key first."
    echo "Run: gpg --full-generate-key"
    exit 1
fi

echo "✅ Prerequisites check passed"

# Function to deploy snapshot
deploy_snapshot() {
    echo ""
    echo "📦 Deploying SNAPSHOT version..."
    
    # Update version to snapshot
    sed -i.bak 's/<version>1\.0\.0<\/version>/<version>1.0.0-SNAPSHOT<\/version>/' pom.xml
    
    # Deploy snapshot
    mvn clean deploy
    
    echo "✅ Snapshot deployed successfully!"
    echo "Check: https://oss.sonatype.org/content/repositories/snapshots/com/synehq/jdbc/"
}

# Function to deploy release
deploy_release() {
    echo ""
    echo "🎯 Deploying RELEASE version..."
    
    # Restore original version
    sed -i.bak 's/<version>1\.0\.0-SNAPSHOT<\/version>/<version>1.0.0<\/version>/' pom.xml
    
    # Deploy release with GPG signing
    mvn clean deploy -Prelease
    
    echo "✅ Release deployed to staging repository!"
    echo ""
    echo "📋 Next steps:"
    echo "1. Go to https://oss.sonatype.org/"
    echo "2. Login with your credentials"
    echo "3. Go to 'Staging Repositories'"
    echo "4. Find your repository (com.synehq.jdbc-xxxx)"
    echo "5. Click 'Close' to validate artifacts"
    echo "6. After successful close, click 'Release' to publish to Maven Central"
    echo ""
    echo "🔍 Verify publication at: https://search.maven.org/"
}

# Function to clean up backup files
cleanup() {
    rm -f pom.xml.bak
}

# Main menu
echo ""
echo "What would you like to do?"
echo "1) Deploy SNAPSHOT version (for testing)"
echo "2) Deploy RELEASE version (for production)"
echo "3) Exit"
echo ""
read -p "Enter your choice (1-3) [1]: " choice
# Clean up any carriage return characters
choice=$(echo "$choice" | tr -d '\r\n')
# Set default if empty
choice=${choice:-1}

case $choice in
    1)
        deploy_snapshot
        cleanup
        ;;
    2)
        deploy_release
        cleanup
        ;;
    3)
        echo "👋 Goodbye!"
        exit 0
        ;;
    *)
        echo "❌ Invalid choice. Please run the script again."
        exit 1
        ;;
esac
