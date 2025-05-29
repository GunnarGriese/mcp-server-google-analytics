#!/bin/bash

# Google Analytics MCP Server Setup Script

set -e

echo "🚀 Setting up Google Analytics MCP Server (Python)"
echo "=================================================="

# Check Python version
python_version=$(python3 --version 2>&1 | cut -d' ' -f2 | cut -d'.' -f1,2)
required_version="3.8"

if [ "$(printf '%s\n' "$required_version" "$python_version" | sort -V | head -n1)" != "$required_version" ]; then
    echo "❌ Python 3.8 or higher is required. Current version: $python_version"
    exit 1
fi

echo "✅ Python version check passed: $python_version"

# Create virtual environment
echo "📦 Creating virtual environment..."
python3 -m venv venv

# Activate virtual environment
echo "🔧 Activating virtual environment..."
source venv/bin/activate

# Upgrade pip
echo "⬆️ Upgrading pip..."
pip install --upgrade pip

# Install dependencies
echo "📥 Installing dependencies..."
pip install -r requirements.txt

echo ""
echo "✅ Setup complete!"
echo ""
echo "📋 Next steps:"
echo "1. Set your environment variables:"
echo "   export GOOGLE_CLIENT_EMAIL=\"your-service-account@project.iam.gserviceaccount.com\""
echo "   export GOOGLE_PRIVATE_KEY=\"your-private-key\""
echo "   export GA_PROPERTY_ID=\"your-ga4-property-id\""
echo ""
echo "2. Activate the virtual environment:"
echo "   source venv/bin/activate"
echo ""
echo "3. Run the server (option 1 - simple runner):"
echo "   python run_local.py"
echo ""
echo "4. Or run the server (option 2 - as module):"
echo "   python -m mcp_server_google_analytics"
echo ""
echo "5. Or install and run globally:"
echo "   pip install -e ."
echo "   mcp-server-google-analytics"
echo ""
echo "📖 For detailed setup instructions, see README.md"