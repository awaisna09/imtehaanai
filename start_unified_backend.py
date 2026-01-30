#!/usr/bin/env python3
"""
Unified Backend Service Startup Script
This script starts the unified backend combining AI Tutor and
Grading API on port 8000
"""

import os
import sys
from pathlib import Path

# Fix Windows console encoding for emoji support
if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except AttributeError:
        # Python < 3.7 doesn't have reconfigure
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')


def check_python_version():
    """Check if Python version is compatible"""
    if sys.version_info < (3, 8):
        print("❌ Error: Python 3.8 or higher is required")
        print(f"Current version: {sys.version}")
        sys.exit(1)
    print(f"✅ Python version: {sys.version.split()[0]}")


def check_dependencies():
    """Check if required packages are installed"""
    required_packages = [
        'fastapi',
        'uvicorn',
        'langchain',
        'langchain-openai',
        'openai',
        'dotenv',
        'pydantic'
    ]

    missing_packages = []

    for package in required_packages:
        try:
            __import__(package.replace('-', '_'))
            print(f"✅ {package}")
        except ImportError:
            missing_packages.append(package)
            print(f"❌ {package}")

    if missing_packages:
        print(f"\n❌ Missing packages: {', '.join(missing_packages)}")
        print("Please install them using: pip install -r requirements.txt")
        return False

    return True


def check_env_file():
    """Check if config.env file exists and has required variables"""
    # In production (Railway), environment variables are set directly, not via file
    environment = os.getenv("ENVIRONMENT", "development").lower()
    
    # In production, config.env is optional (Railway uses environment variables)
    if environment == "production":
        # Check if required environment variables are set directly
        openai_key = os.getenv('OPENAI_API_KEY')
        if not openai_key:
            print("❌ ERROR: OPENAI_API_KEY not found in environment variables")
            print("Please set OPENAI_API_KEY in Railway environment variables")
            return False
        print("✅ Production mode: Using environment variables (config.env not required)")
        return True
    
    # In development, check for config.env file
    env_file = Path('config.env')
    if not env_file.exists():
        print("⚠️  WARNING: config.env file not found (development mode)")
        print("Please create a config.env file with your API keys:")
        print("OPENAI_API_KEY=your_key_here")
        print("LANGSMITH_API_KEY=your_key_here (optional)")
        print("Or set environment variables directly")
        # Don't fail in development - allow environment variables
        return True

    # Check if file has content
    if env_file.stat().st_size == 0:
        print("⚠️  WARNING: config.env file is empty")
        return True  # Don't fail, allow environment variables

    print("✅ config.env file found")
    return True


def load_env_vars():
    """Load and validate environment variables"""
    try:
        from dotenv import load_dotenv
        
        # In production (Railway), environment variables are already set
        # In development, try to load from config.env if it exists
        environment = os.getenv("ENVIRONMENT", "development").lower()
        
        if environment != "production":
            # Development mode: try to load config.env if it exists
            if os.path.exists('config.env'):
                load_dotenv('config.env')
                print("✅ Loaded config.env file (development mode)")
        else:
            print("✅ Production mode: Using Railway environment variables")

        # Check for required API key (from environment or config.env)
        openai_key = os.getenv('OPENAI_API_KEY')
        if not openai_key:
            print("❌ ERROR: OPENAI_API_KEY not found")
            if environment == "production":
                print("Please set OPENAI_API_KEY in Railway environment variables")
            else:
                print("Please set OPENAI_API_KEY in config.env or environment variables")
            return False

        # Check for optional LangSmith key
        langsmith_key = os.getenv('LANGSMITH_API_KEY')
        if langsmith_key:
            print("✅ LangSmith API key found")
        else:
            print("⚠️  LangSmith API key not found (optional)")

        print("✅ Environment variables loaded successfully")
        return True

    except ImportError:
        print("❌ ERROR: dotenv not installed")
        return False
    except Exception as e:
        print(f"❌ ERROR loading environment: {e}")
        return False


def start_service():
    """Start the unified backend service"""
    print("\n🚀 Starting Unified Backend Service...")

    try:
        # Try to use uvloop for faster async performance (10-20% improvement)
        try:
            import uvloop
            uvloop.install()
            print("✅ Using uvloop for faster async performance")
        except ImportError:
            print("⚠️  uvloop not installed (optional - install with: pip install uvloop)")
        
        # Import and run the service
        from unified_backend import app
        import uvicorn

        # Get port from environment or config
        # Railway provides PORT environment variable automatically
        from dotenv import load_dotenv
        # Try to load config.env if it exists (for local development)
        if os.path.exists('config.env'):
            load_dotenv('config.env')
        # Railway sets PORT automatically, fallback to 8000 for local dev
        port = int(os.getenv('PORT', os.getenv('API_PORT', '8000')))
        host = os.getenv('HOST', os.getenv('API_HOST', '0.0.0.0'))

        print("✅ Unified backend imported successfully")
        print(f"🌐 Starting server on http://{host}:{port}")
        print(f"📚 API Documentation: http://{host}:{port}/docs")
        print(f"🔍 Health Check: http://{host}:{port}/health")
        print(f"📊 Grading API: http://{host}:{port}/grade-answer")
        print(f"🤖 AI Tutor: http://{host}:{port}/tutor/chat")
        print("\nPress Ctrl+C to stop the service")

        # Enable auto-reload in development mode
        environment = os.getenv('ENVIRONMENT', 'development').lower()
        reload = environment == 'development'
        if reload:
            print("🔄 Auto-reload enabled (development mode)")
            # Use import string format for reload to work properly
            uvicorn.run("unified_backend:app", host=host, port=port, reload=reload)
        else:
            # Use app object directly when reload is disabled
            uvicorn.run(app, host=host, port=port, reload=reload)

    except ImportError as e:
        print(f"❌ Error importing unified backend: {e}")
        print("Please check that all dependencies are installed")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error starting service: {e}")
        sys.exit(1)


def main():
    """Main function"""
    print("🤖 Unified Backend Service Startup")
    print("=" * 50)
    print("This service combines:")
    print("  • AI Tutor Service")
    print("  • Answer Grading API")
    print("  • All on port 8000")
    print("=" * 50)

    # Check Python version
    check_python_version()

    # Check dependencies
    print("\n📦 Checking dependencies...")
    if not check_dependencies():
        sys.exit(1)

    # Check environment file
    print("\n🔧 Checking configuration...")
    check_env_file()

    # Load environment variables
    if not load_env_vars():
        sys.exit(1)

    # Start the service
    start_service()


if __name__ == "__main__":
    main()
