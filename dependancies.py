import subprocess
import sys

def install_package(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

def install_playwright_browsers():
    subprocess.check_call([sys.executable, "-m", "playwright", "install"])

def main():
    # List of required packages
    packages = [
        "playwright",
        "pyotp",
        "requests",
        "pandas",
        "pytest-playwright"
    ]
    
    # Install all the required packages
    for package in packages:
        print(f"Installing {package}...")
        install_package(package)
    
    # Install Playwright browsers
    print("Installing Playwright browsers...")
    install_playwright_browsers()
    
    print("All dependencies installed successfully!")

if __name__ == "__main__":
    main()
