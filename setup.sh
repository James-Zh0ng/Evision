#!/bin/bash
echo "Setting up the environment..."

# Check for Python installation
if ! command -v python3 &> /dev/null; then
    echo "Error: Python3 not found. Please install Python3 and try again."
    exit 1
fi

# Install virtualenv and set up the virtual environment
pip3 install virtualenv
virtualenv venv

# Activate the virtual environment and install requirements
source venv/bin/activate
pip install -r requirements.txt

# Execute the Python script
python3 create_database.py

echo "Done!"
