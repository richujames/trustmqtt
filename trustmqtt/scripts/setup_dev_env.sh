#!/usr/bin/env bash
set -euo pipefail

OS_NAME="$(uname -s)"
PYTHON_CMD=""

if [ "$OS_NAME" = "Linux" ]; then
  echo "Detected Linux. Installing system dependencies..."
  sudo apt-get update
  sudo apt-get install -y build-essential cmake libhiredis-dev libssl-dev pkg-config python3 python3-venv python3-pip git
  PYTHON_CMD="python3"
elif [ "$OS_NAME" = "Darwin" ]; then
  echo "Detected macOS. Installing system dependencies if Homebrew is available..."
  if command -v brew >/dev/null 2>&1; then
    brew install cmake hiredis openssl pkg-config python@3 git
    PYTHON_CMD="python3"
  else
    echo "Homebrew not found. Please install Homebrew and the required packages manually."
    PYTHON_CMD="python3"
  fi
else
  echo "Detected non-Linux system: $OS_NAME"
  echo "Skipping package manager install. Ensure Python 3 and Git are installed."
  if command -v python3 >/dev/null 2>&1; then
    PYTHON_CMD="python3"
  elif command -v python >/dev/null 2>&1; then
    PYTHON_CMD="python"
  else
    echo "Python executable not found. Please install Python 3 and rerun this script."
    exit 1
  fi
fi

if ! command -v "$PYTHON_CMD" >/dev/null 2>&1; then
  echo "Python not found: $PYTHON_CMD"
  exit 1
fi

# Python env for scoring-worker
"$PYTHON_CMD" -m venv venv

if [ "$OS_NAME" = "Windows_NT" ] || [ "$OS_NAME" = "MINGW64_NT-"* ] || [ "$OS_NAME" = "MSYS_NT-"* ]; then
  ACTIVATION_SCRIPT="venv/Scripts/activate"
else
  ACTIVATION_SCRIPT="venv/bin/activate"
fi

if [ -f "$ACTIVATION_SCRIPT" ]; then
  source "$ACTIVATION_SCRIPT"
else
  echo "Activation script not found: $ACTIVATION_SCRIPT"
  echo "Please activate the venv manually."
fi

pip install --upgrade pip
pip install -r scoring-worker/requirements.txt

echo "Dev environment prepared."
echo "Run 'source $ACTIVATION_SCRIPT' if the environment is not already activated."
