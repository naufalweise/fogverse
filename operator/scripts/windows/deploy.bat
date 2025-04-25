@echo off
SETLOCAL

REM Check if the virtual environment directory exists
IF NOT EXIST ".env-operator" (
    echo Creating virtual environment in .env-operator...
    python -m venv .env-operator
)

REM Activate the virtual environment
CALL .env-operator\Scripts\activate.bat

REM Install dependencies
pip install -r requirements.txt

REM Run the Python script
python scripts\create-cluster.py --config examples\cluster-config.yaml

REM deploy kubernetes
kubectl apply -f out\deployments.yaml -n kafka


ENDLOCAL
