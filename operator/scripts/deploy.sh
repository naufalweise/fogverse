# Check if the virtual environment directory exists
if [ ! -d ".env-operator" ]; then
  echo "Creating virtual environment in .env-operator..."
  python -m venv .env-operator
fi

source .env-operator/bin/activate
pip install -r requirements.txt
python scripts/create-cluster.py --config examples/cluster-config.yaml

kubectl apply -f out/deployments.yaml -n kafka