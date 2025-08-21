FROM python:3.13-slim
WORKDIR /app
COPY autoscaler_operator.py .
RUN pip install kopf kubernetes pyyaml requests
CMD ["kopf", "run", "--standalone", "/app/autoscaler_operator.py"]
