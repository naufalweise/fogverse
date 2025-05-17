# This Dockerfile is used to create a custom Kafka image with Jolokia for JMX monitoring.
# It uses the Confluent Kafka image as a base and adds a wrapper script to start Jolokia.
# The wrapper script is responsible for starting the Kafka server and the Jolokia agent.
# The Jolokia agent is used to expose JMX metrics over HTTP, which can be scraped by monitoring tools like Prometheus.

FROM confluentinc/cp-kafka:latest

USER root

COPY jolokia-wrapper.sh /usr/local/bin/jolokia-wrapper.sh
RUN chmod +x /usr/local/bin/jolokia-wrapper.sh

ENTRYPOINT ["/usr/local/bin/jolokia-wrapper.sh"]
