FROM confluentinc/cp-kafka:latest

USER root

COPY jolokia-wrapper.sh /usr/local/bin/jolokia-wrapper.sh
RUN chmod +x /usr/local/bin/jolokia-wrapper.sh

ENTRYPOINT ["/usr/local/bin/jolokia-wrapper.sh"]
