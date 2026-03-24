FROM apache/spark:3.5.5-scala2.12-java17-python3-ubuntu

USER root
ENV PATH="/opt/spark/bin:/opt/bitnami/spark/bin:${PATH}"

COPY infra/spark/entrypoint.sh /opt/tf2-spark/entrypoint.sh
RUN chmod +x /opt/tf2-spark/entrypoint.sh

ENTRYPOINT ["/opt/tf2-spark/entrypoint.sh"]
