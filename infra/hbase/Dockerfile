FROM eclipse-temurin:11-jre-jammy

ARG HBASE_VERSION=2.5.10
ENV HBASE_HOME=/opt/hbase
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="${HBASE_HOME}/bin:${PATH}"

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl bash procps \
    && rm -rf /var/lib/apt/lists/*

# archive.apache.org can be slow and drop connections, so resume and retry.
RUN curl -fSL --retry 20 --retry-delay 5 --retry-all-errors -C -         -o /tmp/hbase.tar.gz         "https://archive.apache.org/dist/hbase/${HBASE_VERSION}/hbase-${HBASE_VERSION}-bin.tar.gz"     && tar -xzf /tmp/hbase.tar.gz -C /opt     && mv "/opt/hbase-${HBASE_VERSION}" "${HBASE_HOME}"     && rm /tmp/hbase.tar.gz

COPY hbase-site.xml ${HBASE_HOME}/conf/hbase-site.xml
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh && mkdir -p /data

EXPOSE 16010 9090 9095
ENTRYPOINT ["/entrypoint.sh"]
