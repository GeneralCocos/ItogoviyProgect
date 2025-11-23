FROM confluentinc/cp-kafka-connect:7.9.0 AS avroconv
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-avro-converter:7.9.0

FROM debezium/connect:2.6

USER root
RUN microdnf install -y unzip && microdnf clean all

ENV KAFKA_CONNECT_PLUGINS_DIR=/kafka/connect

COPY --from=avroconv /usr/share/confluent-hub-components/confluentinc-kafka-connect-avro-converter/ \
     $KAFKA_CONNECT_PLUGINS_DIR/confluentinc-kafka-connect-avro-converter/

COPY plugins/debezium-connector-postgres-3.3.0.Final-plugin.tar.gz /tmp/
COPY plugins/debezium-connector-sqlserver-3.3.0.Final-plugin.tar.gz /tmp/
COPY plugins/debezium-connector-oracle-3.3.0.Final-plugin.tar.gz /tmp/
COPY plugins/confluentinc-kafka-connect-jdbc-10.8.4.zip /tmp/

RUN set -e \
 && mkdir -p "$KAFKA_CONNECT_PLUGINS_DIR/debezium-connector-postgres" \
 && tar -xzf /tmp/debezium-connector-postgres-3.3.0.Final-plugin.tar.gz \
        -C "$KAFKA_CONNECT_PLUGINS_DIR/debezium-connector-postgres" --strip-components=1 \
 && mkdir -p "$KAFKA_CONNECT_PLUGINS_DIR/debezium-connector-sqlserver" \
 && tar -xzf /tmp/debezium-connector-sqlserver-3.3.0.Final-plugin.tar.gz \
        -C "$KAFKA_CONNECT_PLUGINS_DIR/debezium-connector-sqlserver" --strip-components=1 \
 && mkdir -p "$KAFKA_CONNECT_PLUGINS_DIR/debezium-connector-oracle" \
 && tar -xzf /tmp/debezium-connector-oracle-3.3.0.Final-plugin.tar.gz \
        -C "$KAFKA_CONNECT_PLUGINS_DIR/debezium-connector-oracle" --strip-components=1 \
 && mkdir -p /tmp/jdbc \
 && unzip -q /tmp/confluentinc-kafka-connect-jdbc-10.8.4.zip -d /tmp/jdbc \
 && JDBC_DIR="$(find /tmp/jdbc -maxdepth 1 -type d -name 'confluentinc-kafka-connect-jdbc*' | head -n1)" \
 && mkdir -p "$KAFKA_CONNECT_PLUGINS_DIR/kafka-connect-jdbc" \
 && cp -r "$JDBC_DIR"/* "$KAFKA_CONNECT_PLUGINS_DIR/kafka-connect-jdbc/" \
 && rm -rf /tmp/*

USER 1001
