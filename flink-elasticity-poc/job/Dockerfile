FROM maven:3.9.16-eclipse-temurin-21 AS builder
WORKDIR /workspace
COPY pom.xml .
COPY src ./src
RUN mvn -q -DskipTests package

FROM flink:2.2.1-scala_2.12-java21
WORKDIR /opt/flink/usrlib
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop && \
	ln -sf /opt/flink/opt/flink-s3-fs-hadoop-2.2.1.jar /opt/flink/plugins/s3-fs-hadoop/flink-s3-fs-hadoop-2.2.1.jar
RUN mv /docker-entrypoint.sh /docker-entrypoint-original.sh
COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh
COPY --from=builder /workspace/target/flink-elastic-job-0.1.0.jar /opt/flink/usrlib/flink-elastic-job.jar
