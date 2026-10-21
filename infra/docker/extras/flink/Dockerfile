# DEV ONLY: single-container Flink 1.20 mini cluster running services/processing. Build context: repository root.
#   docker build -f infra/docker/extras/flink/Dockerfile -t astryon/processing-dev .
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /w
COPY services/processing/pom.xml .
RUN mvn -q -B dependency:go-offline
COPY services/processing/src src
RUN mvn -q -B package -DskipTests

FROM flink:1.20.3-java17
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop && cp /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/s3-fs-hadoop/
COPY --from=build /w/target/processing-0.1.0-SNAPSHOT.jar /opt/flink/usrlib/processing.jar
COPY infra/docker/extras/flink/entrypoint.sh /entrypoint-processing.sh
ENTRYPOINT ["/entrypoint-processing.sh"]
