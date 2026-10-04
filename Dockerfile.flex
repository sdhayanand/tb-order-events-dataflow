# Dataflow Flex Template launcher image.
#
# One image serves both templates: the main class is chosen at build time with --build-arg so the
# same bundled jar is reused.  Normally scripts/build-template.sh lets `gcloud dataflow flex-template
# build --jar ...` produce this image through Cloud Build; this Dockerfile is for people who prefer
# building/pushing the launcher image themselves:
#
#   mvn -Pdataflow -DskipTests package
#   docker build -f Dockerfile.flex \
#     --build-arg MAIN_CLASS=com.tailoredbrands.otd.dataflow.OrderEventsStreamingPipeline \
#     -t us-central1-docker.pkg.dev/$PROJECT/tb-otd/dataflow-order-events-streaming:latest .
#   docker push ...
#   gcloud dataflow flex-template build gs://$PROJECT-tb-otd-dataflow/templates/order-events-streaming.json \
#     --image us-central1-docker.pkg.dev/$PROJECT/tb-otd/dataflow-order-events-streaming:latest \
#     --sdk-language JAVA --metadata-file metadata/order-events-streaming-metadata.json
FROM gcr.io/dataflow-templates-base/java21-template-launcher-base:latest

ARG MAIN_CLASS=com.tailoredbrands.otd.dataflow.OrderEventsStreamingPipeline
ARG JAR=target/tb-order-events-dataflow-1.0.0-bundled.jar

ENV FLEX_TEMPLATE_JAVA_MAIN_CLASS=${MAIN_CLASS}
ENV FLEX_TEMPLATE_JAVA_CLASSPATH=/template/pipeline.jar

COPY ${JAR} /template/pipeline.jar

# The base image already defines the launcher entrypoint (/opt/google/dataflow/java_template_launcher).
