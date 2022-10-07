FROM quay.io/astronomer/ap-airflow:2.4.1-onbuild
ENV AIRFLOW__CORE__PARALLELISM=1
ENV AIRFLOW__CORE__DAG_CONCURRENCY=1

# USER root
# # Install OpenJDK-8
# RUN apt-get update && \
#     apt-get install -y default-jdk

# Setup JAVA_HOME -- useful for docker commandline
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64
RUN export JAVA_HOME

ENV SPARK_HOME /host_spark
RUN export SPARK_HOME