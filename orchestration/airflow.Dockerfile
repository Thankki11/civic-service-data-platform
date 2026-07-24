# Airflow mo rong: bo sung provider Spark/Trino + binary spark-submit (pyspark)
# + JRE de submit toi Spark standalone, va thu vien cho plugin custom
# (KeycloakHook/CustomSparkSubmitOperator dung requests + pyjwt) va buoc
# seed_and_sync_xml (boto3/lxml/faker).
FROM apache/airflow:slim-2.10.2-python3.12

ARG AIRFLOW_VERSION=2.10.2
ARG PYTHON_VERSION=3.12
# Constraints giu apache-airflow o dung 2.10.2 va chon provider tuong thich
# -> tranh bi keo len airflow 3.x (loi "airflow: command not found").
ARG CONSTRAINT_URL=https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre curl procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    "apache-airflow-providers-apache-spark" \
    "apache-airflow-providers-trino" \
    pyspark pyjwt requests boto3 lxml Faker \
    --constraint "${CONSTRAINT_URL}"

# Driver Spark (pyspark) PHAI khop dung phien ban cluster (apache/spark:3.5.1),
# neu khong se loi InvalidClassException (serialVersionUID) khi submit client-mode.
# Constraints keo pyspark len 3.5.2 -> ep ve dung 3.5.1.
RUN pip install --no-cache-dir --force-reinstall --no-deps "pyspark==3.5.1"
RUN pip install --no-cache-dir "psycopg2-binary==2.9.9"

# Bake cac JAR cua moi pipeline vao driver Airflow. Pipeline Registry truyen
# local:///opt/... bang --jars; Spark worker image cung co dung path nay, nen
# khong tai Maven va khong upload lai JAR lon qua mang moi lan chay.
USER root
RUN mkdir -p /opt/spark/jars \
    && curl -fsSL \
      "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.2/iceberg-spark-runtime-3.5_2.12-1.5.2.jar" \
      -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.5.2.jar \
    && curl -fsSL \
      "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" \
      -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
    && curl -fsSL \
      "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar" \
      -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
    && curl -fsSL \
      "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar" \
      -o /opt/spark/jars/postgresql-42.6.0.jar \
    && curl -fsSL \
      "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar" \
      -o /opt/spark/jars/mysql-connector-j-8.0.33.jar \
    && chmod 0644 /opt/spark/jars/*.jar
USER airflow

# spark-submit di kem pyspark nhung khong nam tren PATH -> tao symlink de
# SparkSubmitOperator tim thay binary. SPARK_HOME giup hook dinh vi Spark.
RUN SPARK_DIR="$(python -c 'import os,pyspark;print(os.path.dirname(pyspark.__file__))')" \
    && ln -sf "$SPARK_DIR/bin/spark-submit" "$HOME/.local/bin/spark-submit"
ENV SPARK_HOME=/home/airflow/.local/lib/python3.12/site-packages/pyspark
ENV PATH="/home/airflow/.local/bin:${SPARK_HOME}/bin:${PATH}"
