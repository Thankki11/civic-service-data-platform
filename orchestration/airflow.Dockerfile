# Airflow mo rong: bo sung provider Spark/Trino + binary spark-submit (pyspark)
# + JRE de submit toi Spark standalone, va thu vien cho plugin custom
# (KeycloakHook/CustomSparkSubmitOperator dung requests + pyjwt) va buoc
# seed_and_sync_xml (boto3/lxml/faker).
FROM apache/airflow:2.10.2

ARG AIRFLOW_VERSION=2.10.2
ARG PYTHON_VERSION=3.12
# Constraints giu apache-airflow o dung 2.10.2 va chon provider tuong thich
# -> tranh bi keo len airflow 3.x (loi "airflow: command not found").
ARG CONSTRAINT_URL=https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre \
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

# spark-submit di kem pyspark nhung khong nam tren PATH -> tao symlink de
# SparkSubmitOperator tim thay binary. SPARK_HOME giup hook dinh vi Spark.
RUN SPARK_DIR="$(python -c 'import os,pyspark;print(os.path.dirname(pyspark.__file__))')" \
    && ln -sf "$SPARK_DIR/bin/spark-submit" "$HOME/.local/bin/spark-submit"
ENV SPARK_HOME=/home/airflow/.local/lib/python3.12/site-packages/pyspark
ENV PATH="/home/airflow/.local/bin:${SPARK_HOME}/bin:${PATH}"
