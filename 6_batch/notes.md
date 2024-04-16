# notes

note the java version that's needed

Spark runs on Java 8/11/17, Scala 2.12/2.13, Python 3.8+, and R 3.5+. [link](https://spark.apache.org/docs/latest/#downloading)

note the need for a few export statements

```
export JAVA_HOME="/workspaces/LKDEZoomcamp2024/spark/jdk-11.0.2"
export PATH="${JAVA_HOME}/bin:${PATH}"

export SPARK_HOME="/workspaces/LKDEZoomcamp2024/spark/spark-3.3.2-bin-hadoop3"
export PATH="${SPARK_HOME}/bin:${PATH}"

export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH"
```

notes

commands for .py files

```
python 4_spark_local_pq_test.py \
    --input_green='data/pq/green/*/*' \
    --input_yellow='data/pq/yellow/*/*' \
    --output='data/report/revenue/'
```

```
URL="spark://codespaces-d61839:7077"

spark-submit \
    --master="${URL}" \
    4_spark_local_pq_test.py \
        --input_green=data/pq/green/*/*/ \
        --input_yellow=data/pq/yellow/*/*/ \
        --output=data/report-2020
```