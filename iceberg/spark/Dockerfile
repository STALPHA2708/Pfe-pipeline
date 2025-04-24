FROM apache/spark:3.4.1

# ✅ Switch to root to install packages
USER root

# ✅ Add Iceberg + S3 JARs
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.4_2.12/1.4.2/iceberg-spark-runtime-3.4_2.12-1.4.2.jar /opt/spark/jars/

# ✅ Install Python + Jupyter + PySpark
RUN pip install --no-cache-dir pyspark==3.4.1 jupyter pandas matplotlib

# ✅ Run Jupyter on launch
CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--allow-root", "--NotebookApp.token=''", "--NotebookApp.password=''", "--NotebookApp.notebook_dir=/opt/workspace"]
