FROM apache/iceberg-rest:1.4.2

USER root

# Crée le dossier des jars s’il n’existe pas
RUN mkdir -p /opt/iceberg/jars

# Ajoute les JARs Hadoop S3 nécessaires
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar /opt/iceberg/jars/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar /opt/iceberg/jars/
