#!/bin/bash
set -e

# Wait for database to be ready
echo "Waiting for MariaDB to be ready..."
while ! nc -z mariadb 3306; do
  sleep 1
done
echo "MariaDB is ready"

# Configure Hadoop for S3
cat > ${HADOOP_HOME}/etc/hadoop/core-site.xml << EOF
<configuration>
    <property>
        <name>fs.s3a.endpoint</name>
        <value>http://minio:9000</value>
    </property>
    <property>
        <name>fs.s3a.access.key</name>
        <value>minioadmin</value>
    </property>
    <property>
        <name>fs.s3a.secret.key</name>
        <value>minioadmin</value>
    </property>
    <property>
        <name>fs.s3a.path.style.access</name>
        <value>true</value>
    </property>
    <property>
        <name>fs.s3a.impl</name>
        <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
    </property>
</configuration>
EOF

# Initialize schema if needed
echo "Checking if Hive schema needs to be initialized..."
$HIVE_HOME/bin/schematool -dbType mysql -info

if [ $? -ne 0 ]; then
  echo "Initializing Hive schema..."
  $HIVE_HOME/bin/schematool -dbType mysql -initSchema
  if [ $? -ne 0 ]; then
    echo "Schema initialization failed!"
    exit 1
  fi
  echo "Schema initialized successfully"
fi

# Start Hive Metastore
echo "Starting Hive Metastore server..."
$HIVE_HOME/bin/hive --service metastore
