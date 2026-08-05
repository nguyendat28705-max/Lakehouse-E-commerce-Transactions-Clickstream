#!/bin/bash
set -euo pipefail

export MSYS_NO_PATHCONV=1

COMPOSE_CMD="docker compose"

wait_for_mysql() {
    echo "[INFO] Cho MySQL san sang..."
    until docker exec lakehouse-mysql mysqladmin ping -h "localhost" --silent >/dev/null 2>&1; do
        sleep 5
    done
}

wait_for_namenode() {
    echo "[INFO] Cho NameNode san sang..."
    until docker exec lakehouse-namenode sh -c "hdfs dfs -ls / >/dev/null 2>&1 || /opt/hadoop/bin/hdfs dfs -ls / >/dev/null 2>&1"; do
        sleep 5
    done
}

wait_for_metastore() {
    echo "[INFO] Cho Hive Metastore san sang..."
    until docker exec lakehouse-hive-metastore bash -lc "echo > /dev/tcp/127.0.0.1/9083" >/dev/null 2>&1; do
        if ! docker ps --format '{{.Names}}' | grep -qx 'lakehouse-hive-metastore'; then
            echo "[ERROR] Hive Metastore da dung truoc khi san sang. In log de chan doan..."
            docker logs lakehouse-hive-metastore || true
            return 1
        fi
        sleep 5
    done
}

init_hive_schema_if_needed() {
    echo "[INFO] Kiem tra Hive Metastore schema..."
    if docker exec lakehouse-mysql mysql -uhive -phivepassword hive_metastore -e "SELECT 1 FROM VERSION LIMIT 1;" >/dev/null 2>&1; then
        echo "[INFO] Hive Metastore schema da ton tai."
        return
    fi

    echo "[INFO] Khoi tao Hive Metastore schema..."
    # Run schematool directly so the one-off container exits after init
    # instead of continuing into the metastore entrypoint and hanging here.
    $COMPOSE_CMD run --rm --no-deps --entrypoint /opt/hive/bin/schematool \
      hive-metastore -dbType mysql -initSchema
}

echo "=== Khoi tao Lakehouse Architecture ==="

# 1. Chuan bi
mkdir -p drivers config logs scripts data airflow/dags airflow/logs airflow/plugins

if [ ! -f "drivers/mysql-connector-j-8.0.33.jar" ]; then
    echo "[INFO] Tai MySQL Connector..."
    curl -L -o drivers/mysql-connector-j-8.0.33.jar \
    https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar
fi

if [ ! -f "drivers/delta-core_2.12-2.4.0.jar" ]; then
    echo "[INFO] Tai Delta Core..."
    curl -L -o drivers/delta-core_2.12-2.4.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar
fi

if [ ! -f "drivers/delta-storage-2.4.0.jar" ]; then
    echo "[INFO] Tai Delta Storage..."
    curl -L -o drivers/delta-storage-2.4.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-storage/2.4.0/delta-storage-2.4.0.jar
fi

# 2. Start MySQL + Airflow Postgres + NameNode
echo "[INFO] Khoi dong MySQL, Airflow Postgres va NameNode..."
$COMPOSE_CMD up -d mysql airflow-postgres namenode
wait_for_mysql
wait_for_namenode

if ! docker exec lakehouse-namenode test -d /hadoop/dfs/name/current; then
    echo "[INFO] Format HDFS lan dau..."
    docker exec lakehouse-namenode hdfs namenode -format -force -nonInteractive
    wait_for_namenode
fi

# 3. Start DataNode
echo "[INFO] Khoi dong DataNode..."
$COMPOSE_CMD up -d datanode
sleep 10

# 4. Tao thu muc HDFS
echo "[INFO] Tao thu muc HDFS..."
docker exec lakehouse-namenode sh -c '
if command -v hdfs >/dev/null 2>&1; then
  HDFS_CMD=hdfs
else
  HDFS_CMD=/opt/hadoop/bin/hdfs
fi
$HDFS_CMD dfs -mkdir -p /spark-logs &&
$HDFS_CMD dfs -chmod 777 /spark-logs &&
$HDFS_CMD dfs -mkdir -p /user/hive/warehouse &&
$HDFS_CMD dfs -chmod 777 /user/hive/warehouse &&
$HDFS_CMD dfs -mkdir -p /lakehouse/bronze &&
$HDFS_CMD dfs -chmod 777 /lakehouse/bronze &&
$HDFS_CMD dfs -mkdir -p /lakehouse/silver &&
$HDFS_CMD dfs -chmod 777 /lakehouse/silver &&
$HDFS_CMD dfs -mkdir -p /lakehouse/gold &&
$HDFS_CMD dfs -chmod 777 /lakehouse/gold &&
$HDFS_CMD dfs -mkdir -p /user/delta &&
$HDFS_CMD dfs -chmod 777 /user/delta &&
$HDFS_CMD dfs -mkdir -p /tmp &&
$HDFS_CMD dfs -chmod 777 /tmp
'

# 5. Start Hive
init_hive_schema_if_needed
echo "[INFO] Khoi dong Hive Metastore..."
$COMPOSE_CMD up -d hive-metastore
wait_for_metastore

# 6. Start Spark
echo "[INFO] Khoi dong Spark..."
$COMPOSE_CMD up -d spark-master spark-worker spark-thrift-server

# 7. Start Superset
echo "[INFO] Khoi dong Superset..."
$COMPOSE_CMD up -d superset

# 8. Start Airflow
echo "[INFO] Khoi tao Airflow metadata..."
$COMPOSE_CMD up --build airflow-init

echo "[INFO] Khoi dong Airflow..."
$COMPOSE_CMD up -d --build airflow-webserver airflow-scheduler

echo "=== HOAN TAT ==="