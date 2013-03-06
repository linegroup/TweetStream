export HBASE_OPTS="-XX:+HeapDumpOnOutOfMemoryError -XX:+UseConcMarkSweepGC -XX:+CMSIncrementalMode"
export HBASE_CLASSPATH=`echo $HBASE_CLASSPATH | sed -e "s|$ZOOKEEPER_CONF:||"`
