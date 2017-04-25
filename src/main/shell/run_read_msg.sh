#!/usr/bin/env bash


base_dir=$(dirname $0)/..


if [ "x$KAFKA_HEAP_OPTS" = "x" ]; then
    export KAFKA_HEAP_OPTS="-Xmx512M"
fi


KAFKA_JAAS_CONF=$base_dir/conf/kafka_client_jaas_dcadmin_key.conf
if [ -f $KAFKA_JAAS_CONF ]; then
    export KAFKA_CLIENT_KERBEROS_PARAMS="-Djava.security.auth.login.config=$KAFKA_JAAS_CONF"
fi

LOG4J=$base_dir/conf/log4j.properties
if [ -f $LOG4J ]; then
    export LOG4J_PARAMS="-Dlog4j.configuration=file:$LOG4J"
fi

jars=$base_dir/kafka-read-1.0-SNAPSHOT.jar
for jar in $base_dir/lib/*.jar
do
jars=$jars:$jar
done

java  ${KAFKA_CLIENT_KERBEROS_PARAMS} $LOG4J_PARAMS -cp $jars  kafka.ConsumerMsg  -t redislog
