#!/bin/bash

run="kafka-health-check "

for VAR in `env`
do
  if [[ $VAR =~ ^KHC_ ]]; then
    kafka_name=`echo "$VAR" | sed -r "s/KHC_(.*)=.*/\1/g" | tr '[:upper:]' '[:lower:]' | tr _ -`
    env_var=`echo "$VAR" | sed -r "s/(.*)=.*/\1/g"`
    run+="-$kafka_name ${!env_var} "
  fi
done

if [[ -n "$KAFKA_ZOOKEEPER_CONNECT" ]]; then
    run+="-zookeeper $KAFKA_ZOOKEEPER_CONNECT "
fi

if [[ -n "$KAFKA_BROKER_ID" ]]; then
    run+="-broker-id $KAFKA_BROKER_ID "
fi

echo $run
eval $run
