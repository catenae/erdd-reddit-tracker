#!/bin/bash

./build.sh

NUMBER=${2:-31}
CPUS=${1:-0-3}

TS=$(date +%s)
for i in $(seq 1 $NUMBER); do
  TS=$((TS + 1))
  docker run -d --restart unless-stopped --net=host --name user_content_crawler_$TS \
  --cpuset-cpus="$CPUS" \
  catenae/rut-links user_content_crawler \
  -i new_users,p1_users,p2_users,p3_users,p4_users,p5_users,p6_users \
  -o new_texts \
  -b 127.0.0.1:9092 \
  -a 127.0.0.1:3000 &
done

wait
