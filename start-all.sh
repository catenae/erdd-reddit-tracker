#!/bin/bash

./build.sh

TS=$(date +%s)

docker run -d --restart unless-stopped --net=host --name submission_crawler_$TS \
--cpuset-cpus="$1" \
catenae/rut-links submission_crawler \
-o user_ids_to_check \
-b 127.0.0.1:9092

docker run -d --restart unless-stopped --net=host --name comment_crawler_$TS \
--cpuset-cpus="$1" \
catenae/rut-links comment_crawler \
-o user_ids_to_check \
-b 127.0.0.1:9092

docker run -d --restart unless-stopped --net=host --name new_user_filter_$TS \
--cpuset-cpus="$1" \
catenae/rut-links new_user_filter \
-i user_ids_to_check \
-o new_users \
-b 127.0.0.1:9092 \
-a 127.0.0.1:3000

docker run -d --restart unless-stopped --net=host --name user_content_crawler_$TS \
--cpuset-cpus="$1" \
catenae/rut-links user_content_crawler \
-i new_users,p1_users,p2_users,p3_users,p4_users,p5_users,p6_users \
-o new_texts \
-b 127.0.0.1:9092 \
-a 127.0.0.1:3000

docker run -d --restart unless-stopped --net=host --name post_storer_$TS \
--cpuset-cpus="$1" \
catenae/rut-links post_storer \
-b 127.0.0.1:9092 \
-i new_texts
