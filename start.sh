#!/bin/bash

# ./start.sh  --data_path /tmp/data --dst_ip 127.0.0.1 --dst_port 3306 --dst_user root --dst_password 123456789

echo "shell parameters: $*"

./run $*
