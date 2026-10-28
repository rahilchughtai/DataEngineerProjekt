#!/bin/sh
# Gibt die eigene IP-Adresse aus, über die der Spark-Master erreichbar ist.
# Worker und Job-Container hängen in zwei Netzen (sparknet, lakenet). Spark würde sonst die erste
# Adresse aus /etc/hosts melden, und die ist unter Umständen aus Sicht des Masters unerreichbar.
# UDP-connect sendet kein Paket, es fragt nur die Routing-Tabelle nach der Quelladresse.
set -eu

python3 -c '
import socket, sys
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.connect((sys.argv[1], int(sys.argv[2])))
print(s.getsockname()[0])
' "${SPARK_MASTER_HOST:-spark-master}" "${SPARK_MASTER_PORT:-7077}"
