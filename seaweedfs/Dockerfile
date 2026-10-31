FROM docker.io/chrislusf/seaweedfs:4.50

# Python und boto3 für init-buckets.py; das Basisimage ist Alpine-basiert.
RUN apk add --no-cache python3 py3-boto3

COPY s3.json.template /etc/seaweedfs/s3.json.template
COPY entrypoint.sh /usr/local/bin/lake-entrypoint.sh
COPY init-buckets.py /usr/local/bin/init-buckets.py
RUN chmod 755 /usr/local/bin/lake-entrypoint.sh /usr/local/bin/init-buckets.py

EXPOSE 8333 9333
ENTRYPOINT ["/usr/local/bin/lake-entrypoint.sh"]
