FROM docker.io/chrislusf/seaweedfs:4.50

COPY s3.json.template /etc/seaweedfs/s3.json.template
COPY entrypoint.sh /usr/local/bin/lake-entrypoint.sh
COPY init-buckets.sh /usr/local/bin/init-buckets.sh
RUN chmod 755 /usr/local/bin/lake-entrypoint.sh /usr/local/bin/init-buckets.sh

EXPOSE 8333 9333
ENTRYPOINT ["/usr/local/bin/lake-entrypoint.sh"]
