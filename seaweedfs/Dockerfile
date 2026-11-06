FROM docker.io/chrislusf/seaweedfs:4.50

COPY s3.template.json /etc/seaweedfs/s3.template.json
COPY entrypoint.sh /usr/local/bin/lake-entrypoint.sh
RUN chmod 755 /usr/local/bin/lake-entrypoint.sh

EXPOSE 8333 9333
ENTRYPOINT ["/usr/local/bin/lake-entrypoint.sh"]
