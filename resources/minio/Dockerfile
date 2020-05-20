FROM minio/minio

ENV MINIO_ACCESS_KEY minio
ENV MINIO_SECRET_KEY miniostorage

RUN mkdir -p /data/test && mkdir -p /data/develop

ADD ./config/develop/policy.json /data/.minio.sys/buckets/develop/policy.json
ADD ./config/test/policy.json /data/.minio.sys/buckets/test/policy.json

EXPOSE 9000

CMD ["server","/data"]