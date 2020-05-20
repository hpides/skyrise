# Use Minio to simulate S3 buckets locally

You can use [Minio](https://github.com/minio/minio) to simulate S3 buckets locally.

## Setup

For this build the minio image provided in the Dockerfile with

```bash
$ docker build -t localminio .
```

The `Dockerfile` specifies that there will be two buckets created automatically. They are called `data` and `develop`. Their configurations can be found in `config/test/policy.json` and `config/develop/policy.json` respectively.

The buckets will only show if there is data in the buckets. You can insert custom data in the buckets by placing files in `data/test/` or `data/develop/`.

To start Minio with/without your custom data run

```bash
$ docker run --rm -v ${PWD}/data:/data localminio
```

in the minio directory.

You can access the buckets via the browser under `http://172.17.0.2:9000/minio/develop/`. The initial username and passwords are `minio` and `miniostorage` respectively (compare Dockerfile).

## Interact with Minio using AWS

After setting up minio you can now interact with your local minio instance using the aws client. For this you first have to [configure aws](https://docs.min.io/docs/aws-cli-with-minio.html).

```bash
$ aws configure set default.s3.signature_version s3v4
$ aws configure
AWS Access Key ID [None]: <username>
AWS Secret Access Key [None]: <password>
Default region name [None]: us-east-1
Default output format [None]: ENTER
```

Like above, the `<username>` and `<password>` are defined in the minio `Dockerfile`. They are `minio` and `miniostorage` by default.

If Minio is running, you can list your local buckets using the aws client with

```bash
$ aws --endpoint-url http://172.17.0.2:9000 s3 ls
2019-10-30 11:02:23 develop
2019-10-29 13:48:25 test
```

Your output should look similar.

## Interact with Minio using C++

You can start the provided application using the command

```bash
export AWS_ACCESS_KEY_ID=<username>
export AWS_SECRET_ACCESS_KEY=<password>
echo '{"s3bucket": "develop", "s3key":"cpp.png", "isLocal":true }' | sam local invoke HelloWorldFunction
```

Like above, the default <username> and <password> are `minio` and `miniostorage`.

The program will connect against your local minio instance, download the file `cpp.png` from the bucket `develop` and output is as a base64.
