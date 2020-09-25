#!/bin/bash

# This script facilitates the operation of the object storage system MinIO. We employ MinIO to simulate the Amazon S3
# object storage in a local development environment. This allows for testing and debugging as in a cloud deployment.
# The script starts and stops a Docker container running MinIO (with the official image minio/minio). It creates buckets
# in a MinIO instance, uploads files to buckets, and cleans up any persistent state held by MinIO. It assumes the user
# to be in the Unix group docker.
#
# A sample use of this script to start a local MinIO instance, create a bucket 'data', and upload a file 'lineitems.tbl':
#    $ ./storage_backend start
#    $ ./storage_backend prepare lineitems.tbl

# Default variables
CONTAINER_NAME="skyrise_minio"
HOST="127.0.0.1:9000"
BUCKET="data"
DOCKER=true

# Sets ACCESS_KEY, ACCESS_KEY_SOURCE, SECRET_KEY and SECRET_KEY_SOURCE
source $(dirname "${BASH_SOURCE[0]}")/get_aws_credentials.sh

# Helper functions
exitWithError() {
    echo "$1";
    echo "Usage: storage_backend.sh [options] COMMAND"
    echo "  Options:"
    echo "   [--host HOST (default='127.0.0.1:9000')]"
    echo "   [--bucket BUCKET (default='data')]"
    echo "   [--no-docker]"
    echo
    echo "  Commands:"
    echo "    start"
    echo "      Starts a Docker container running MinIO."
    echo "      Port 9000 is forwarded to localhost."
    echo "      Data is stored inside the container."
    echo "      If a container was previously started and stopped,"
    echo "      this command restarts the existing container."
    echo "    stop"
    echo "      Stops the Docker container running MinIO."
    echo "      The container and its data are preserved and"
    echo "      available upon restart."
    echo "    remove"
    echo "      Stops the Docker container running MinIO."
    echo "      The container and its data are deleted."
    echo "    status"
    echo "      Checks whether MinIO is running in a Docker container"
    echo "      (or as a regular process if --no-docker is supplied)."
    echo "    prepare [FILE1 ...]"
    echo "      Creates the bucket BUCKET and uploads the given files"
    echo "      to the bucket."
    echo "    put FILE1 [FILE2 ...]"
    echo "      Uploads the given files to bucket BUCKET."
    echo "      Existing files are replaced with their new versions."
    echo "      The bucket must already exist."
    exit 1;
}

assert_docker_running() {
  if [ "$Docker" = false ]; then
		echo >&2 "This command can only be used with Docker."
		exit 1
	fi
	docker images >/dev/null 2>&1 || { echo >&2 "Docker is not running. Aborting."; exit 1; }
}

convert_to_absolute_path() {
	case "$1" in
	  /*) echo "$1";;
	  *) echo "$PWD/$1";;
	esac
}

test_minio_container_exists() {
	docker ps -a | grep "$CONTAINER_NAME" >/dev/null 2>&1
	return "$?"

}

test_minio_container_running() {
	docker ps | grep "$CONTAINER_NAME" >/dev/null 2>&1
	return "$?"

}

assert_minio_container_running() {
	test_minio_container_running || { echo >&2 "Docker container is not running. Aborting."; exit 1; }
}

# AWS Signing
XAMZDATE=$(date -u "+%Y%m%dT%H%M%SZ")
DAY=$(date -u "+%Y%m%d")

get_signing_key() {
	date_key=$(printf "$DAY" | openssl dgst -sha256 -mac HMAC -macopt "key:AWS4${SECRET_KEY}" -binary | xxd -p -c 256)
	date_region_key=$(printf "us-east-1" | openssl dgst -sha256 -mac HMAC -macopt "hexkey:${date_key}" -binary | xxd -p -c 256)
	date_region_service_key=$(printf "s3" | openssl dgst -sha256 -mac HMAC -macopt "hexkey:${date_region_key}" -binary | xxd -p -c 256)
	signing_key=$(printf "aws4_request" | openssl dgst -sha256 -mac HMAC -macopt "hexkey:${date_region_service_key}" -binary | xxd -p -c 256)
	printf ${signing_key}
}

get_canonical_request_hash() {
	uri=$1
	payload_hash=$2
	canonical_request="PUT\n/$uri\n\nhost:$HOST\nx-amz-content-sha256:$payload_hash\nx-amz-date:$XAMZDATE\n\nhost;x-amz-content-sha256;x-amz-date\n$payload_hash"
	printf $(printf "$canonical_request" | openssl sha256 -binary | xxd -p -c 256)
}

get_authorization_string() {
	canonical_request_hash=$1
	signing_key=$(get_signing_key)
	scope="$DAY/us-east-1/s3/aws4_request"
	string_to_sign="AWS4-HMAC-SHA256\n$XAMZDATE\n$scope\n$canonical_request_hash"
	signature=$(printf "$string_to_sign" | openssl dgst -sha256 -mac HMAC -macopt "hexkey:${signing_key}" -binary | xxd -p -c 256)
	printf "AWS4-HMAC-SHA256 Credential=${ACCESS_KEY}/$scope,SignedHeaders=host;x-amz-content-sha256;x-amz-date,Signature=${signature}"
}

create_bucket() {
	empty_string_sha256="e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	canonical_request_hash=$(get_canonical_request_hash "$BUCKET/" "$empty_string_sha256")
	auth=$(get_authorization_string "$canonical_request_hash")
	
	response=$(curl -s -v -X PUT \
	    -H "Host: $HOST" \
	    -H "Content-Length: 0" \
	    -H "Authorization: ${auth}" \
	    -H "X-Amz-Content-Sha256: ${empty_string_sha256}" \
	    -H "X-Amz-Date: $XAMZDATE" \
	    http://$HOST/${BUCKET}/ 2>&1 | grep "HTTP/1.1 200 OK")

    return "$?"
}

upload_file() {
	filename=$1
	file_sha256=$(cat "$filename" | openssl sha256 -binary | xxd -p -c 256)
	file_size=$(wc -c < "$filename")
	canonical_request_hash=$(get_canonical_request_hash "$BUCKET/$filename" "$file_sha256")
	auth=$(get_authorization_string "$canonical_request_hash")
	response=$(curl -s -v -X PUT \
	    -H "Host: $HOST" \
	    -H "Content-Length: $file_size" \
	    -H "Authorization: ${auth}" \
	    -H "X-Amz-Content-Sha256: ${file_sha256}" \
	    -H "X-Amz-Date: $XAMZDATE" \
	    --data-binary "@${filename}" \ \
	    http://$HOST/${BUCKET}/$filename 2>&1 | grep "HTTP/1.1 200 OK")

  	return "$?"
}

# Start
start_minio() {
	assert_docker_running
	test_minio_container_running
	if [ "$?" -eq 0 ]; then
		echo "MinIO is already running."
		exit 0
	fi

	test_minio_container_exists
	if [ "$?" -eq 0 ]; then
		echo "Container with MinIO existing. Starting container.."
		docker start $CONTAINER_NAME >/dev/null 2>&1
		exit 0
	fi

	if [ ${#ACCESS_KEY} -le 2 ] || [ ${#SECRET_KEY} -le 7 ]; then
		echo "ACCESS_KEY requires at least 3 characters. SECRET_KEY requires at least 8 characters."
		exit 1
	fi

	echo "Creating container.. "
	CONTAINER_ID=$(docker run \
		-d  \
		--name $CONTAINER_NAME \
		-p 9000:9000 \
		-e "MINIO_ACCESS_KEY=$ACCESS_KEY" \
		-e "MINIO_SECRET_KEY=$SECRET_KEY" \
		minio/minio server /data)

	[ $? -eq "0" ] || { echo >&2 "Container creation failed. Please make sure Docker is configured correctly."; exit 1; }

	echo "Container ID     : ${CONTAINER_ID:0:12}"
	echo "Container Name   : $CONTAINER_NAME"
	echo "Access Key ID    : $ACCESS_KEY_SOURCE"
	echo "Secret Access Key: $SECRET_KEY_SOURCE"
}

# Stop
stop_minio() {
	assert_docker_running
	test_minio_container_running
	if [ "$?" -eq 0 ]; then
		echo "Stopping container.."
		docker stop $CONTAINER_NAME >/dev/null 2>&1
	else
		echo "Container is not running."
	fi
}

# Remove
remove_minio() {
	assert_docker_running
	stop_minio
	test_minio_container_exists
	if [ "$?" -eq 0 ]; then
		echo "Removing container.."
		docker rm $CONTAINER_NAME >/dev/null 2>&1
	else
		echo "Container does not exist."
	fi
}

# Status
status_minio() {
  if [ "$Docker" = true ]; then
		assert_docker_running
		test_minio_container_running
		if [ "$?" -ne 0 ]; then
			echo "Container is not running."
			exit 1
		fi
	fi

	curl -s "$HOST" | grep "<Code>AccessDenied</Code>" >/dev/null 2>&1
	if [ "$?" -ne 0 ]
	then
		echo "MinIO not working as expected."
		echo "Try to recreate or restart the container."
		exit 1
	fi

	echo "MinIO is up and running."
}

# Put
put_minio() {
	[ "$#" -ge 1 ] || { exitWithError "Please specify at least one FILE"; }

  if [ "$Docker" = true ]; then
		assert_docker_running
		assert_minio_container_running
	fi
	
	while [ "$#" -gt 0 ]; do
		echo "Uploading $1.."
		upload_file "$1"
		if [ "$?" -ne 0 ]; then
			echo "Error: Could not upload file. Does the bucket exist?"
			exit 1
		fi
		shift
	done
}

# Prepare
prepare_minio() {
  if [ "$Docker" = true ]; then
		assert_docker_running
		assert_minio_container_running
	fi

	echo "Creating Bucket with name $BUCKET.."
	create_bucket "$BUCKET"
	if [ "$?" -ne 0 ]; then
		echo "Error: Could not create bucket. Does it already exist?"
		exit 1
	fi

	if [ "$#" -ge 1 ]; then
		put_minio "$@"
	fi
}

# Main
while [ "$#" -gt 0 ]; do
    case $1 in
    --host)
        HOST="$2"
        shift
        ;;
    --bucket)
        BUCKET="$2"
        shift
        ;;
    --no-docker)
        DOCKER=false
        ;;
    *) 
	break 
	;;
    esac
    shift
done

[ "$#" -gt 0 ] || { exitWithError "Please specify command"; }

case "$1" in
	start)
	shift
	start_minio "$@"
	;;
	stop)
	shift
	stop_minio "$@"
	;;
	remove)
	shift
	remove_minio "$@"
	;;
	status)
	shift
	status_minio "$@"
	;;
	prepare)
	shift
	prepare_minio "$@"
	;;
	put)
	shift
	put_minio "$@"
	;;
	*)
	exitWithError "Unknown command";
	;;
esac

exit 0