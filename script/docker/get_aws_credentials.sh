#!/bin/bash

# This script extracts the AWS credentials from the established environment variables or configuration file.
# It is meant to be sourced by other scripts that require them and makes them available via the following variables:
#
# ACCESS_KEY            The AWS Access Key Id
# ACCESS_KEY_SOURCE     Indicates form where the access key was obtained (file or environment)
# SECRET_KEY            The AWS Secret Key
# SECRET_KEY_SOURCE     Indicates form where the secret access key was obtained (file or environment)

if [ -z "$AWS_ACCESS_KEY_ID" ] || [ -z "$AWS_SECRET_ACCESS_KEY" ]; then
  credentials_file="$HOME/.aws/credentials"
  access_key_from_file=$(awk -F "=" '/aws_access_key_id/ {print $2}' "$credentials_file" 2>/dev/null)
  secret_key_from_file=$(awk -F "=" '/aws_secret_access_key/ {print $2}' "$credentials_file" 2>/dev/null)
  if [ -z "$access_key_from_file" ] || [ -z "$secret_key_from_file" ]; then
    ACCESS_KEY="skyriseminio"
    ACCESS_KEY_SOURCE=$ACCESS_KEY
    SECRET_KEY="skyriseminio"
    SECRET_KEY_SOURCE=$SECRET_KEY
  else
    ACCESS_KEY=$access_key_from_file
    ACCESS_KEY_SOURCE="<from $credentials_file>"
    SECRET_KEY=$secret_key_from_file
    SECRET_KEY_SOURCE="<from $credentials_file>"
  fi
else
  ACCESS_KEY=$AWS_ACCESS_KEY_ID
  # shellcheck disable=SC2034
  ACCESS_KEY_SOURCE="<from AWS_ACCESS_KEY_ID environment variable>"
  SECRET_KEY=$AWS_SECRET_ACCESS_KEY
  # shellcheck disable=SC2034
  SECRET_KEY_SOURCE="<from AWS_SECRET_ACCESS_KEY environment variable>"
fi
