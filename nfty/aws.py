import logging
import boto3
from botocore.exceptions import ClientError


logger = logging.getLogger("AppLogger")


class S3:
    def __init__(self):
        self.s3_client = boto3.client("s3")

    def create_bucket(self, bucket_name, region=None):
        try:
            if region is None:
                self.s3_client.create_bucket(Bucket=bucket_name)
            else:
                self.s3_client = boto3.client("s3", region_name=region)
                location = {"LocationConstraint": region}
                self.s3_client.create_bucket(
                    Bucket=bucket_name, CreateBucketConfiguration=location
                )
        except ClientError as e:
            return False
        return True

    def list_buckets(self):
        response = self.s3_client.list_buckets()
        return [bucket for bucket in response["Buckets"]]

    def upload(self, data, bucket, key):
        return self.s3_client.upload_fileobj(data, bucket, key)


def get_secret(secret_name, region_name="us-east-1"):
    session = boto3.session.Session()
    client = session.client(
        service_name="secretsmanager",
        region_name=region_name,
    )
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            return "The requested secret " + secret_name + " was not found"
        elif e.response["Error"]["Code"] == "InvalidRequestException":
            return "The request was invalid due to:", e
        elif e.response["Error"]["Code"] == "InvalidParameterException":
            return "The request had invalid params:", e
        elif e.response["Error"]["Code"] == "DecryptionFailure":
            return (
                "The requested secret can't be decrypted using the provided KMS key:",
                e,
            )
        elif e.response["Error"]["Code"] == "InternalServiceError":
            return "An error occurred on service side:", e
    else:
        # Secrets Manager decrypts the secret value using the associated KMS CMK
        # Depending on whether the secret was a string or binary, only one of these fields will be populated
        if "SecretString" in get_secret_value_response:
            return get_secret_value_response["SecretString"]
        else:
            return get_secret_value_response["SecretBinary"]
