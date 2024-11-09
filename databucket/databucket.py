# /databucket/s3_operations.py

import os
import boto3
from dotenv import load_dotenv
from botocore.exceptions import ClientError
import mimetypes
from typing import List, Union, BinaryIO
from pathlib import Path

# Load environment variables
load_dotenv()


class DataBucket:
    def __init__(self, bucket_name: str = None):
        """Initialize S3 client with credentials from .env file"""
        self.s3_access_key = os.getenv('S3_ACCESS_KEY_ID')
        self.s3_secret_key = os.getenv('S3_ACCESS_KEY_SECRET')
        self.bucket_name = bucket_name or os.getenv('S3_BUCKET_NAME')

        if not all([self.s3_access_key, self.s3_secret_key]):
            raise ValueError(
                "S3 credentials not found in environment variables")

        self.s3_client = boto3.client(
            's3',
            s3_access_key_id=self.s3_access_key,
            s3_secret_access_key=self.s3_secret_key
        )

    def upload_file(self, file_path: Union[str, Path], s3_path: str = None) -> bool:
        """
        Upload a file to S3 bucket

        Args:
            file_path: Local path to the file
            s3_path: Desired path in S3 (if None, uses filename)

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            file_path = Path(file_path)
            if not file_path.exists():
                raise FileNotFoundError(f"File not found: {file_path}")

            # If s3_path not provided, use original filename
            s3_path = s3_path or file_path.name

            # Detect content type
            content_type = mimetypes.guess_type(
                file_path)[0] or 'application/octet-stream'

            self.s3_client.upload_file(
                str(file_path),
                self.bucket_name,
                s3_path,
                ExtraArgs={'ContentType': content_type}
            )
            return True
        except ClientError as e:
            print(f"Error uploading file: {e}")
            return False

    def download_file(self, s3_path: str, local_path: Union[str, Path]) -> bool:
        """
        Download a file from S3 bucket

        Args:
            s3_path: Path of the file in S3
            local_path: Local path to save the file

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            local_path = Path(local_path)
            local_path.parent.mkdir(parents=True, exist_ok=True)

            self.s3_client.download_file(
                self.bucket_name,
                s3_path,
                str(local_path)
            )
            return True
        except ClientError as e:
            print(f"Error downloading file: {e}")
            return False

    def list_files(self, prefix: str = "") -> List[str]:
        """
        List all files in the bucket with given prefix

        Args:
            prefix: Filter results to files starting with this prefix

        Returns:
            List of file paths in the bucket
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )

            if 'Contents' not in response:
                return []

            return [obj['Key'] for obj in response['Contents']]
        except ClientError as e:
            print(f"Error listing files: {e}")
            return []

    def delete_file(self, s3_path: str) -> bool:
        """
        Delete a file from S3 bucket

        Args:
            s3_path: Path of the file to delete

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.s3_client.delete_object(
                Bucket=self.bucket_name,
                Key=s3_path
            )
            return True
        except ClientError as e:
            print(f"Error deleting file: {e}")
            return False

    def create_folder(self, folder_path: str) -> bool:
        """
        Create a folder in S3 bucket

        Args:
            folder_path: Path of the folder to create

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Ensure the folder path ends with a slash
            folder_path = folder_path.rstrip('/') + '/'

            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=folder_path
            )
            return True
        except ClientError as e:
            print(f"Error creating folder: {e}")
            return False

    def get_file_url(self, s3_path: str, expiration: int = 3600) -> str:
        """
        Generate a presigned URL for a file

        Args:
            s3_path: Path of the file in S3
            expiration: URL expiration time in seconds (default 1 hour)

        Returns:
            str: Presigned URL or empty string if error
        """
        try:
            url = self.s3_client.generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': self.bucket_name,
                    'Key': s3_path
                },
                ExpiresIn=expiration
            )
            return url
        except ClientError as e:
            print(f"Error generating URL: {e}")
            return ""
