# Databucket

A simple Python library for S3 bucket operations using config from environment variables.

## Installation

```bash
pip install git+https://github.com/heysarver/databucket.git
```

## Usage

Set the following environment variables:

- S3_ACCESS_KEY_ID
- S3_ACCESS_KEY_SECRET
- S3_BUCKET_NAME (optional)

```python
from databucket import DataBucket

s3 = DataBucket('my-bucket-name') # or use env
s3.upload_file('path/to/file.jpg', 'images/file.jpg')
```
