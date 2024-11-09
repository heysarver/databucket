# /setup.py

from setuptools import setup, find_packages

setup(
    name="databucket",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "boto3>=1.26.0",
        "python-dotenv>=0.19.0",
    ],
    author="Your Name",
    author_email="your.email@example.com",
    description="Simple S3 bucket operations library",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/heysarver/databucket",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)
