# News Data SFTP Pipeline
Serverless Pipeline to load SFTP data to Amazon Redshift

![News Data SFTP Pipeline Architecture](https://user-images.githubusercontent.com/20443780/151702981-521c7e94-47e5-4887-9563-a0259f5ef00a.png)


# Prerequisites
1. Python (on local)
2. Spark (on local)
3. Private Key (sftp_ssh.pem)
4. SFTP Credentials (sftp_credentials.yml)
5. Required Dependencies (requirements.txt)

## Steps to execute the workflow
Run in CMD
pip install -r requirements.txt
py News_Data_SFTP_Pipeline.py

The curated files will be loaded in %CURRENT_DIR%/tmp/curated/

## Implementation Plan Backlog:
- AWS Transfer for SFTP
Create a SFTP server, set up user accounts, and associate the server with a S3 bucket to sync files. 
Reference - https://aws.amazon.com/blogs/aws/new-aws-transfer-for-sftp-fully-managed-sftp-service-for-amazon-s3/

- Attach a Lambda function to the bucket
The above python code - "News_Data_SFTP_Pipeline.py" is to be executed on AWS Lambda.

- Create, build and push docker image to Amazon ECR

- Column enrichments like extracing Page Editor, Reporting by, Editing by, etc. (from "ACCUMULATED_STORY_TEXT" or "TAKE_TEXT" attribute) which would simplify querying and searching through the data.

- Few attributes have records containing articles and tables, these could be stored in document databases.

- The data contains text from multiple languages - Arabic, Spanish, etc.; these could be translated using APIs and standardized to English.

- An Airflow DAG script could also be used to schedule the pipeline run on an EC2 instance.
