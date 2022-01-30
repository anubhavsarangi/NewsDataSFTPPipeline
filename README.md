# News Data SFTP Pipeline
Serverless Pipeline to load SFTP data to Amazon Redshift

![News Data SFTP Pipeline Architecture](https://user-images.githubusercontent.com/20443780/151702981-521c7e94-47e5-4887-9563-a0259f5ef00a.png)


# Prerequisites
1. Python (on local)
2. Spark (on local)
3. Private Key (sftp_ssh.pem)
4. SFTP Credentials (sftp_credentials.yml)
5. Required Dependencies (requirements.txt)

Run in CMD
pip install -r requirements.txt
py News_Data_SFTP_Pipeline.py

The curated files will be loaded in %CURRENT_DIR%/tmp/curated/
