# NewsDataSFTPPipeline
Serverless Pipeline to load SFTP data to Amazon Redshift

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
