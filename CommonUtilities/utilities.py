import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle
import paramiko

from Configuration.etlconfig import *
import logging

# Logging configution
logging.basicConfig(
    filename="Logs/etljob.log",
    filemode="w" ,
    format = '%(asctime)s-%(levelname)s-%(message)s',
    level = logging.INFO
    )
logger = logging.getLogger(__name__)


def sales_data_from_Linux_server(self):
    # download the sales frile form Linux server to local via SFTP/ssh
    try:
        logger.info("Sales file from Linux server doenload started...")
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy)
        ssh_client.connect(hostname,username=username,password=password)
        sftp = ssh_client.open_sftp()
        sftp.get(remote_file_path,local_file_path)
        sftp.close()
        ssh_client.close()
        logger.info("Sales file from Linux server doenload completed...")
    except Exception as e:
        logger.error("Error while donloading the sales file from Linux server.")

