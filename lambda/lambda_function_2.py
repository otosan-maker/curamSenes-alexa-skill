import sys
import logging
import pymysql
import boto3
import json
from botocore.exceptions import ClientError


#rds settings
rds_host  = "xxxxxxxxxxxxxx"
name = "admin"
password = "xxxxxxxxxxxxx" 
db_name = "health001"

logger = logging.getLogger()
logger.setLevel(logging.INFO)

try:
    conn = pymysql.connect(host=rds_host, user=name, passwd=password, db=db_name, connect_timeout=5)
except pymysql.MySQLError as e:
    logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
    logger.error(e)
    sys.exit()

logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")
try:
    client = boto3.client('iot-data', region_name='eu-west-1')
except ClientError as e:
    print("Client error: %s" % e)

def lambda_handler(event, context):
    item_count = 0
    with conn.cursor() as cur:
        cur.execute("select dm.m_name name,pat.id_device iddevice,pat.id_patient idpatient from patient pat, dispense_medication dm where pat.id_patient = dm.id_patient and abs(hour(now())-hour) < 1;")
        for row in cur:
            logger.info(row)
            strIDPatient = str(row[2])
            strMName     = str(row[0])
            strIDClietn  = str(row[1])
            
            strSQL = "insert into dispense_medication_sent(id_patient,m_name) values ("+ strIDPatient +",'" + strMName + "');"
            logger.info(strSQL)
            cur.execute(strSQL)
            with conn.cursor() as cur2:
                cur2.execute("SELECT LAST_INSERT_ID();")
                for row2 in cur2:
                    strIDDSM = strMName     = str(row2[0])
                    logger.info(strIDDSM)
            
            strTopic = strIDClietn + '/medication'
            strPayLoad = '{"m_name":"'+ str(row[0]) +'","id_dsm":'+ strIDDSM +',"time":1000}'
            response = client.publish(topic=strTopic,qos=1,payload= strPayLoad  )
    conn.commit()

    return "Added %d items from RDS MySQL table" %(item_count)