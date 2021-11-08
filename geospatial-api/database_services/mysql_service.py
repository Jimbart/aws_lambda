# -*- coding: utf-8 -*-

# Norman Sánchez - martes, 12 de Marzo de 2019 (GMT-6)
# para realizar operaciones específicas con MySQL
import configparser
import logging
import boto3
import base64
from botocore.exceptions import ClientError
from mysql.connector import MySQLConnection as MySQL
import json, os, sys
import pandas as pd

logger = logging.getLogger(__name__)

class MySQLConnection(object):

    """Todas las clases que se conectan a MySQL deben heredar de esta clase"""

    def __init__(self, configfile):
        self.config = self.parseconfig(configfile)
        self.conn = MySQL(**self.config)


    @staticmethod
    def parseconfig(configfile):
        logger.debug('parseconfig')
        try:
            # get secret config from config.inis
            config = configparser.ConfigParser()
            config.read(configfile)

            # get secret from secrets manager
            session = boto3.session.Session()
            client = session.client(
                service_name='secretsmanager',
                region_name=config.get('MySQL', 'region_name')
            )
            get_secret_value_response = client.get_secret_value(
                SecretId=config.get('MySQL', 'secret_name')
            )
            secret = json.loads(get_secret_value_response.get('SecretString'))

            return {
                'user': secret.get('username')
                ,'password': secret.get('password')
                ,'host': secret.get('host')
                ,'database': secret.get('dbname')
            }
        except Exception as exc:
            exc_response = sys.exc_info()
            logger.error('line: {}, {}'.format(exc_response[2].tb_lineno, str(exc)))
            return None

    
    def encode_data(self, data, code):
        try:
            return data.encode(code)

        except Exception:
            return data



    def spexec(self, spd, args):
        logger.debug('spexec')
        cursor = self.conn.cursor()
        cursor.callproc(spd, args)
        self.conn.commit()
        records = []

        for resulset in cursor.stored_results():
            records += resulset.fetchall()
        cursor.close()

        if records == [(None,)] or records == []:
            raise Exception('Stored Procedure did not return any records')

        return records


    def db_colname(self, pandas_colname):
        colname =  pandas_colname.replace(' ','_').strip()                  
        return colname

    def spexec_to_dfs(self, spd, args):
        logger.debug('spexec_to_dfs')
        cursor = self.conn.cursor()
        cursor.callproc(spd, args)
        self.conn.commit()
        dataframes = []

        for resulset in cursor.stored_results(): 
            column_names = [col[0] for col in resulset.description]
            df = pd.DataFrame(resulset.fetchall(), columns=column_names)
            dataframes.append(df)

        cursor.close()

        return dataframes


    def close(self):
        """Cerrar conexiones"""
        self.conn.close()
