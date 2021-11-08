# -*- coding: utf-8 -*-
"""Modulo que obtiene los elementos de un viaje y enviarlos a bigquery"""


import json
import csv
import pandas as pd
import numpy as np
import uuid
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient import discovery
import configparser
from google.cloud import bigquery
from google.cloud.bigquery.table import Table
from google.cloud.bigquery.dataset import Dataset
from google.oauth2 import service_account
import logging
import sys
import boto3
import base64
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class BigQueryMethods(object):
    """Clase que maneja los metodos para la info en big query"""

    def __init__(self, configfile):
        self.config = self.parseconfig(configfile)
        self.dataset = self.config['dataset']
        self.service_account = self.config['service_account']
        self.project_id = self.config['project_id']
        self.location = self.config['location']

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
                region_name=config.get('bigquery', 'region_name')
            )
            get_secret_value_response = client.get_secret_value(
                SecretId=config.get('bigquery', 'secret_name')
            )
            secret = json.loads(get_secret_value_response.get('SecretString'))

            return {
                'dataset': secret.get('dataset')
                ,'service_account': secret
                ,'project_id': secret.get('project_id')
                ,'location': secret.get('location')
            }

        except Exception as exc:
            exc_response = sys.exc_info()
            logger.error('line: {}, {}'.format(exc_response[2].tb_lineno, str(exc)))
            return None

    @staticmethod
    def isnan(value):
        try:

            return np.math.isnan(float(value))

        except Exception:
            return False

    def __generar_payload_file(self, schema, data):
        try:
            columns = []

            for key in schema:
                columns.append(key.name)

            filename = "payload.csv"
            df = pd.DataFrame(data)

            for key in schema:
                if key.field_type == 'INTEGER':  # correccion x bug en INTEGER
                    logger.debug('corrigiendo... {}'.format(key.name))
                    data[key.name] = data[key.name].map(
                        lambda a: 0 if self.isnan(a) else int(a))

            df.to_csv(
                filename,
                index=False,
                encoding='utf-8',
                quoting=csv.QUOTE_NONNUMERIC,
                columns=columns)

            return filename

        except Exception as ex:
            logger.error(str(ex))
            return ''

    def __formatear_renglones(self, schema, data):
        try:
            columns = []
            
            for key in schema:                      # obtengo la lista de columnas
                columns.append(key.name)

            data = data.reindex(columns = columns)  # para que todas las columnas existan                        
            data = pd.DataFrame(data[columns])      # para tomar solo las columnas q necesito
            data = data.where((pd.notnull(data)), None)
            rows = []

            for row in range(data.shape[0]):        # convierto a lista row x row
                rows.append((data.iloc[row][columns]).tolist())

            return 'OK', rows
        
        except Exception as ex:
            logger.error(str(ex))
            return 'FAIL', ex             

    def __create_temporal(self, bigquery_client, source_tablename):
        logger.info('__create_temporal')
        try:
            tmp_name = '_' + str(uuid.uuid4().hex)
            logger.info('tmp_name {}'.format(tmp_name))
            job_config = bigquery.QueryJobConfig()
            logger.info('job_config {}'.format(job_config))
            # Set the destination table
            table_ref = bigquery_client.dataset(self.dataset).table(tmp_name)
            logger.info('table_ref {}'.format(table_ref))
            job_config.destination = table_ref
            sql = """
                SELECT *
                FROM `{}.{}.{}`
                LIMIT 0;
            """.format(self.project_id, self.dataset, source_tablename)
            logger.info('sql {}'.format(sql))

            # Start the query, passing in the extra configuration.
            query_job = bigquery_client.query(
                sql,
                # Location must match that of the dataset(s) referenced in the query
                # and of the destination table.
                location=self.location,
                job_config=job_config)  # API request - starts the query
            logger.info('query_job {}'.format(query_job))

            query_job.result()  # Waits for the query to finish            
            logger.info('tabla temporal creada {}'.format(tmp_name))
            return 'OK', tmp_name

        except Exception as ex:
            logger.error(str(ex))
            return 'ERROR', str(ex)

    def __update_rows(self, bigquery_client, tmp_name, tablename, schema,
                     insert_id, timestamp):
        logger.info('__update_rows')
        try:
            fields_with_asig = []  # para obtener 'idEmpresa = b.idEmpresa'
            for key in schema:
                fields_with_asig.append(key.name + ' = b.' + key.name)

            fields_for_join = []  # para obtener a.Viaje = b.Viaje
            for key in insert_id:
                fields_for_join.append('a.' + key + ' = b.' + key)

            # detecto diferencias para ejecutar query
            query = '''
                SELECT
                    COUNT(*) as result
                FROM
                    ''' + self.dataset + '.' + tmp_name + ''' a
                    JOIN ''' + self.dataset + '.' + tablename + ''' b ON
                        ''' + ' AND '.join([
                ' AND '.join(fields_for_join),
                'a.' + timestamp + ' <> b.' + timestamp
            ]) + ';'

            # ejecuto el query
            query_job = bigquery_client.query(query
                                            ,location=self.location)
            results = query_job.result()  # Waits for the query to finish   

            for row in results:
                records = row[0]
                break

            # actualizo solo si vale la pena hacerlo
            if records > 0:
                query = '''
                    UPDATE ''' + self.dataset + '.' + tablename + ''' a
                    SET 
                        ''' + ','.join(fields_with_asig) + '''
                    FROM
                        ''' + self.dataset + '.' + tmp_name + ''' b 
                    WHERE
                        ''' + ' AND '.join(fields_for_join) + ';'
                query_job = bigquery_client.query(query
                                                ,location=self.location)
                results = query_job.result()  # Waits for the query to finish   

            return 'OK', ''

        except Exception as ex:
            logger.error(str(ex))
            return 'ERROR', str(ex)

    def __insert_rows(self, bigquery_client, tmp_name, tablename, schema, insert_id):
        logger.info('__insert_rows')
        try:
            fields = []  # para obtener 'idEmpresa, Viaje, ..., etc
            for key in schema:
                fields.append(key.name)

            # detecto diferencias para ejecutar query
            query = '''     
                    select
                        count(*)
                    from 
                        ''' + self.dataset + '.' + tmp_name + ''' as a
                        left join ''' + self.dataset + '.' + tablename + ' as b using (' + ','.join(
                insert_id) + ''')
                    where
                        b.''' + insert_id[0] + ' is null;' ''
            query_job = bigquery_client.query(query
                                            ,location=self.location)
            results = query_job.result()

            for row in results:
                records = row[0]
                break

            # cargo los registros
            if records > 0:
                query = '''     
                    INSERT INTO ''' + self.dataset + '.' + tablename + '(' + ','.join(
                    fields) + ''')
                    SELECT
                        a.''' + ',a.'.join(fields) + '''
                    FROM
                        ''' + self.dataset + '.' + tmp_name + ''' as a
                        LEFT JOIN ''' + self.dataset + '.' + tablename + ' as b USING (' + ','.join(
                        insert_id) + ''')
                    WHERE
                        b.''' + insert_id[0] + ' IS NULL;' ''
                query_job = bigquery_client.query(query
                                                ,location=self.location)
                results = query_job.result()

            return 'OK', ''

        except Exception as ex:
            logger.error(str(ex))
            return 'ERROR', str(ex)


    def __borrar_temporal(self, bigquery_client, tmp_name):
        logger.info('__borrar_temporal')
        try:
            table_ref = bigquery_client.dataset(self.dataset).table(tmp_name)
            bigquery_client.delete_table(table_ref)  # API request            

            return 'OK', ''

        except Exception as ex:
            logger.error(str(ex))
            return 'ERROR', str(ex)


    def obtejer_json_formato_bigquery(self, lista):
        logger.info('obtejer_json_formato_bigquery')

        results = []

        for element in lista:
            json_row = {}
            json_row['insertId'] = str(uuid.uuid4())
            json_row['json'] = element
            results.append(json_row)

        return results


    def cargar_datos_desde_json(self, t_id, json_data):
        logger.info('cargar_datos_desde_json')
        try:
            scopes = ['https://www.googleapis.com/auth/bigquery']
            credentials = ServiceAccountCredentials.from_json_keyfile_dict(self.service_account, scopes)
            service = discovery.build('bigquery', 'v2', credentials=credentials, cache_discovery=False)
            table_data = {
                "kind": "bigquery#tableDataInsertAllRequest",
                "skipInvalidRows": False,
                "ignoreUnknownValues": True,
                "rows": json_data
            }
            reponse = service.tabledata().insertAll(
                projectId=self.project_id
                ,datasetId=self.dataset
                ,tableId=t_id
                ,body=table_data).execute()

            if 'insertErrors' in reponse:
                raise Exception(reponse['insertErrors'])

            return 'OK', ''

        except Exception as exc:
            exc_output = sys.exc_info()
            logger.error('{},{}'.format(str(exc),str(exc_output[2].tb_lineno)))
            return 'FAIL', str(exc)


    def cargar_datos(self, tablename, data, insert_id, timestamp):
        logger.info('cargar_datos: {}'.format(tablename))
        try:
            commit = False

            if data.shape[0] == 0:
                raise Exception('no hay datos que cargar')
            
            credentials = service_account.Credentials.from_service_account_info(json.loads(json.dumps(self.service_account)))
            bigquery_client = bigquery.Client(credentials=credentials, project=self.project_id)
            dataset_ref = bigquery_client.dataset(self.dataset)
            table_ref = dataset_ref.table(tablename)
            table = bigquery_client.get_table(table_ref)
            status, tmp_name = self.__create_temporal(bigquery_client, tablename)

            if status != 'OK':
                raise Exception(tmp_name)

            # hago la precarga de datos
            commit = True
            json_datos = json.loads(data.to_json(orient='records'))

            datos_bgquery = self.obtejer_json_formato_bigquery(json_datos)

            status, result = self.cargar_datos_desde_json(tmp_name, datos_bgquery)

            if status != 'OK':
                raise Exception(result)

            # persisto la info en comun 
            status, result = self.__update_rows(bigquery_client, tmp_name,
                                               tablename, table.schema,
                                               insert_id, timestamp)
            if status != 'OK':
                raise Exception(result)

            # integro el conjunto nuevo
            status, result = self.__insert_rows(bigquery_client ,tmp_name
                                                ,tablename, table.schema
                                                ,insert_id)

            if status != 'OK':
                raise Exception(result)

            # elimino tabla temporal
            status, result = self.__borrar_temporal(bigquery_client, tmp_name)

            if status != 'OK':
                raise Exception(result)

            return True, ''

        except Exception as ex:
            if commit:
                logger.error(str(ex))
                status, result = self.__borrar_temporal(bigquery_client, tmp_name)
                return False, str(ex)

            else:
                logger.warning(str(ex))
                return True, str(ex)


    def __create_table_from_query(self, bigquery_client, query, tablename):
        logger.info('__create_table_from_query')
        try:
            # configuro para exportar hacia tabla
            job_config = bigquery.QueryJobConfig()
            destination_dataset = bigquery_client.dataset(self.dataset)
            destination_table = destination_dataset.table(tablename)
            job_config.destination = destination_table
            job_config.create_disposition = 'CREATE_IF_NEEDED'
            job_config.write_disposition = 'WRITE_TRUNCATE'

            # ejecuto el query
            query_job = bigquery_client.query(query
                                            ,location=self.location
                                            ,job_config=job_config)
            query_job.result()  # Waits for the query to finish                                            

            return 'OK', ''

        except Exception as ex:
            logger.error(str(ex))
            return 'ERROR', str(ex)


    def __create_file_from_table(self, bigquery_client, tablename, filenames):
        logger.info('__create_file_from_table')
        try:

            destination_uris = []

            for filename in filenames:
                destination_uris.append('gs://{}/{}/{}'.format(self.project_id, self.dataset, filename))

            dataset_ref = bigquery_client.dataset(self.dataset, project=self.project_id)
            table_ref = dataset_ref.table(tablename)
            extract_job = bigquery_client.extract_table(
                table_ref,
                destination_uris,
                location=self.location)  # API request
            extract_job.result()  # Waits for job to complete.

            return 'OK', ''

        except Exception as ex:
            logger.error(str(ex))
            return 'ERROR', str(ex)


    def extraer_hacia_cloud_storage(self, query, tablename, filenames):
        logger.info('extraer_hacia_cloud_storage')
        try:
            # inicializo el cliente
            credentials = service_account.Credentials.from_service_account_info(json.loads(json.dumps(self.service_account)))
            bigquery_client = bigquery.Client(credentials=credentials, project=self.project_id)

            # genero tabla intermedia
            status, result = self.__create_table_from_query(bigquery_client, query, tablename)

            if status != 'OK':
                raise Exception(result)

            # exporto contenido de tabla intermedia hacia cloud storage
            status, result = self.__create_file_from_table(bigquery_client, tablename, filenames.split(','))

            if status != 'OK':
                raise Exception(result)

            return 'OK', ''

        except Exception as exc:
            exc_output = sys.exc_info()
            logger.error('{},{}'.format(str(exc),str(exc_output[2].tb_lineno)))
            return 'FAIL', str(exc)


    
