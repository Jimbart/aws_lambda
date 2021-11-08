#-*- coding: utf-8 -*-

from logging import getLogger, basicConfig
import configparser
import redis
import boto3
import pandas as pd
import numpy as np
import base64
import sys

import json
import urllib

from database_services.mysql_service import MySQLConnection
from datetime import datetime
import collections

logger = getLogger()

class Arcos:
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read('config.ini')

    def validar_arcos(self, id_cedi, combinaciones):
        '''Validar existencia de arcos'''
        try:
            logger.info('obtener los arcos')
            host = self.config.get('redis', 'url')
            r = redis.StrictRedis(host=host, port=6379)
            list_keys = r.keys('*')
            
            x = list()
            for comb in combinaciones:
                x.append(str(id_cedi)+'_'+comb[0]+'_'+comb[1])

            
            diff = (list(set(x) - set(list_keys)))
            
            return True, diff
            

        except Exception as exc:
            logger.error(str(exc))
            return False, str(exc)

    def get_secret(self):
        """Para obtener el secreto"""
        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=self.config.get('GEOCODING', 'region_name')
        )
        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=self.config.get('GEOCODING', 'secret_name')
            )
            secret = json.loads(get_secret_value_response.get('SecretString'))
            return True, secret
        except Exception as exc:
            exc_response = sys.exc_info()
            logger.error('line: {}, {}'.format(exc_response[2].tb_lineno, str(exc)))
            return None

    def validar(self, id_cedi, destinos, id_usuario, id_empresa):
        destinosFaltantes = list()
        status, diff = self.obtener_keys_faltantes(id_cedi, destinos)
        creditos_suficientes = True
        
        if status:
            numFaltantes = len(diff)

            if numFaltantes > 0:
                destinosFaltantes = self.obtener_mayores_coincidencias(diff)
                destinosFaltantes = json.loads(destinosFaltantes)

                tipo_arco = self.obtener_tipo_arco(id_cedi)
                if tipo_arco == 0:
                    logger.info("EL TIPO DE ARCO ES GOOGLE")
                    creditos_suficientes = self.verificar_disponibilidad(id_usuario, id_empresa, numFaltantes)

        return status, numFaltantes, destinosFaltantes, creditos_suficientes
    
    def obtener(self, id_cedi, destinos, id_usuario, id_empresa):
        status, diff = self.obtener_keys_faltantes(id_cedi, destinos)

        return status, diff
    
    def verificar_disponibilidad(self, id_usuario, id_empresa, numero_arcos):
        logger.info("verificar_disponibilidad")
        
        logger.info("id_usuario")
        logger.info(id_usuario)
        logger.info("id_empresa")
        logger.info(id_empresa)
        logger.info("NUMERO DE DESTINOS:")
        logger.info(numero_arcos)

        try:
            my_sql_conn = MySQLConnection('config.ini')
            args = [id_usuario, id_empresa, numero_arcos]
            status = False

            result = my_sql_conn.spexec('sp_validar_creditos_arcos', args)
            logger.info("RESULTADO DEL SP")
            logger.info(result)

            if result[0][0] == 'OK':
                status = True

            return status

        except Exception as exc:
            logger.error("verificar_disponibilidad")
            exc_response = sys.exc_info()
            logger.error('line: {}, {}'.format(exc_response[2].tb_lineno, str(exc)))
            return False
    
    def calcular_arcos(self, id_cedi, destinos, UUID, id_usuario, id_empresa):
        logger.error("INICIA LA LLAMADA PRINCIPAL")
        status, diff = self.obtener_keys_faltantes(id_cedi, destinos)
        
        if status:
            if len(diff) > 0:
                logger.error("HAY DISTANCIAS POR CALCULAR")
                status = self.mandar_sqs(UUID, diff, id_usuario, id_empresa)

            logger.error("FIN DE LA LLAMADA PRINCIPAL")
        return status
    
    def obtener_keys_faltantes(self, id_cedi, destinos):
        try:
            # DESCOMENTAR PARA PRUEBAS LOCALES
            # -------------------------------------------- #
            # destinos = json.loads(destinos)
            # -------------------------------------------- #
            logger.error("INICIA OBTENER KEYS FALTANTES")
            status, keysDistancias = self.obtener_distancias_redis(id_cedi) # SI FALLA, HACE UN RAISE DESDE ESTA FUNCION
            
            diff = list()

            if status:
                logger.error("LLAMADA A OBTENER FECHAS DESTINOS")
                status, fechas_destinos = self.obtener_fechas_destinos(id_cedi, destinos)
                logger.error("PASANDO A JSON LAS FECHAS")
                fechas_destinos = json.loads(fechas_destinos)

                logger.error("LAS FECHAS DE LOS DESTINOS")
                logger.error(fechas_destinos)

                logger.error("PASANDO FECHAS A DATAFRAME")
                dfFechas = pd.DataFrame.from_dict(fechas_destinos)
                dfFechas['idDestino'] = dfFechas['idDestino'].astype(str)

                if status and fechas_destinos:
                    logger.error("LLAMADA A CALCULAR LA MATRIZ")
                    matrizDestinos = self.calcular_matriz(id_cedi, destinos)

                    logger.error("SACANDO LISTAS")
                    diff = (list(set(matrizDestinos) - set(keysDistancias)))
                    
                    # RESTANTES SON TODOS LOS DESTINOS POR SI NO HAY DESTINOS EN LA DIFERENCIA Y NO PONER UN ELSE
                    restantes = list(set(matrizDestinos))
                    # SI HAY ALGO EN LA DIFERENCIA, OBTENGO LOS RESTANTES SOLAMENTE (YA SE QUE DEBO CALCULAR LOS DE LA DIFERENCIA)
                    if diff: 
                        restantes = (list(set(matrizDestinos) - set(diff)))

                    logger.error("LOS KEYS RESTANTES")
                    logger.error(restantes)

                    if restantes:
                        dfKeysData = self.obtener_datos_redis(restantes)

                        dfRestantes = self.splitearKeys(restantes)
                        dfDataCompleto = pd.concat([dfRestantes, dfKeysData], axis=1)
                        
                        dfDataCompleto = pd.merge(dfDataCompleto, dfFechas, on='idDestino')

                        logger.error("FORMATEANDO FECHA DE DB")
                        dfDataCompleto['fechaActualizacion'] =  pd.to_datetime(dfDataCompleto['fechaActualizacion'], format='%Y-%m-%d %H:%M:%S.%f')
                        dfDataCompleto.rename(columns={'fechaActualizacion':'FechaActualizacionDB'}, inplace=True)
                        
                        # dfDataCompleto = pd.concat([dfRestantes, dfKeysData], axis=1)

                        logger.error("DESPUES DEL MERGE")
                        # with pd.option_context('display.max_rows', None, 'display.max_columns', None):
                        #     print (dfDataCompleto)
                        # logger.error("------------------------------------------------->>")

                        logger.error("VERIFICANDO FECHAS")
                        
                        # logger.error("/////////////////////////////")
                        # logger.error(dfDataCompleto['FechaActualizacionRedis'])
                        # logger.error(dfDataCompleto['FechaActualizacionDB'])
                        # logger.error(dfDataCompleto['key'])
                        # logger.error("/////////////////////////////")

                        dfDataCompleto['recalcular'] = np.where((dfDataCompleto['FechaActualizacionRedis'] < dfDataCompleto['FechaActualizacionDB']), 1, 0)
                        logger.error("...............----------.............")
                        logger.error("DESPUES DE VERIFICAR FECHAS")
                        # with pd.option_context('display.max_rows', None, 'display.max_columns', None):
                        #     print (dfDataCompleto)
                        # logger.error("...............----------.............")
                        logger.error(dfDataCompleto)
                        dfParaCalcular = dfDataCompleto.loc[dfDataCompleto['recalcular'] == 1]
                        logger.error(dfParaCalcular)
                        logger.error(".....--------..........")
                        if not dfParaCalcular.empty:
                            logger.error("PASANDO A LISTA OTRA VEZ")
                            json_calcular = dfParaCalcular['key'].tolist()
                            diff = diff + json_calcular
                        else:
                            logger.error("ESTA VACIO EL DF")

            logger.error(diff)
            return status, diff
        except Exception as exc:
            logger.error("Funcion: obtener_keys_faltantes")
            logger.error(exc)
            raise Exception(exc)

    def obtener_distancias_redis(self, id_cedi):
        try:
            logger.error("INICIA LA OBTENCION DE DISTANCIAS DE REDIS")

            tipo_arco = self.obtener_tipo_arco(id_cedi)
            patron = ""

            if tipo_arco == 0:
                logger.info("EL TIPO DE ARCO ES GOOGLE")
                patron = str(id_cedi) + "_*"
            else:
                logger.info("EL TIPO DE ARCO ES OSRM")
                patron = "osrm_" + str(id_cedi) + "_*"
            
            logger.error(patron)
            # config = configparser.ConfigParser()
            # config.read('config.ini')
            host = self.config.get('redis', 'url')
            logger.error(host)
            r = redis.StrictRedis(host=host, port=6379, charset="utf-8", decode_responses=True)
            logger.error("LLAMADA A REDIS")
            logger.error(r)
            # keys = r.keys()

            keys = r.keys(pattern=patron)

            logger.error("FIN LLAMADA REDIS")
            return True, keys

        except Exception as exc:
            logger.error("Funcion: obtener_distancias_redis")
            logger.error(exc)
            raise Exception(exc)
    
    def obtener_datos_redis(self, keys):
        try:
            logger.error("INICIA LA OBTENCION DE DATOS DE REDIS")
            # config = configparser.ConfigParser()
            # config.read('config.ini')
            host = self.config.get('redis', 'url')
            r = redis.StrictRedis(host=host, port=6379, charset="utf-8", decode_responses=True)
            logger.error("LLAMADA A REDIS")
            logger.error("ENTRADA")
            logger.error(keys)
            logger.error("///////----//////")

            keys_data = r.mget(keys)
            
            logger.error("SALIDA")
            logger.error(keys_data)

            dfKeysData = pd.DataFrame(keys_data)
            logger.error(dfKeysData)
            logger.error("CAMBIANDO NOMBRE DE COLUMNA")
            dfKeysData.columns = ['data']
            logger.error("REEMPLAZANDO COMILLA SENCILLA POR DOBLE")
            dfKeysData.data = dfKeysData.data.str.replace("'",'"')
            logger.error("HACIENDO LOADS")
            dfKeysData.data = dfKeysData.data.apply(json.loads)
            
            logger.error(dfKeysData)

            # LO PASO A LISTA Y LUEGO A DATAFRAME OTRA VEZ PORQUE EN LAMBDA NO SIRVE EL json_normalize
            # --------------------------------------------------------------------------------- #
            list_keys_data = dfKeysData['data'].tolist()
            logger.error(list_keys_data)
            dfKeysData = pd.DataFrame.from_dict(list_keys_data)
            # --------------------------------------------------------------------------------- #
            
            dfKeysData.rename(columns={'FechaActualizacion':'FechaActualizacionRedis'}, inplace=True)
            logger.error("FORMATEANDO FECHA DE REDIS")
            dfKeysData['FechaActualizacionRedis'] =  pd.to_datetime(dfKeysData['FechaActualizacionRedis'], format='%Y-%m-%d %H:%M:%S.%f')
            with pd.option_context('display.max_rows', 100, 'display.max_columns', 100):
                print (dfKeysData)
            print ("# --------------------------------------------------------------------------------- #")

            logger.error("FIN LLAMADA REDIS")
            return dfKeysData

        except Exception as exc:
            logger.error("Funcion: obtener_datos_redis")
            logger.error(exc)
            raise Exception(exc)
    
    def calcular_matriz(self, id_cedi, destinos):
        try:
            logger.error("INICIA EL CALCULO DE LA MATRIZ")
            matrizDestinos = list()
            # SE AGREGA EL CEDI COMO UN DESTINO MAS PARA LA MATRIZ
            destinos.append(id_cedi)

            # for origen in destinos:
            #     for destino in destinos:
            #         distancia = dict()
            #         key = str(id_cedi) + "_" + str(origen) + "_" + str(destino)
            #         matrizDestinos.append(key)

            logger.error("CALCULANDO LA MATRIZ CON PANDAS")
            x1 = pd.DataFrame(destinos, columns=['idDestino'])
            x1['key'] = 1
            dfArcos = pd.merge(x1, x1, on='key')
            dfArcos["idDestino_x"] = dfArcos["idDestino_x"].astype(str)
            dfArcos["idDestino_y"] = dfArcos["idDestino_y"].astype(str)

            logger.error("OBTENIENDO TIPO DE ARCO")
            tipo_arco = self.obtener_tipo_arco(id_cedi)

            if tipo_arco == 1:
                dfArcos["key"] = "osrm" + "_" + str(id_cedi) + "_" +  dfArcos["idDestino_x"] + "_" + dfArcos["idDestino_y"]
            else:
                dfArcos["key"] = str(id_cedi) + "_" +  dfArcos["idDestino_x"] + "_" + dfArcos["idDestino_y"]

            logger.error("OBTENIENDO LISTA DE KEYS")
            matrizDestinos = dfArcos['key'].tolist()
            logger.error(matrizDestinos)

            logger.error("FIN DEL CALCULO DE LA MATRIZ")
            
            return matrizDestinos

        except Exception as exc:
            logger.error("Funcion: calcular_matriz")
            logger.error(exc)
            raise Exception(exc)
    
    def obtener_tipo_arco(self, id_cedi):
        try:
            logger.error('obtener_tipo_arco')
            my_sql_conn = MySQLConnection('config.ini')
            args = [id_cedi]

            result = my_sql_conn.spexec('sp_api_arms_obtener_tipo_arco', args)
            logger.error('resultado de la consulta')

            if result[0][0] != 'OK':
                raise Exception(result[0][1])

            return result[0][1]

        except Exception as exc:
            logger.error("Funcion: obtener_tipo_arco")
            logger.error(exc)
            raise Exception(exc)
    
    def obtener_mayores_coincidencias(self, diff):
        try:
            dfDiff = self.splitearKeys(diff)
            df1 = dfDiff[['idOrigen']]
            df2 = dfDiff[['idDestino']]
            df1.columns = ['data']
            df2.columns = ['data']

            dfTotal = df1.append(df2)
            dfConteo = dfTotal.data.value_counts()
            json_conteo = dfConteo.to_json()
            
            return json_conteo

        except Exception as exc:
            logger.error("Funcion: obtener_mayores_coincidencias")
            logger.error(exc)
            raise Exception(exc)

    def splitearKeys(self, lista):
        dfLista = pd.DataFrame(lista)
        logger.error("SPLITEANDO KEYS")
        dfLista.columns = ['key']
        newColumns = dfLista['key'].str.split("_", expand = True)
        dfLista['idOrigen'] = newColumns[1]
        dfLista['idDestino'] = newColumns[2]

        return dfLista
    
    def mandar_sqs(self, UUID, parejas, id_usuario, id_empresa):
        try:
            logger.error("INICIA EL ENCOLADO A SQS")
            sqs = boto3.client('sqs', region_name='us-west-2')
            queue_url = self.config.get('SQS', 'urlArcos')

            success = False
            
            logger.error("HACIENDO CHUNKS")
            dfParejas = pd.DataFrame(parejas)
            n = 10000  #chunk row size
            dfParejasChunks = [dfParejas[i:i+n] for i in range(0,dfParejas.shape[0],n)]
            
            i=0
            for dfChunk in dfParejasChunks:
                logger.error("PASAR A JSON LAS PAREJAS")
                data = dict()
                data["id_usuario"] = id_usuario
                data["id_empresa"] = id_empresa
                data["parejas"] = list(dfChunk[0])
                # data["test"] = test
                
                data = json.dumps(data)
                logger.error("DATA")
                logger.error(data)
                UUIDstr = str(UUID)
                UUIDstrD = UUIDstr + "-" + str(i)
                i = i +1
                logger.error("UUID")
                logger.error(UUIDstr)
                logger.error("UUIDD")
                logger.error(UUIDstrD)
                logger.error("UUID COMO STRING")
                logger.error("INICIA LA LLAMADA A SQS")
                logger.error(queue_url)
                response = sqs.send_message(
                    QueueUrl=queue_url,
                    MessageBody= (
                        data
                    ),
                    MessageGroupId = UUIDstr,
                    MessageDeduplicationId = UUIDstrD,
                    MessageAttributes = {
                        'UUID': {
                            'DataType': 'String',
                            'StringValue': UUIDstr
                        }
                    }
                )
                logger.error("TERMINA LA LLAMADA A SQS")
    
                if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                    logger.error("HUBO RESPUESTA SATISFACTORIA DE SQS")
                    success = True
                else:
                    logger.error("Funcion: mandar_sqs")
                    logger.error(response)
                
            logger.error("FIN DEL ENCOLADO A SQS")
            return success

        except Exception as exc:
            logger.error("Funcion: mandar_sqs")
            logger.error(exc)
            raise Exception(exc)
    
    def obtener_errores(self, UUID):

        try:
            logger.error("INICIA OBTENER ERRRORES DE SQS")
            # config = configparser.ConfigParser()
            # config.read('config.ini')

            sqs = boto3.client('sqs', region_name='us-west-2')
            queue_url = self.config.get('SQS', 'urlArcosErrores')

            UUIDstr = str(UUID)
            logger.error("UUID COMO STRING")
            logger.error("INICIA LA LLAMADA A SQS")
            print (queue_url)
            response = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10,
                AttributeNames=[
                    'MessageGroupId'
                ],
                VisibilityTimeout=0,
                WaitTimeSeconds=0
            )
            logger.error("RESPUESTA RECEIVE MESSAGE")
            logger.error(response)

            errores = list()

            if 'Messages' in response:
                for mensaje in response['Messages']:
                    idGrupo = mensaje['Attributes']['MessageGroupId']
                    if idGrupo == UUIDstr:
                        errores.append(mensaje['Body'])

            logger.info("TERMINA OBTENER ERRRORES DE SQS")

            return True, errores

        except Exception as exc:
            logger.error("Funcion: obtener_errores")
            logger.error(exc)
            raise Exception(exc)

    def obtener_fechas_destinos(self, id_cedi, destinos):
        try:
            logger.error('obtener_fechas_destinos')
            my_sql_conn = MySQLConnection('config.ini')
            args = [id_cedi, json.dumps(destinos)]

            result = my_sql_conn.spexec('sp_api_arms_obtener_fecha_actualizacion_destinos', args)
            logger.error('resultado de la consulta')

            if result[0][0] != 'OK':
                raise Exception(result[0][1])

            return True, result[0][1]

        except Exception as exc:
            logger.error('Funcion: obtener_fechas_destinos')
            logger.error(exc)
            raise Exception(exc)