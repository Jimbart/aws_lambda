# -*- coding: utf-8 -*-

# Norman Sánchez - miércoles, 08 de Mayo de 2019 (GMT-5)
# Maneja la conección a Firebase.

import os, sys
import configparser
import logging
import firebase_admin
from firebase_admin import credentials, auth
from firebase_admin import db
import json
import boto3
import base64
from botocore.exceptions import ClientError


logger = logging.getLogger(__name__)


class FBConnection(object):

    """Clase de conexion a Firebase. Todas las clases que se conectan a Firebase deben heredar de esta clase"""
    def __init__(self, configfile):
        env, service_account, database_url = self.parseconfig(configfile)
        cred = credentials.Certificate(service_account)
        self.env = env
        if (not len(firebase_admin._apps)):
            firebase_admin.initialize_app(cred,{'databaseURL':database_url})


    @staticmethod
    def parseconfig(configfile):
        """Configuracion de la conexion"""
        logger.debug('parseconfig')
        try:

            # get secret config from config.inis
            config = configparser.ConfigParser()
            config.read(configfile)

            # get secret from secrets manager
            session = boto3.session.Session()
            client = session.client(
                service_name='secretsmanager',
                region_name=config.get('firebase', 'region_name')
            )
            get_secret_value_response = client.get_secret_value(
                SecretId=config.get('firebase', 'secret_name')
            )
            secret = json.loads(get_secret_value_response.get('SecretString'))


            env = secret.get('database')
            service_account = secret
            database_url = secret.get('database_url')
            return env, service_account, database_url
        except Exception as exc:
            exc_response = sys.exc_info()
            logger.error('line: {}, {}'.format(exc_response[2].tb_lineno, str(exc)))
            return None

    def firebase_get(self, key):
        firebase_admin.get_app()
        return db.reference(key).get()


    def firebase_delete(self, key):
        firebase_admin.get_app()
        return db.reference(key).delete()        


    def firebase_set(self, key, value):
        firebase_admin.get_app()
        return db.reference(key).set(value)


    def nodo_config(self, cedi_id):
        return '{env}/{cedi}'.format(env=self.env, cedi=cedi_id)


    def nodo_viajes(self, cedi_id, viaje_id):
        return '{env}/{cedi}/viajes/{viaje}'.format(env=self.env, cedi=cedi_id, viaje=viaje_id)

    def nodo_destinos_plan(self, cedi_id, viaje_id):
        return '{env}/{cedi}/viajes/{viaje}/destinos'.format(env=self.env, cedi=cedi_id, viaje=viaje_id)

    def nodo_transacciones(self, cedi_id, viaje_id):
        return'{env}/{cedi}/transacciones/{viaje}'.format(env=self.env, cedi=cedi_id, viaje=viaje_id)


    def nodo_incidentes(self, cedi_id, viaje_id):
        return'{env}/{cedi}/incidentes/{viaje}'.format(env=self.env, cedi=cedi_id, viaje=viaje_id)

    def nodo_gastos(self, cedi_id, viaje_id):
        return'{env}/{cedi}/gastos/{viaje}'.format(env=self.env, cedi=cedi_id, viaje=viaje_id)

    def nodo_actividades(self, cedi_id, viaje_id):
        return'{env}/{cedi}/actividades/{viaje}'.format(env=self.env, cedi=cedi_id, viaje=viaje_id)

    def nodo_incidentes_rt(self, cedi_id, viaje_id):
        return '{env}/{cedi}/incidentes-RT/{viaje}/'.format(env=self.env, cedi=cedi_id, viaje=viaje_id)

    def nodo_operador_ranking(self, cedi_id):
        return '{env}/{cedi}/operador_ranking/'.format(env=self.env, cedi=cedi_id)

    def nodo_tracking_viajes(self, cedi_id, viaje_id):
        return '{env}/{cedi}/tracking-viajes/{viaje}/'.format(env=self.env, cedi=cedi_id, viaje=viaje_id)

    def nodo_info_viaje(self, cedi_id, viaje_id):
        return '{env}/{cedi}/transacciones/{viaje}/info-viaje/'.format(env=self.env, cedi=cedi_id, viaje=viaje_id)

    def nodo_operador_datos(self, cedi_id, nombre_operador):
        return '{env}/{cedi}/operador_ranking/{nombre}/'.format(env=self.env, cedi=cedi_id, nombre=nombre_operador)
    
    def nodo_destinos_real(self, cedi_id, viaje_id):
        return '{env}/{cedi}/transacciones/{viaje}/destinos/'.format(env=self.env, cedi=cedi_id, viaje=viaje_id)

    def validar_token(self, cedi):
        try:
            logger.info('validar_token, cedi {}'.format(cedi))
            token = auth.create_custom_token(cedi)
            return True, token

        except Exception as exc:
            logger.error('Hubo un error en validar_token {}'.format(str(exc)))
            exc_output = sys.exc_info()
            logger.error('{},{}'.format(str(exc),str(exc_output[2].tb_lineno)))
            return False, str(exc)
