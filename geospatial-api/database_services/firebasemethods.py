# -*- coding: utf-8 -*-

# RACH - lunes, 27 de mayo de 2019 (GMT-5)
# para realizar operaciones con firebase



import json
import logging
import sys
import time
from database_services.firebase_connection import FBConnection

logger = logging.getLogger(__name__)

class FirebaseMethods(FBConnection):
    def __init__(self, configfile):
        super(FirebaseMethods, self).__init__(configfile)
    
    def crear_viaje(self, cedi_id, viaje_id, json_viaje):
        logger.debug('crear_viaje')
        try:
            inten = 1
            while inten <= 5:
                key = self.nodo_viajes(cedi_id, viaje_id)
                self.firebase_set(key, json_viaje)
                json_response = self.firebase_get(key)

                if json_response != json_viaje:
                    inten += 1
                    time.sleep(1)

                else:
                    inten = 6
            return True, ''
        except Exception as exc:
            exc_output = sys.exc_info()
            logger.error('{},{}'.format(str(exc),str(exc_output[2].tb_lineno)))
            return False, str(exc)
    
    def leer_transacciones(self, cedi_id, viaje_id):
        logger.debug('leer_transacciones')
        try:
            json_transacciones = self.firebase_get(self.nodo_transacciones(cedi_id, viaje_id))

            if not json_transacciones:
                json_transacciones = {}

            return True, json_transacciones

        except Exception as exc:
            logger.error(exc)
            return False, str(exc)

    def escribir_transacciones(self, cedi_id, viaje_id, json_viaje):
        logger.debug('escribir_viaje')
        try:
            inten = 1

            while inten <= 5:
                key = self.nodo_transacciones(cedi_id, viaje_id)
                self.firebase_set(key, json_viaje)
                json_response = self.firebase_get(key)

                if json_response != json_viaje:
                    logger.warning('jsons differs... retrying')
                    inten += 1
                    time.sleep(1)

                else:
                    inten = 6

            return True, ''

        except Exception as exc:
            logger.error(exc)
            return False, str(exc)
    
    def leer_incidentes(self, cedi_id, viaje_id):
        logger.debug('leer_incidentes')
        try:
            json_incidentes =self.firebase_get(self.nodo_incidentes(cedi_id, viaje_id))

            if not json_incidentes:
                json_incidentes = {}

            return True, json_incidentes

        except Exception as exc:
            logger.error(exc)
            return False, str(exc)

    def escribir_incidentes(self, cedi_id, viaje_id, json_incidentes):
        logger.debug('escribir_incidentes')
        try:
            inten = 1

            while inten <= 5:
                key = self.nodo_incidentes(cedi_id, viaje_id)
                self.firebase_set(key, json_incidentes)
                json_response = self.firebase_get(key)

                if json_response != json_incidentes:
                    inten += 1
                    time.sleep(1)

                else:
                    inten = 6

            return True, ''

        except Exception as exc:
            logger.error(exc)
            return False, str(exc)
    
    def escribir_incidente(self, cedi_id, viaje_id, uid, json_incidente):
        logger.debug('escribir_incidente')
        try:
            inten = 1
            
            while inten <= 5:
                key = '/'.join(
                    [self.nodo_incidentes_rt(cedi_id, viaje_id)
                    ,str(uid)])
                self.firebase_set(key, json_incidente)
                json_response = self.firebase_get(key)

                if json_response != json_incidente:
                    logger.warning('jsons differs... retrying')
                    inten += 1
                    time.sleep(1)

                else:
                    inten = 6

            return True, 'OK'

        except Exception as exc:
            logger.error(exc)
            return False, str(exc)
    
    def get(self, key, default=None):
        logger.debug('get')
        try:
            json_response = self.firebase_get(key)

            if not json_response:
                raise Exception('no existe: {key}'.format(key=key))

            return json_response

        except Exception as exc:
            logger.warning(str(exc))
            return default