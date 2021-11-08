#-*- coding: utf-8 -*-

# Norman Sánchez - miércoles, 08 de Mayo de 2019 (GMT-5)
# Clase de validaciones generales

from logging import getLogger, basicConfig
from database_services.mysql_service import MySQLConnection
from database_services.firebase_connection import FBConnection
from difflib import SequenceMatcher as SM
from cedis import Cedis
import json, sys

logger = getLogger()

class Validaciones:
    def __init__(self):
        self.my_sql_conn = MySQLConnection('config.ini')

    ## Valida el usuario y el password de Mobility
    def validar_user_pass_mob(self, cedi, password):
        try:
            logger.info('validate_cedi')
            logger.info('Los datos enviados {}, {}'.format(cedi, password))
            args = [cedi, password]
            res  = self.my_sql_conn.spexec('sp_api_mob_login', args)

            if res[0][0] != 'OK':
                raise Exception(res[0][1])

            else:
                cedi = json.loads(res[0][1])["cedis"]
                fbmethods = FBConnection('config.ini')
                status, response = fbmethods.validar_token(cedi)
                if not status:
                    raise Exception(response)
                
                return True, json.loads(res[0][1]), response

        except Exception as exc:
            exc_output = sys.exc_info()
            logger.error('{},{}'.format(str(exc),str(exc_output[2].tb_lineno)))
            return False, exc, ""

    ## Valida que exista el Token
    def validar_token_mob(self, id_cedi, tokenviaje):
        try:
            logger.info('validate_token')
            logger.info('Los datos enviados {}, {}'.format(id_cedi, tokenviaje))
            args = [id_cedi, tokenviaje]
    
            res = self.my_sql_conn.spexec('sp_api_mob_validar_token', args)
            
            
            if res[0][0] != 'OK':
                raise Exception(res[0][1])

            else:
                return True, res[0][1]

        except Exception as e:
            logger.error(e)
            return False, str(e)

    ## Asigna el Token a un viaje
    def asignar_token_mob(self, event):
        try:
            logger.info('asignate_token')
            id_cedi  = event['id_cedi']
            id_viaje = event['id_viaje']
            nombre_operador = event['nombre_operador']
            fletera = event['fletera']
            correo_operador = event['correo_operador']
            placas = event['placas']
            numero_economico = event['numero_economico']
            logger.info('Los datos enviados {}, {}, {}, {}, {}, {}, {}'.format(id_cedi, id_viaje, nombre_operador, fletera, correo_operador, placas, numero_economico))

            args = [id_cedi, id_viaje, nombre_operador, fletera, correo_operador, placas, numero_economico]

            res = self.my_sql_conn.spexec('sp_api_mob_tomar_token', args)
     
                
            if res[0][0] != 'OK':
                raise Exception(res[0][1])

            return True, res[0][1]

        except Exception as e:
            logger.error(e)
            return False, str(e)
