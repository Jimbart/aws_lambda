#-*- coding: utf-8 -*-

from logging import getLogger, basicConfig
from database_services.mysql_service import MySQLConnection

import json
import configparser

logger = getLogger()

class Catalogos:
    def __init__(self):
        self.my_sql_conn = MySQLConnection('config.ini')

    def validar_destinos(self, id_cedi, destinos):
        try:
            logger.info('validar_destinos')
            args = [json.dumps(destinos), id_cedi]
            res  = self.my_sql_conn.spexec('sp_api_arms_validar_destinos', args)

            if res[0][0] != 'OK':
                raise Exception(res[0][1])

            return True, json.loads(res[0][1]), res[0][2]
            
        except Exception as e:
            logger.error(e)
            return False, str(e), ''
    
    def validar_usuario(self, cedis, correo_usuario):
        try:
            logger.info('validar_usuario')
            args = [cedis, correo_usuario]
            res  = self.my_sql_conn.spexec('sp_api_arms_login', args)

            if res[0][0] != 'OK':
                raise Exception(res[0][1])

            return True, res[0][1]
        except Exception as e:
            logger.error(e)
            return False, str(e)