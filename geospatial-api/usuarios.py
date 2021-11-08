#-*- coding: utf-8 -*-

from logging import getLogger, basicConfig
from database_services.mysql_service import MySQLConnection

import json
import configparser

logger = getLogger()

class Usuarios:
    def __init__(self, *args, **kwargs):
        self.my_sql_conn = MySQLConnection('config.ini')
    
    def guardar_preferencias_usuario(self, id_usuario, preferencias):
        """guardar_preferencias_usuario"""
        try:
            logger.info('guardar_preferencias_usuario')
            args = [id_usuario, json.dumps(preferencias)]

            res  = self.my_sql_conn.spexec('sp_api_arms_guardar_preferencias_usuario', args)
          
            if res[0][0] != 'OK':
                raise Exception(res[0][1])
            
            return True, res[0][1]
            
        except Exception as exc:
            logger.error('Ocurrio un error al guardar las preferencias del usuario')
            logger.error(exc)
            return False, str(exc)
    
    def actualizar_ss_preferido_usuario(self, id_cedi, id_usuario, spreads):
        """actualizar_ss_preferido_usuario"""
        try:
            logger.info('guardar_preferencias_usuario')
            args = [id_cedi, id_usuario, json.dumps(spreads)]

            res  = self.my_sql_conn.spexec('sp_api_arms_actualizar_ss_preferido_usuario', args)
          
            if res[0][0] != 'OK':
                raise Exception(res[0][1])
            
            return True, res[0][1]
            
        except Exception as exc:
            logger.error('Ocurrio un error al actualizar el SS preferido')
            logger.error(exc)
            return False, str(exc)
    
    