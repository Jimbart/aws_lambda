#-*- coding: utf-8 -*-

from logging import getLogger, basicConfig
from database_services.mysql_service import MySQLConnection

import json
import configparser

logger = getLogger()

class Plan:
	def __init__(self):
		self.my_sql_conn = MySQLConnection('config.ini')

	def listar_planes(self, id_cedi):
		try:
			logger.info('validar_destinos')
			args = [id_cedi]
			res  = self.my_sql_conn.spexec('sp_api_arms_listar_planes', args)
			
			if res[0][0] != 'OK':
				raise Exception(res[0][1])
			
			return True, json.loads(res[0][1])
		except Exception as exc:
			logger.error('Ocurrio un error al listar los planes')
			logger.error(exc)
			return False, str(exc)