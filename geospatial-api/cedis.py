#-*- coding: utf-8 -*-

from logging import getLogger, basicConfig
from database_services.mysql_service import MySQLConnection

import json
import configparser

logger = getLogger()

class Cedis:
    def __init__(self, *args, **kwargs):
        self.my_sql_conn = MySQLConnection('config.ini')

    def listar_cedis(self, correo):
        """listar_cedis"""
        try:
            logger.info('listar_cedis')
            args = [correo]

            res  = self.my_sql_conn.spexec('sp_api_arms_listar_cedis', args)
            
            if res[0][0] != 'OK':
                raise Exception(res[0][1])

            pertenece = json.loads(res[0][1])
            todos = json.loads(res[0][3])

            agregar = list()

            for cedi in todos:
                encontrado = 0
                for cediP in pertenece:
                    if cedi['idCedi'] == cediP['idCedi']:
                        encontrado = 1
                        break
                if encontrado == 0:
                    agregar.append(cedi)

            completo = pertenece + agregar

            return True, {
                        'cedis': completo
                        ,'usuario': json.loads(res[0][2])
                    }
            
        except Exception as exc:
            logger.info('Ocurrio un error al listar los cedis')
            logger.error(exc)
            return False, str(exc)

    def obtener_preferencias_motor(self, id_cedis, perfil):
        """obtener_preferencias"""
        try:
            logger.info('obtener_preferencias')
            args = [id_cedis, perfil]

            res  = self.my_sql_conn.spexec('sp_api_arms_obtener_preferencias_motor', args)
            
            if res[0][0] != 'OK':
                raise Exception(res[0][1])
            
            cat_preferencias = {
                                    "Heuristica": [
                                    "Minimiza Gasto",
                                    "Minimiza Tiempo",
                                    "Minimiza Co2",
                                    "Minimiza Km",
                                    "Gasto con tarifas"
                                ],
                                "Metaheuristica": [
                                    "Hormiga",
                                    "North",
                                    "West"
                                ],
                                "ValorMax": "18000000",
                                "FacRespetarDiasDestinos": [
                                    "SI",
                                    "NO"
                                ],
                                "FacEntregasUnDia": [
                                    "SI",
                                    "NO"
                                ],
                                "FacVentanaEntrega": [
                                    "SI",
                                    "NO"
                                ],
                                "FacVolumen": [
                                    "SI",
                                    "NO"
                                ],
                                "FacPeso": [
                                    "SI",
                                    "NO"
                                ],
                                "FacValor": [
                                    "SI",
                                    "NO"
                                ],
                                "FacRestriccionVolumen": [
                                    "SI",
                                    "NO"
                                ],
                                "FacCompatibilidad": [
                                    "SI",
                                    "NO"
                                ],
                                "FacDescansos": [
                                    "SI",
                                    "NO"
                                ],
                                "FacComidas": [
                                    "SI",
                                    "NO"
                                ],
                                "FacHoteles": [
                                    "SI",
                                    "NO"
                                ],
                                "DuracionDescanso": "10",
                                "DuracionComida": "10",
                                "DuracionHotel": "10",
                                "IntervaloDescanso": "10",
                                "HoraComida": "14:00"
                                }
            
            return True, {
                        'preferencias': json.loads(res[0][1])
                        ,'perfiles': json.loads(res[0][2])
                        ,'cat_preferencias': cat_preferencias
                    }
            
        except Exception as exc:
            logger.info('Ocurrio un error al listar los cedis')
            logger.error(exc)
            return False, str(exc)
    
    def obtener_preferencias_default_motor(self, id_cedis, perfil):
        """obtener_preferencias"""
        try:
            logger.info('obtener_preferencias')
            args = [id_cedis, perfil]

            res  = self.my_sql_conn.spexec('sp_api_arms_obtener_preferencias_default', args)
            
            if res[0][0] != 'OK':
                raise Exception(res[0][1])
            
            cat_preferencias = {
                                "Heuristica": [
                                    "Minimiza Gasto",
                                    "Minimiza Tiempo",
                                    "Minimiza Co2",
                                    "Minimiza Km",
                                    "Gasto con tarifas"
                                ],
                                "Metaheuristica": [
                                    "Hormiga",
                                    "North",
                                    "West"
                                ],
                                "ValorMax": "18000000",
                                "FacRespetarDiasDestinos": [
                                    "SI",
                                    "NO"
                                ],
                                "FacEntregasUnDia": [
                                    "SI",
                                    "NO"
                                ],
                                "FacVentanaEntrega": [
                                    "SI",
                                    "NO"
                                ],
                                "FacVolumen": [
                                    "SI",
                                    "NO"
                                ],
                                "FacPeso": [
                                    "SI",
                                    "NO"
                                ],
                                "FacValor": [
                                    "SI",
                                    "NO"
                                ],
                                "FacRestriccionVolumen": [
                                    "SI",
                                    "NO"
                                ],
                                "FacCompatibilidad": [
                                    "SI",
                                    "NO"
                                ],
                                "FacDescansos": [
                                    "SI",
                                    "NO"
                                ],
                                "FacComidas": [
                                    "SI",
                                    "NO"
                                ],
                                "FacHoteles": [
                                    "SI",
                                    "NO"
                                ],
                                "DuracionDescanso": "10",
                                "DuracionComida": "10",
                                "DuracionHotel": "10",
                                "IntervaloDescanso": "10",
                                "HoraComida": "14:00"
                                }
            
            return True, {
                        'perfiles': json.loads(res[0][5]),
                        'motor': {
                            'preferencias': json.loads(res[0][1]),
                            'cat_preferencias': cat_preferencias
                        },
                        'pedidos_default': json.loads(res[0][2]),
                        'destinos_default': json.loads(res[0][3]),
                        'vehiculos_default': json.loads(res[0][4])
                    }
            
        except Exception as exc:
            logger.info('Ocurrio un error al listar los cedis')
            logger.error(exc)
            return False, str(exc)
    
    def obtener_preferencias_usuario(self, id_cedis, correo):
        """obtener_preferencias_usuario"""
        try:
            logger.info('obtener_preferencias_usuario')
            args = [id_cedis, correo]
            print(args)

            res  = self.my_sql_conn.spexec('sp_api_arms_obtener_preferencias_usuario', args)

            print(res)            
            if res[0][0] != 'OK':
                raise Exception(res[0][1])
            
            print(type(res[0][1]))
            return True, json.loads(res[0][1])
            
        except Exception as exc:
            logger.error('Ocurrio un error al listar los cedis')
            logger.error(exc)
            return False, str(exc)
    
    def obtener_vehiculos(self, id_cedis):
        """obtener vehiculos por cedis"""
        try:
            logger.info('obtener_vehiculos')
            args = [id_cedis]

            res  = self.my_sql_conn.spexec('sp_api_arms_obtener_vehiculos_por_cedis', args)
            
            if res[0][0] != 'OK':
                raise Exception(res[0][1])
            
            
            return True, json.loads(res[0][1])
            
        except Exception as exc:
            logger.info('Ocurrio un error al listar los cedis')
            logger.error(exc)
            return False, str(exc)

    def registrar_vehiculos(self, id_cedi, vehiculos):
        """registrar vehiculos por cedi"""
        try:
            logger.info('registrar_vehiculos')
            args = [id_cedi, json.dumps(vehiculos)]

            res  = self.my_sql_conn.spexec('sp_api_arms_registrar_vehiculos', args)
            
            if res[0][0] != 'OK':
                raise Exception(res[0][1])
            
            
            return True, res[0][1]
            
        except Exception as exc:
            logger.info('Ocurrio un error al registrar los vehiculos')
            logger.error(exc)
            return False, str(exc)

    def registrar_destinos(self, id_cedi, destinos):
        """registrar destinos por cedi"""
        try:
            logger.info('registrar_destinos')
            args = [id_cedi, json.dumps(destinos)]

            res  = self.my_sql_conn.spexec('sp_api_arms_registrar_destinos', args)
            
            if res[0][0] != 'OK':
                raise Exception(res[0][1])
            
            
            return True, res[0][1]
            
        except Exception as exc:
            logger.info('Ocurrio un error al registrar los vehiculos')
            logger.error(exc)
            return False, str(exc)

    def obtener_destinos(self, id_cedis):
        """obtener destinos por cedis"""
        try:
            logger.info('obtener_destinos')
            args = [id_cedis]

            res  = self.my_sql_conn.spexec('sp_api_arms_obtener_destinos_por_cedis', args)
            
            if res[0][0] != 'OK':
                raise Exception(res[0][1])
            
            
            return True, json.loads(res[0][1])
            
        except Exception as exc:
            logger.info('Ocurrio un error al listar los cedis')
            logger.error(exc)
            return False, str(exc)
    
    def guardar_preferencias_cedi(self, id_cedi, perfil, preferencias):
        """guardar_preferencias_cedi"""
        try:
            logger.info('guardar_preferencias_cedi')
            args = [id_cedi, perfil, json.dumps(preferencias)]

            res  = self.my_sql_conn.spexec('sp_api_arms_guardar_preferencias_cedi', args)
          
            if res[0][0] != 'OK':
                raise Exception(res[0][1])
            
            return True, res[0][1]
            
        except Exception as exc:
            logger.error('Ocurrio un error al guardar las preferencias del cedi')
            logger.error(exc)
            return False, str(exc)