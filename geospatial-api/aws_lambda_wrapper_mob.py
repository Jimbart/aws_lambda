# -*- coding: utf-8 -*-

# Norman Sánchez - miércoles, 08 de Mayo de 2019 (GMT-5)
# para concentrar los microservicios de mobility

from argparse import ArgumentParser
from logging import basicConfig
from validaciones import Validaciones
from viajes import ViajeMobility
from viajes import ViajeIncidente
import json, sys

import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def set_logger(debug=False):
    """Sets the logger configuration"""
    try:
        logformat = "[ %(levelname)s ] - %(asctime)-15s :: %(message)s"
        basicConfig(format=logformat)
        if debug:
            logger.setLevel(10)

    except Exception as exc:
        logger.error(exc)

def get_params():
    """Para obtener los parametros en la consola"""
    try:
        logger.info('get_params')
        parser = ArgumentParser(description='ejecucion manual')
        parser.add_argument('json_test', type=str, metavar='<json_test>', help='')
        parser.add_argument('--debug', action="count", default=0, help='')
        args = parser.parse_args()
        return (
            json.loads(args.json_test), None, args.debug
        )
    except Exception as exc:
        exc_output = sys.exc_info()
        logger.error('{},{}'.format(str(exc),str(exc_output[2].tb_lineno)))
        return {
            'status'     : False
            , 'resultado': str(exc)
        }


##
# Handler - VALIDAR USUARIO Y PASSWORD
##
def mob_validar_user_pass_handler(event, context):
    try:
        logger.info('mob_validar_user_pass_handler')
        if not 'cedi' in event:
            raise Exception('Falta parámetro cedi')
        if not 'password' in event:
            raise Exception('Falta parámetro password')

        cedi     = event['cedi']
        password = event['password']
        validar = Validaciones()

        status, res , response = validar.validar_user_pass_mob(cedi, password)

        if not status:
            raise Exception(res)

        return {
            'status'      : True
            , 'resultado' : res
            , 'token' : response.decode("utf-8")
        }

    except Exception as exc:
        exc_output = sys.exc_info()
        logger.error('{},{}'.format(str(exc),str(exc_output[2].tb_lineno)))
        return {
            'status'     : False
            , 'resultado': str(exc)
        }

        
##
# HANDLER - VALIDAR USUARIO Y PASSWORD
##
def mob_validar_token_handler(event, context):
    try:
        logger.info('mob_validar_token_handler')
        if not 'id_cedi' in event:
            raise Exception('Falta parametro id_cedi')
        if not 'tokenviaje' in event:
            raise Exception('Falta parametro tokenviaje')

        id_cedi    = event['id_cedi']
        tokenviaje = event['tokenviaje']

        validar = Validaciones()
        status, res = validar.validar_token_mob(id_cedi, tokenviaje)

        if not status:
            raise Exception(res)

        return {
            'status'      : True
            , 'resultado' : res
        }

    except Exception as exc:
        exc_output = sys.exc_info()
        logger.error('{},{}'.format(str(exc),str(exc_output[2].tb_lineno)))
        return {
            'status'     : False
            , 'resultado': str(exc)
        }

        
##
# HANDLER - ASIGNAR/TOMAR TOKEN/VIAJE
##
def mob_asignar_token_handler(event, context):
    try:
        if not 'id_cedi' in event:
            raise Exception('Falta parametro id_cedi')
        if not 'id_viaje' in event:
            raise Exception('Falta parametro id_viaje')
        if not 'nombre_operador' in event:
            raise Exception('Falta parametro nombre_operador')
        if not 'fletera' in event:
            event['fletera'] = False
        if not 'correo_operador' in event:
            event['correo_operador'] = False
        if not 'placas' in event:
            raise Exception('Falta parametro placas')
        if not 'numero_economico' in event:
            raise Exception('Falta parametro numero_economico')

        validar = Validaciones()
        status, res = validar.asignar_token_mob(event)

        if not status:
            raise Exception(res)

        return {
            'status'      : True
            , 'resultado' : res
        }

    except Exception as exc:
        exc_output = sys.exc_info()
        logger.error('{},{}'.format(str(exc),str(exc_output[2].tb_lineno)))
        return {
            'status'     : False
            , 'resultado': str(exc)
        }

def persistir_info_viaje_handler(event, context):
    """Para persistir la info de un viaje en firebase"""
    try:
        if not 'id_viaje' in event:
            raise Exception('Falta parametro id_viaje')
        if not 'json_viaje' in event:
            raise Exception('Falta parametro json_viaje')

        json_firebase = event['json_viaje']
        id_viaje = event['id_viaje']
        if not isinstance(json_firebase, dict):
            json_firebase = json.loads(json_firebase)
        status = True       # variables de resultados
        info = ''
        
        viaje = ViajeMobility()
        status, info = viaje.persistir_info(json_firebase, id_viaje)

        if not status:
            raise Exception(info)

        return {
            'status'      : True
            , 'resultado' : info
        }

    except Exception as exc:
        exc_output = sys.exc_info()
        logger.error('{},{}'.format(str(exc),str(exc_output[2].tb_lineno)))
        return {
            'status'     : False
            , 'resultado': str(exc)
        }

def registrar_incidente_handler(event, context):
    """Para persistir la info de un viaje en firebase"""
    try:
        if not 'id_viaje' in event:
            raise Exception('Falta parametro id_viaje')
        if not 'comentario' in event:
            raise Exception('Falta parametro comentario')
        if not 'etapa' in event:
            raise Exception('Falta parametro etapa')
        if not 'fecha' in event:
            raise Exception('Falta parametro fecha')
        if not 'foto' in event:
            raise Exception('Falta parametro foto')
        if not 'latitud' in event:
            raise Exception('Falta parametro latitud')
        if not 'longitud' in event:
            raise Exception('Falta parametro longitud')
        if not 'tipo' in event:
            raise Exception('Falta parametro tipo')
        if not 'operador' in event:
            raise Exception('Falta parametro operador')
        if not 'correo_operador' in event:
            raise Exception('Falta parametro correo_operador')
        if not 'destino' in event:
            raise Exception('Falta parametro destino')

        
        
        incidente = ViajeIncidente()
        status, info = incidente.registrar_incidente(event)

        if not status:
            raise Exception(info)

        return {
            'status'      : True
            , 'resultado' : info
        }

    except Exception as exc:
        exc_output = sys.exc_info()
        logger.error('{},{}'.format(str(exc),str(exc_output[2].tb_lineno)))
        return {
            'status'     : False
            , 'resultado': str(exc)
        }

if __name__ == '__main__':
    EVENT, CONTEXT, DEBUG = get_params()
    set_logger(DEBUG)
    # para hacer login en el mobility
    # print(json.dumps(mob_validar_user_pass_handler(EVENT, CONTEXT)))

    # # para validar el token de mobility
    # print(json.dumps(mob_validar_token_handler(EVENT, CONTEXT)))

    # para asignar el token de mobility
    # print(json.dumps(mob_asignar_token_handler(EVENT, CONTEXT)))

    # para persistir la info de mobility (liquidar viaje)
    # print(json.dumps(persistir_info_viaje_handler(EVENT, CONTEXT)))

    # para registrar un incidente
    print(json.dumps(registrar_incidente_handler(EVENT, CONTEXT)))