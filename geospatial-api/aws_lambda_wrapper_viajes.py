# -*- coding: utf-8 -*-

# Raquel viernes, 17 de mayo de 2019 (GMT-6)
# para concentrar los microservicios de viajes

from argparse import ArgumentParser
from logging import basicConfig
import logging, sys, json
from viajes import Viajes

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

def predespegar_viajes_handler(event, context):
    """Para predespegar los viajes desde ARMS y enviar la información a FB"""
    try:
        logger.info('predespegar_viajes_handler')
        if not 'id_viaje' in event:
            raise Exception('Falta parámetro id_viaje')
        if not 'id_cedi' in event:
            raise Exception('Falta parámetro id_cedi')
        if not 'id_usuario' in event:
            raise Exception('Falta parámetro id_usuario')
        id_viaje    = event['id_viaje']
        id_cedi     = event['id_cedi']
        id_usuario  = event['id_usuario']
        viaje = Viajes()

        status, res, token = viaje.predespegar(id_viaje, id_cedi, id_usuario)

        if not status:
            raise Exception(res)
        
        return {
            'status'    : True,
            'resultado' : res,
            'token'     : token
        }

    except Exception as exc:
        exc_output = sys.exc_info()
        logger.error('{},{}'.format(str(exc),str(exc_output[2].tb_lineno)))
        return {
            'status'     : False
            , 'resultado': str(exc)
        }

def return_viaje_mock_handler(event, context):
    """Para regresar un viaje mock"""
    try:
        logger.info('return_viaje_mo_handler')
        if not 'idEmpresa' in event:
            raise Exception('Falta parámetro idEmpresa')

        id_empresa = event['idEmpresa']
        
        status, res = validacion_empresa_mock(id_empresa)
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

def validacion_empresa_mock(id_empresa):
    """Valida que la empresa 16 exista"""
    if not id_empresa == 16:
        return False, "You're company is not exist"
    else:
        with open('./json_mocks/json_viaje_arms4.json') as json_test:
            data = json.load(json_test)
        return True, data

def obtener_viajes_por_status_handler(event, context):
    """Para consultar viajes por status"""
    try:
        logger.info('return_viaje_por_status_handler')
        if not 'id_cedi' in event:
            raise Exception('Falta parámetro id_cedi')
        if not 'status' in event:
            raise Exception('Falta parámetro status')

        id_cedi = event['id_cedi']
        status  = event['status']

        viajes = Viajes()
        
        status, res = viajes.obtener_por_status(id_cedi, status)
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

def desautorizar_viajes_handler(event, context):
    """Para predespegar los viajes desde ARMS y enviar la información a FB"""
    try:
        logger.info('predespegar_viajes_handler')
        if not 'id_viaje' in event:
            raise Exception('Falta parámetro id_viaje')
        if not 'id_usuario' in event:
            raise Exception('Falta parámetro id_usuario')

        id_viaje    = event['id_viaje']
        id_usuario  = event['id_usuario']

        viaje = Viajes()
        status, res = viaje.desautorizar(id_viaje, id_usuario)

        if not status:
            raise Exception(res)
        
        return {
            'status'    : True,
            'resultado' : res
        }

    except Exception as exc:
        exc_output = sys.exc_info()
        logger.error('{},{}'.format(str(exc),str(exc_output[2].tb_lineno)))
        return {
            'status'     : False
            , 'resultado': str(exc)
        }

if __name__ == '__main__':
    """Definir el handler ejecutado desde la consola"""
    EVENT, CONTEXT, DEBUG = get_params()
    set_logger(DEBUG)

    # Para predespegar un viaje
    # print(predespegar_viajes_handler(EVENT, CONTEXT))

    # Para regresar un json mockeado para tracking
    # print(return_viaje_mock_handler(EVENT, CONTEXT))

    # Para obtener viajes por status
    # print(obtener_viajes_por_status_handler(EVENT, CONTEXT))

    # Para desautorizar viajes
    print(desautorizar_viajes_handler(EVENT, CONTEXT))
