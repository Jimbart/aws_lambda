# -*- coding: utf-8 -*-

# Raquel viernes, 17 de mayo de 2019 (GMT-6)
# para concentrar los microservicios de viajes

from argparse import ArgumentParser
from logging import basicConfig
import logging, sys, json
from tracking import Tracking

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

def login_tracking_handler(event, context):
    try:
        logger.info('login_tracking_handler')
        if not 'correo' in event:
            raise Exception('Falta parámetro correo')
        if not 'password' in event:
            raise Exception('Falta parámetro password')

        correo     = event['correo']
        password   = event['password']

        tracking = Tracking()
        status, res = tracking.login(correo, password)

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

def obtener_viajes_tracking_handler(event, context):
    """Para obtener los viajes de mobility para tracking"""
    try:
        logger.info('obtener_viajes_tracking_handler')
        if not 'cedis' in event:
            raise Exception('Falta parámetro cedis')

        cedis = event['cedis']
        
        tracking = Tracking()
        status, res = tracking.obtener_viajes(cedis)
        
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

if __name__ == '__main__':
    """Definir el handler ejecutado desde la consola"""
    EVENT, CONTEXT, DEBUG = get_params()
    set_logger(DEBUG)

    # Para hacer login en el tracking
    # print(login_tracking_handler(EVENT, CONTEXT))
    # python aws_lambda_wrapper_tracking.py '{"correo": "emgiles@arete.ws", "password": "arms2019"}'

    # Para obtener los viajes de firebase para tracking
    print(obtener_viajes_tracking_handler(EVENT, CONTEXT))
    # python aws_lambda_wrapper_tracking.py '{"cedis": [4,16,81,91]}'
