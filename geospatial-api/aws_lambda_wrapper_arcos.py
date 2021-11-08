# -*- coding: utf-8 -*-

# Eduardo jueves, 14 de marzo de 2019 (GMT-6)
# servicios para arcos, verificación y obtención de parejas faltantes

from argparse import ArgumentParser
from logging import getLogger, basicConfig
import json
import configparser
from arcos import Arcos
import googlemaps
from googlemaps.geocoding import *
import sys

logger = getLogger()

config = configparser.ConfigParser()
config.read('config.ini')

def set_logger(debug=False):
    """Sets the logger configuration"""
    try:
        logformat = "[ %(levelname)s ] - %(asctime)-15s :: %(message)s"
        basicConfig(format=logformat)
        if debug:
            logger.setLevel(10)

    except Exception as exc:
        logger.error(exc)

def validar_arcos_existentes_get_params():
    """Obtiene los parametros de la linea de comandos"""
    try:
        parser = ArgumentParser(description='ejecucion manual')
        parser.add_argument('id_cedi', type=int, metavar='<id_cedi>', default='', help='')
        parser.add_argument('destinos', type=str, metavar='<destinos>', default='', help='')
        parser.add_argument('id_usuario', type=int, metavar='<id_usuario>', default='', help='')
        parser.add_argument('id_empresa', type=int, metavar='<id_empresa>', default='', help='')
        parser.add_argument('--debug', action="count", default=0, help='')
        args = parser.parse_args()

        return (
            {
                'id_cedi': args.id_cedi,
                'destinos': args.destinos,
                'id_usuario': args.id_usuario,
                'id_empresa': args.id_empresa
            }, None, args.debug)

    except Exception as exc:
        logger.error(exc)

def validar_arcos_existentes_handler(event, context):
    try:
        if not 'id_cedi' in event:
            raise Exception('Falta parametro id_cedi')
        if not 'destinos' in event:
            raise Exception('Falta parametro destinos')
        if not 'id_usuario' in event:
            raise Exception('Falta parametro id_usuario')
        if not 'id_empresa' in event:
            raise Exception('Falta parametro id_empresa')

        id_cedi     = event['id_cedi']
        id_usuario  = event['id_usuario']
        id_empresa  = event['id_empresa']

        try:
            destinos = (event['destinos'])
        except:
            raise Exception('Error con el formato de destinos')

        arcos = Arcos()
        status, numFaltantes, destinosFaltantes, creditos_suficientes = arcos.validar(id_cedi, destinos, id_usuario, id_empresa)

        return {
            'status': status,
            'Tiempo': numFaltantes,
            'creditosSuficientes': creditos_suficientes,
            'destinosFaltantes': destinosFaltantes,
            'mensaje': ""
        }
        
    except Exception as exc:
        logger.error(exc)
        return {
            'status': False
            ,'Tiempo': 0
            ,'creditosSuficientes': False
            ,'destinosFaltantes': []
            ,'mensaje': str(exc)
        }

def calcular_arcos_get_params():
    """Obtiene los parametros de la linea de comandos"""
    try:
        parser = ArgumentParser(description='ejecucion manual')
        parser.add_argument('id_cedi', type=int, metavar='<id_cedi>', default='', help='')
        parser.add_argument('destinos', type=str, metavar='<destinos>', default='', help='')
        parser.add_argument('UUID', type=str, metavar='<UUID>', default='', help='')
        parser.add_argument('id_usuario', type=str, metavar='<id_usuario>', default='', help='')
        parser.add_argument('id_empresa', type=str, metavar='<id_empresa>', default='', help='')
        # parser.add_argument('test', type=str, metavar='<test>', default='', help='')
        parser.add_argument('--debug', action="count", default=0, help='')
        args = parser.parse_args()

        return (
            {
                'id_cedi': args.id_cedi,
                'destinos': args.destinos,
                'UUID': args.UUID,
                'id_usuario': args.id_usuario,
                'id_empresa': args.id_empresa
                # 'test': args.test
            }, None, args.debug)

    except Exception as exc:
        logger.error(exc)

def calcular_arcos_handler(event, context):
    try:
        if not 'id_cedi' in event:
            raise Exception('Falta parametro id_cedi')
        if not 'destinos' in event:
            raise Exception('Falta parametro destinos')
        if not 'UUID' in event:
            raise Exception('Falta parametro UUID')
        if not 'id_usuario' in event:
            raise Exception('Falta parametro id_usuario')
        if not 'id_empresa' in event:
            raise Exception('Falta parametro id_empresa')
        # if not 'test' in event:
        #     raise Exception('Falta parametro test')

        id_cedi = event['id_cedi']

        try:
            destinos = (event['destinos'])
        except:
            raise Exception('Error con el formato de destinos')
        
        UUID = (event['UUID'])
        id_usuario = (event['id_usuario'])
        id_empresa = (event['id_empresa'])
        # test = (event['test'])

        arcos = Arcos()
        status = arcos.calcular_arcos(id_cedi, destinos, UUID, id_usuario, id_empresa)

        return {
            'status': status
        }
        
    except Exception as exc:
        logger.error(exc)
        return {
            'status': False
            ,'resultado': str(exc)
        }

def obtener_errores_get_params():
    """Obtiene los parametros de la linea de comandos"""
    try:
        parser = ArgumentParser(description='ejecucion manual')
        parser.add_argument('UUID', type=str, metavar='<UUID>', default='', help='')
        parser.add_argument('--debug', action="count", default=0, help='')
        args = parser.parse_args()

        return (
            {
                'UUID': args.UUID
            }, None, args.debug)

    except Exception as exc:
        logger.error(exc)

def obtener_errores_handler(event, context):
    try:
        if not 'UUID' in event:
            raise Exception('Falta parametro UUID')
        
        UUID = (event['UUID'])

        arcos = Arcos()
        status, mensaje = arcos.obtener_errores(UUID)

        return {
            'status': status,
            'resultado': mensaje
        }
        
    except Exception as exc:
        logger.error(exc)
        return {
            'status': False
            ,'resultado': str(exc)
        }

def obtener_coordenadas_get_params():
    """Obtiene los parametros de la linea de comandos"""
    try:
        parser = ArgumentParser(description='ejecucion manual')
        parser.add_argument('direcciones', type=str, metavar='<direcciones>', default='', help='')
        parser.add_argument('--debug', action="count", default=0, help='')
        args = parser.parse_args()

        return (
            {
                'direcciones': args.direcciones
            }, None, args.debug)

    except Exception as exc:
        logger.error(exc)

def obtener_coordenadas_handler(event, context):
    try:
        if not 'direcciones' in event:
            raise Exception('Falta parametro direcciones')
        
        direcciones = (event['direcciones'])

        arcos = Arcos()
        status, secret = arcos.get_secret()
        gmaps = googlemaps.Client(secret['api-key'])
        dir = {}
        logger.info('recorro las direcciones')
        for direccion in direcciones:
            logger.info('dir: ', direccion)
            res = geocode(gmaps,direccion)
            dir[direccion] = {}
            if res == []:
                logger.warning('No se encontraron coordenadas')
                dir[direccion]['lat'] = 0
                dir[direccion]['lng'] = 0
            else:
                json_res = res[0]
                dir[direccion]['lat'] = json_res['geometry']['location']['lat']
                dir[direccion]['lng'] = json_res['geometry']['location']['lng']

        return {
            'status': status,
            'resultado': dir
        }
        
    except Exception as exc:
        logger.error(exc)
        exc_response = sys.exc_info()
        logger.error('line: {}, {}'.format(exc_response[2].tb_lineno, str(exc)))
        return {
            'status': False
            ,'resultado': str(exc)
        }

def obtener_arcos_faltantes_get_params():
    """Obtiene los parametros de la linea de comandos"""
    try:
        parser = ArgumentParser(description='ejecucion manual')
        parser.add_argument('id_cedi', type=int, metavar='<id_cedi>', default='', help='')
        parser.add_argument('destinos', type=str, metavar='<destinos>', default='', help='')
        parser.add_argument('id_usuario', type=int, metavar='<id_usuario>', default='', help='')
        parser.add_argument('id_empresa', type=int, metavar='<id_empresa>', default='', help='')
        parser.add_argument('--debug', action="count", default=0, help='')
        args = parser.parse_args()

        return (
            {
                'id_cedi': args.id_cedi,
                'destinos': args.destinos,
                'id_usuario': args.id_usuario,
                'id_empresa': args.id_empresa
            }, None, args.debug)

    except Exception as exc:
        logger.error(exc)

def obtener_arcos_faltantes_handler(event, context):
    try:
        if not 'id_cedi' in event:
            raise Exception('Falta parametro id_cedi')
        if not 'destinos' in event:
            raise Exception('Falta parametro destinos')
        if not 'id_usuario' in event:
            raise Exception('Falta parametro id_usuario')
        if not 'id_empresa' in event:
            raise Exception('Falta parametro id_empresa')

        id_cedi     = event['id_cedi']
        id_usuario  = event['id_usuario']
        id_empresa  = event['id_empresa']

        try:
            destinos = (event['destinos'])
        except:
            raise Exception('Error con el formato de destinos')

        arcos = Arcos()
        status, arcos_res = arcos.obtener(id_cedi, destinos, id_usuario, id_empresa)

        return {
            'status': status,
            'arcos': arcos_res
        }
        
    except Exception as exc:
        logger.error(exc)
        return {
            'status': False
            ,'arcos': str(exc)
        }

if __name__ == '__main__':
    # VALIDAR QUE EL ARCO EXISTA
    # EVENT, CONTEXT, DEBUG = validar_arcos_existentes_get_params()
    # set_logger(DEBUG)
    # print(json.dumps(validar_arcos_existentes_handler(EVENT, CONTEXT)))
    # python aws_lambda_wrapper_arcos.py 16 '[4122, 4121, 4129]' 12 1
    # python aws_lambda_wrapper_arcos.py 16 '[5484, 3861, 5463]' 12 1
    # -------------------------------------------------------------------------------------------------
    # MANDAR A CALCULAR LAS DISTANCIAS FALTANTES
    EVENT, CONTEXT, DEBUG = calcular_arcos_get_params()
    set_logger(DEBUG)
    print(json.dumps(calcular_arcos_handler(EVENT, CONTEXT)))
    # python aws_lambda_wrapper_arcos.py 16 '[3859, 3850, 3851]' 1548 12 1
    # -------------------------------------------------------------------------------------------------
    # OBTENER ERRORES DE LA COLA DE ERRORES
    # EVENT, CONTEXT, DEBUG = obtener_errores_get_params()
    # set_logger(DEBUG)
    # print(json.dumps(obtener_errores_handler(EVENT, CONTEXT)))
    # python aws_lambda_wrapper_arcos.py 121
    # -------------------------------------------------------------------------------------------------
    # OBTENER ARCOS FALTANTES
    # EVENT, CONTEXT, DEBUG = obtener_arcos_faltantes_get_params()
    # set_logger(DEBUG)
    # print(json.dumps(obtener_arcos_faltantes_handler(EVENT, CONTEXT)))
    # python aws_lambda_wrapper_arcos.py 16 '[5484, 3861, 5463]' 12 1

    # Para obtener la lat y long de una lista de direcciones
    # EVENT, CONTEXT, DEBUG = obtener_coordenadas_get_params()
    # set_logger(DEBUG)
    # print(json.dumps(obtener_coordenadas_handler(EVENT, CONTEXT)))
