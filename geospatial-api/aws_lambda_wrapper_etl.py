# -*- coding: utf-8 -*-

# Rach viernes, 9 de agosto de 2019
# Para construir el ETL de MySQL y Firebase a BQ

from logging import getLogger, basicConfig
from argparse import ArgumentParser


import os, sys, json
import configparser
from etl import ETL

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

def transferir_info_viajes(event, context):
    """Para transferir la info de los viajes a BQ"""
    try:
        id_empresa = int(event.get('idEmpresa', os.environ.get('idEmpresa')))
        id_cedi = int(event.get('idCedi', os.environ.get('idCedi')))
        viaje = int(event.get('viaje', 0))
        ignore_pof_completo = bool(int(event.get('ignore_pof_completo', os.environ.get('ignore_pof_completo', True))))
        ignore_pof_danios = bool(int(event.get('ignore_pof_danios', os.environ.get('ignore_pof_danios', True))))
        ignore_pof_doctos = bool(int(event.get('ignore_pof_doctos', os.environ.get('ignore_pof_doctos', True))))
        tolerancia = int(event.get('tolerancia', os.environ.get('tolerancia', 10)))

        params = {
            'empresa_id':id_empresa
            ,'cedi_id':id_cedi
            ,'viaje':viaje
            ,'ignore_pof_completo':ignore_pof_completo
            ,'ignore_pof_danios':ignore_pof_danios
            ,'ignore_pof_doctos':ignore_pof_doctos
            ,'tolerancia':tolerancia
        }

        etl = ETL(params)
        status, res = etl.transfer_viajes()

        # Lo que se registra desde CloudFunctions:
        # - Incidentes
        # - Actividades
        # - AR 
        # - Tracking

        # Lo que registran:
        # - Viajes
        # - Destinos
        # - Pedidos
        # - Encuestas
        return status, res
    except Exception as exc:
        exc_output = sys.exc_info()
        logger.error('{},{}'.format(str(exc),str(exc_output[2].tb_lineno)))
        return False, str(exc)


if __name__ == "__main__":
    EVENT, CONTEXT, DEBUG = get_params()
    set_logger(DEBUG)
    print(transferir_info_viajes(EVENT, CONTEXT))
