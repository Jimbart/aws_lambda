# -*- coding: utf-8 -*-

# Raquel martes, 22 de enero de 2019 (GMT-6)
#               

from argparse import ArgumentParser
from logging import getLogger, basicConfig
import os, sys
import pickle
import redis
import json
from datetime import datetime
import csv
import configparser
from arcos import Arcos
from plan import Plan
from catalogos import Catalogos
import numpy as np
from cedis import Cedis
from usuarios import Usuarios
from viajes import Viajes
from itinerario import Itinerario

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
        print(args)
        return (
            json.loads(args.json_test), None, args.debug
        )
    except Exception as exc:
        exc_output = sys.exc_info()
        logger.error('{},{}'.format(str(exc),str(exc_output[2].tb_lineno)))
        return ({
            'status'     : False
            , 'resultado': str(exc)
        }, None, args.debug)

def obtener_key_redis_handler(event, context):
    try:
        """Para obtener los json del motor por fecha dada"""
        uuid = event['uuid']
        fecha = datetime.strptime(event['fecha'], '%Y-%m-%d %H:%M:%S')  # se espera una hora en formato de 24 hrs 2019-01-24 15:18:46 
        host = config.get('redis', 'url')
        r = redis.StrictRedis(host=host, port=6379)
        
        # listar los keys del uuid dado
        keys = r.hkeys(uuid)
        resultado = {}
        #seleccionar los keys de la fecha dada a la fecha actual
        if keys == []:
            raise Exception('No existen registros del UUID dado')
        try:
            np_keys = (np.asarray(keys))
            sort_keys = (np.sort(np_keys, axis=-1, kind='quicksort', order=None))
            np_keys_to_list = sort_keys.tolist()
            key = np_keys_to_list[-1]
            key1 = key.decode('ascii')
            key_datetime = datetime.strptime(key1, '%Y-%m-%d %H:%M:%S')
            resultado[key1] = json.loads(r.hget(uuid, key))
            # if key_datetime > fecha:
                
            #for key in keys:
            #    key1 = key.decode('ascii')
            #    key_datetime = datetime.strptime(key1, '%Y-%m-%d %H:%M:%S')
            #    if key_datetime > fecha:
            #        resultado[key1] = json.loads(r.hget(uuid, key))
            
        except Exception as exc:
            logger.error(exc)
            return {
                'status': False
                ,'resultado': 'El json no es valido'
            }

        return {
            'status': True
            ,'resultado': resultado
        }
        
    except Exception as exc:
        logger.error(exc)
        return {
            'status': False
            ,'resultado': str(exc)
        }




def poblar_redis_inicial_handler(event = None, context = None):
    try:
        logger.info('poblar_redis_inicial_handler')

        # leo archivo
        with open("user_data/arcos_1.csv") as csvfile:
            reader = csv.DictReader(csvfile)
            arcos_ordered_dict = list(reader)

        # obtengo arreglo de diccionarios
        arcos = []
        for arco in arcos_ordered_dict:
            arcos.append(json.loads(json.dumps(arco)))
        
        host = config.get('redis', 'url')
        r = redis.StrictRedis(host=host, port=6379)
        # inyecto arcos, utilizo pipelines
        pipe = r.pipeline()

        for arco in arcos:
            key = '{idCedi}_{IDNodoOrigen}_{IDNodoDestino}'.format(
                idCedi=16,IDNodoOrigen=arco['IDNodoOrigen'],IDNodoDestino=arco['IDNodoDestino'])
            value = json.dumps({
                'Distancia':arco['Distancia']
                ,'TiempoTrayecto':arco['TiempoTrayecto']
                ,'FechaActualizacion':arco['FechaActualizacion']})
            pipe.set(key, value)

        print(pipe.execute())

        return {
            'status': True
            ,'resultado': ''
        }
        
    except Exception as exc:
        logger.error(exc)
        return {
            'status': False
            ,'resultado': str(exc)
        }


def autorizar_viajes_handler(event, context):
    try:
        """Para autorizar los viajes enviados"""
        if not 'IdCedi' in event:
            raise Exception('Falta parametro IdCedi')
        if not 'IdAutorizador' in event:
            raise Exception('Falta parametro IdAutorizador')
        if not 'IdPlaneador' in event:
            raise Exception('Falta parametro IdPlaneador')
        if not 'IdSesion' in event:
            raise Exception('Falta parametro IdSesion')
        if not 'Proyecto' in event:
            raise Exception('Falta parametro Proyecto')
        if not 'Viajes' in event:
            raise Exception('Falta parametro Viajes')

        id_cedi = event['IdCedi']
        id_autorizador = event['IdAutorizador']
        id_planeador = event['IdPlaneador']
        id_sesion = event['IdSesion']
        proyecto = event['Proyecto']
        viajes = event['Viajes']

        class_viajes = Viajes()

        status, res, id_viajes, mensaje = class_viajes.autorizar(id_cedi, id_autorizador, id_planeador, id_sesion, proyecto, viajes)
        
        return {
            'status': status
            ,'FechaAutorizacion': res
            ,'IdViajes': id_viajes
            ,'resultado': mensaje
        }
        
    except Exception as exc:
        logger.error(exc)
        return {
            'status': False
            ,'FechaAutorizacion': None
            ,'IdViajes': None
            ,'resultado': str(exc)
        }


def validar_arcos_handler(event, context):
    try:
        """Para validar los viajes enviados"""
        if not 'id_cedi' in event:
            raise Exception('Falta parametro id_cedi')
        if not 'combinaciones' in event:
            raise Exception('Falta parametro combinaciones')
        id_cedi = event['id_cedi']
        combinaciones = event['combinaciones']

        arcos = Arcos()

        status, res = arcos.validar_arcos(id_cedi, combinaciones)

        if not status:
            raise Exception(res)

        if res != []:
            return {
                'status': True
                ,'resultado': 'No existen todos los arcos'
                ,'arcos_faltantes':res
            }

        return {
            'status': True
            ,'resultado': 'Existen todos los arcos'
        }
        
    except Exception as exc:
        logger.error(exc)
        return {
            'status': False
            ,'resultado': str(exc)
        }


def login_arms_handler(event, context):
    try:
        """Para autorizar los viajes enviados"""
        if not 'cedis' in event:
            raise Exception('Falta parametro cedis')
        if not 'correo_usuario' in event:
            raise Exception('Falta parametro correo_usuario')
        cedis = event['cedis']
        correo_usuario = event['correo_usuario']

        catalogos = Catalogos()

        status, res = catalogos.validar_usuario(cedis, correo_usuario)

        if not status:
            raise Exception(res)


        return {
            'status': True
            ,'resultado': res
        }
        
    except Exception as exc:
        logger.error(exc)
        return {
            'status': False
            ,'resultado': str(exc)
        }
          

def validar_destinos_handler(event, context):
    try:
        destinos = (event['destinos'])
        id_cedi = event['id_cedi']

        catalogos = Catalogos()
        status, json_destinos, destinos_faltantes = catalogos.validar_destinos(id_cedi, destinos)

        if not status:
            raise Exception(json_destinos)

        return {
            'status': True
            ,'destinos': json_destinos
            ,'destinos_faltantes': destinos_faltantes
        }
        
    except Exception as exc:
        logger.error(exc)
        return {
            'status': False
            ,'resultado': str(exc)
        }

def listar_planes_handler(event, context):
    try:
        if not 'id_cedi' in event:
            raise Exception('Falta parametro id_cedi')
        id_cedi = event['id_cedi']

        plan = Plan()
        status, res = plan.listar_planes(id_cedi)

        if not status:
            raise Exception(res)

        return {
            'status': True
            ,'resultado': res
        }
        
    except Exception as exc:
        logger.error(exc)
        return {
            'status': False
            ,'resultado': str(exc)
        }  

def listar_cedis_handler(event, context):
    try:
        if not 'correo' in event:
            raise Exception('Falta parametro correo')
        correo = event['correo']

        cedis = Cedis()
        status, res = cedis.listar_cedis(correo)

        if not status:
            raise Exception(res)

        return {
            'status': True
            ,'resultado': res
        }
        
    except Exception as exc:
        logger.error(exc)
        return {
            'status': False
            ,'resultado': str(exc)
        }   

def obtener_preferencias_handler(event, context):
    try:
        if not 'id_cedis' in event:
            raise Exception('Falta parametro id_cedis')
        id_cedis = event['id_cedis']

        if not 'perfil' in event:
            raise Exception('Falta parametro perfil')
        perfil = event['perfil']

        cedis = Cedis()
        status, res = cedis.obtener_preferencias_motor(id_cedis, perfil)

        if not status:
            raise Exception(res)

        return {
            'status': True
            ,'resultado': res
        }
        
    except Exception as exc:
        logger.error(exc)
        return {
            'status': False
            ,'resultado': str(exc)
        } 

def obtener_preferencias_usuario_handler(event, context):
    try:
        if not 'id_cedis' in event:
            raise Exception('Falta parametro id_cedis')
        id_cedis = event['id_cedis']

        if not 'correo' in event:
            raise Exception('Falta parametro correo')
        correo = event['correo']

        cedis = Cedis()
        status, res = cedis.obtener_preferencias_usuario(id_cedis, correo)

        if not status:
            raise Exception(res)

        return {
            'status': True
            ,'resultado': res
        }
        
    except Exception as exc:
        logger.error(exc)
        return {
            'status': False
            ,'resultado': str(exc)
        }  

def obtener_vehiculos_cedi_handler(event, context):
    try:
        if not 'id_cedis' in event:
            raise Exception('Falta parametro id_cedis')
        id_cedis = event['id_cedis']

       

        cedis = Cedis()
        status, res = cedis.obtener_vehiculos(id_cedis)

        if not status:
            raise Exception(res)

        return {
            'status': True
            ,'resultado': res
        }
        
    except Exception as exc:
        logger.error(exc)
        return {
            'status': False
            ,'resultado': str(exc)
        } 

def registrar_vehiculos_cedi_handler(event, context):
    try:
        if not 'id_cedi' in event:
            raise Exception('Falta parametro id_cedi')
        id_cedi = event['id_cedi']

        if not 'vehiculos' in event:
            raise Exception('Falta parametro vehiculos')
        vehiculos = event['vehiculos']

       

        cedis = Cedis()
        status, res = cedis.registrar_vehiculos(id_cedi, vehiculos)

        if not status:
            raise Exception(res)

        return {
            'status': True
            ,'resultado': res
        }
        
    except Exception as exc:
        logger.error(exc)
        return {
            'status': False
            ,'resultado': str(exc)
        }

def registrar_destinos_cedi_handler(event, context):
    try:
        if not 'id_cedi' in event:
            raise Exception('Falta parametro id_cedi')
        id_cedi = event['id_cedi']

        if not 'destinos' in event:
            raise Exception('Falta parametro destinos')
        destinos = event['destinos']

       

        cedis = Cedis()
        status, res = cedis.registrar_destinos(id_cedi, destinos)

        if not status:
            raise Exception(res)

        return {
            'status': True
            ,'resultado': res
        }
        
    except Exception as exc:
        logger.error(exc)
        return {
            'status': False
            ,'resultado': str(exc)
        }   

def obtener_destinos_cedi_handler(event, context):
    try:
        if not 'id_cedi' in event:
            raise Exception('Falta parametro id_cedi')
        id_cedi = event['id_cedi']

       

        cedis = Cedis()
        status, res = cedis.obtener_destinos(id_cedi)

        if not status:
            raise Exception(res)

        return {
            'status': True
            ,'resultado': res
        }
        
    except Exception as exc:
        logger.error(exc)
        return {
            'status': False
            ,'resultado': str(exc)
        }  

def guardar_preferencias_usuario_handler(event, context):
    try:
        if not 'id_usuario' in event:
            raise Exception('Falta parametro id_usuario')
        id_usuario = event['id_usuario']

        if not 'preferencias' in event:
            raise Exception('Falta parametro preferencias')
        preferencias = event['preferencias']

        usuarios = Usuarios()
        status, res = usuarios.guardar_preferencias_usuario(id_usuario, preferencias)

        if not status:
            raise Exception(res)

        return {
            'status': True
            ,'resultado': res
        }
        
    except Exception as exc:
        logger.error(exc)
        return {
            'status': False
            ,'resultado': str(exc)
        }

def guardar_preferencias_cedi_handler(event, context):
    try:
        if not 'id_cedi' in event:
            raise Exception('Falta parametro id_cedi')
        if not 'preferencias' in event:
            raise Exception('Falta parametro preferencias')

        id_cedi         = event['id_cedi']
        perfil          = event['perfil']
        preferencias    = event['preferencias']

        cedis = Cedis()
        status, res = cedis.guardar_preferencias_cedi(id_cedi, perfil, preferencias)

        if not status:
            raise Exception(res)

        return {
            'status': True
            ,'resultado': res
        }
        
    except Exception as exc:
        logger.error(exc)
        return {
            'status': False
            ,'resultado': str(exc)
        }

def obtener_preferencias_default_handler(event, context):
    try:
        if not 'id_cedis' in event:
            raise Exception('Falta parametro id_cedis')
        id_cedis = event['id_cedis']

        if not 'perfil' in event:
            raise Exception('Falta parametro perfil')
        perfil = event['perfil']

        cedis = Cedis()
        status, res = cedis.obtener_preferencias_default_motor(id_cedis, perfil)

        if not status:
            raise Exception(res)

        return {
            'status': True
            ,'resultado': res
        }
        
    except Exception as exc:
        logger.error(exc)
        return {
            'status': False
            ,'resultado': str(exc)
        }

def recalcular_itinerario_handler(event, context):
    try:
        if not 'datos' in event:
            raise Exception('Falta parametro datos')
        datos = event['datos']

        itinerario = Itinerario()
        status, res = itinerario.recalcular(datos)

        if not status:
            raise Exception(res)

        return {
            'status': True
            ,'resultado': res
        }
        
    except Exception as exc:
        logger.error(exc)
        return {
            'status': False
            ,'resultado': str(exc)
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

        status, res = validar_empresa(correo, password)
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

def validar_empresa(correo, password):
    logger.info('validando_correo_password')
    res = {
        'user': "Norman Sanchez",
        'company': "Areté Software",
        'id': 16
    }

    if not password == "arms2019":
        return False, "You're company is not exist"
    else:
        return True, res

def extraccion_datos_handler(event, context):
    logger.info('inicio extraccion de informacion de pedidos')
    if not 'idCedi' in event:
        raise Exception('Falta parámetro idCedi')
    if not 'Plan' in event:
        raise Exception('Falta parámetro Plan')

    _idCedi = event['idCedi']
    _sNombrePlan= event['Plan']

    import Utilidades.Extraccion as drmt
    extractor=drmt.Extraccion()
    return extractor.obtener_plan(_idCedi,_sNombrePlan)

def actualizar_ss_preferido_usuario_handler(event, context):
    try:
        if not 'id_cedi' in event:
            raise Exception('Falta parametro id_cedi')
        if not 'id_usuario' in event:
            raise Exception('Falta parametro id_usuario')
        if not 'spreads' in event:
            raise Exception('Falta parametro spreads')

        id_cedi = event['id_cedi']
        id_usuario = event['id_usuario']
        spreads = event['spreads']

        usuarios = Usuarios()
        status, res = usuarios.actualizar_ss_preferido_usuario(id_cedi, id_usuario, spreads)

        if not status:
            raise Exception(res)

        return {
            'status': True
            ,'resultado': res
        }
        
    except Exception as exc:
        logger.error(exc)
        return {
            'status': False
            ,'resultado': str(exc)
        }


if __name__ == '__main__':
    EVENT, CONTEXT, DEBUG = get_params()
    set_logger(DEBUG)
    # print(json.dumps(extraccion_datos_handler(EVENT, CONTEXT)))
    # para obtener el json por un key dado
    # print(json.dumps(obtener_key_redis_handler(EVENT, CONTEXT)))

    # set_logger(0)
    # print(json.dumps(poblar_redis_inicial_handler()))

    # Para autorizar los viajes enviado
    print(json.dumps(autorizar_viajes_handler(EVENT, CONTEXT)))

    # Para validar los arcos enviados
    # print(json.dumps(validar_arcos_handler(EVENT, CONTEXT)))

    # Para validar el correo del usuario
    # print(json.dumps(login_arms_handler(EVENT, CONTEXT)))
    
    # para validar destinos
    # print(json.dumps(validar_destinos_handler(EVENT, CONTEXT)))

    # para listar los planes de un cedi
    # print(json.dumps(listar_planes_handler(EVENT, CONTEXT)))

    # para listar los cedis asociados a un usuario
    # print(json.dumps(listar_cedis_handler(EVENT, CONTEXT)))

    # para obtener las preferencias del motor
    # print(json.dumps(obtener_preferencias_handler(EVENT, CONTEXT)))

    # para obtener las preferencias del usuario
    # print(json.dumps(obtener_preferencias_usuario_handler(EVENT, CONTEXT)))

    # para obtener los vehiculos de un cedi
    # print(json.dumps(obtener_vehiculos_cedi_handler(EVENT, CONTEXT)))

    # para registrar los vehiculos de un cedi
    # print(json.dumps(registrar_vehiculos_cedi_handler(EVENT, CONTEXT)))

    # para registrar los destinos de un cedi
    # print(json.dumps(registrar_destinos_cedi_handler(EVENT, CONTEXT)))

    # para obtener los destinos de un cedi
    # print(json.dumps(obtener_destinos_cedi_handler(EVENT, CONTEXT)))

    # para guardar las preferencias de un usuario
    # print(json.dumps(guardar_preferencias_usuario_handler(EVENT, CONTEXT)))

    # para obtener las preferencias defalt por cedi
    # print(json.dumps(obtener_preferencias_default_handler(EVENT, CONTEXT)))

    # para recalcular el itinerario
    # print(json.dumps(recalcular_itinerario_handler(EVENT, CONTEXT)))

    # para hacer login con el tracking
    # print(json.dumps(login_tracking_handler(EVENT, CONTEXT)))

    # para guardar las preferencias de un cedi
    # print(json.dumps(guardar_preferencias_cedi_handler(EVENT, CONTEXT)))

    # para guardar los SS preferidos del usuario 
    # print(json.dumps(actualizar_ss_preferido_usuario_handler(EVENT, CONTEXT)))