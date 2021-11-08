#-*- coding: utf-8 -*-

from logging import getLogger, basicConfig
from database_services.mysql_service import MySQLConnection
from database_services.firebasemethods import FirebaseMethods

import json, sys
import configparser
import random
import string
import pandas as pd
import numpy as np
import uuid

logger = getLogger()

class Viajes:
    def __init__(self, *args, **kwargs):
        self.my_sql_conn = MySQLConnection('config.ini')

    def autorizar(self, id_cedi, id_autorizador, id_planeador, id_sesion, proyecto, viajes):
        """autorizar viajes"""
        try:
            logger.info('autorizar')
            status = True
            errores = ""

            for viaje in viajes:
                args = [id_cedi, id_autorizador, id_planeador, id_sesion, proyecto, json.dumps(viaje), 0]
                print ("------>>>")
                print (args)
                print ("------>>>")
                res  = self.my_sql_conn.spexec('sp_api_arms_autorizar_viajes2', args)

                if res[0][0] != 'OK':
                    status = False
                    errores = errores + str(res[0][1])
                
            if not status:
                raise Exception(errores)
            
            
            return True, errores, json.loads(res[0][2]), ""
            
        except Exception as exc:
            logger.info('Ocurrio un error al registrar los vehiculos')
            logger.error(exc)
            return False, "",  None, str(exc)
    
    def predespegar(self, id_viaje, id_cedi, id_usuario):
        """predespegar viaje"""
        try:
            logger.info('predespegar')
            
            # TODO: generar token... Toño me va a dar un algo
            status, token = self.generar_token()
            if not status:
                raise Exception(token)
            # TODO: reservar viaje en mysql
            status, res = self.predespegar_viaje_mysql(id_viaje, token, id_usuario)
            if not status:
                raise Exception(res)
            # TODO: enviar datos a firebase
            status, res = self.enviar_viaje_firebase(id_viaje, id_cedi)
            if not status:
                raise Exception(res)
            # TODO: enviar datos a bq
            
            return True, '', token
        except Exception as exc:
            exc_output = sys.exc_info()
            logger.error('{},{}'.format(str(exc),str(exc_output[2].tb_lineno)))
            return False, str(exc), ''

    def generar_token(self):
        """Generar token de un viaje"""
        try:
            logger.info('generar_token')
            token = ''.join(random.choices(string.digits, k=6))
            return True, token
        except Exception as exc:
            exc_output = sys.exc_info()
            logger.error('{},{}'.format(str(exc),str(exc_output[2].tb_lineno)))
            return False, str(exc)
    
    def predespegar_viaje_mysql(self, id_viaje, token, id_usuario):
        """Actualizar status y escribir token de un viaje en mysql"""
        try:
            logger.info('reservar_viaje_mysql')
            args = [id_viaje, token, id_usuario]
            res  = self.my_sql_conn.spexec('sp_api_arms_predespegar_viaje', args)
            
            if res[0][0] != 'OK':
                raise Exception(res[0][1])
            
            return True, res[0][1]
        except Exception as exc:
            exc_output = sys.exc_info()
            logger.error('{},{}'.format(str(exc),str(exc_output[2].tb_lineno)))
            return False, str(exc)

    def enviar_viaje_firebase(self, id_viaje, id_cedi):
        """Se envian los datos de un viaje a firebase"""
        try:
            logger.info('enviar_viaje_firebase')
            # TODO: obtener los datos de un viaje de mysql
            status, res = self.obtener_viaje_firebase(id_viaje, id_cedi)
            if not status:
                raise Exception(res)
            # TODO: estructurar y escribir datos en firebase
            fbm = FirebaseMethods('config.ini')
            status, res = fbm.crear_viaje(id_cedi, id_viaje, res)
            if not status:
                raise Exception(res)
            return True, ''
        except Exception as exc:
            exc_output = sys.exc_info()
            logger.error('{},{}'.format(str(exc),str(exc_output[2].tb_lineno)))
            return False, str(exc)
    
    def obtener_viaje_firebase(self, id_viaje, id_cedi):
        """Obtener los datos de un viaje de Mysql"""
        try:
            logger.info('obtener_viaje')
            args = [id_cedi, id_viaje]
            res  = self.my_sql_conn.spexec('sp_api_predespegar_viaje_firebase', args)
            
            if res[0][0] != 'OK':
                raise Exception(res[0][1])
            
            return True, json.loads(res[0][1])
        except Exception as exc:
            exc_output = sys.exc_info()
            logger.error('{},{}'.format(str(exc),str(exc_output[2].tb_lineno)))
            return False, str(exc)
    
    def obtener_por_status(self, id_cedi, status):
        """Obtener json de viajes por status"""
        try:
            logger.info('obtener_por_status')
            status, df_result = self.obtener_viajes_insumos_por_status(id_cedi, status)
            if not status:
                raise Exception(df_result)
            data = self.consolidar_inputs(df_result)

            
            return True, data
        except Exception as exc:
            exc_output = sys.exc_info()
            logger.error('{},{}'.format(str(exc),str(exc_output[2].tb_lineno)))
            return False, str(exc)

    def obtener_viajes_insumos_por_status(self, id_cedi, status):
        """Obtener insumos de viajes por status"""
        try:
            logger.info('obtener_viajes_insumos_por_status')
            args = [id_cedi, status]

            result = self.my_sql_conn.spexec_to_dfs('sp_api_obtener_viajes_insumos_por_status', args)
            if result[0].loc[0, 'result'] != 'OK':
                raise Exception(result[0].loc[0, 'data'])
            
            return True, result

        except Exception as exc:
            exc_output = sys.exc_info()
            logger.error('{},{}'.format(str(exc),str(exc_output[2].tb_lineno)))
            return False, str(exc)

    def consolidar_inputs(self, df_result):
        """Para consolidar los df que regreso la consulta"""
        try:
            logger.info('consolidando los inputs df')
            paradas = self.consolidar_frame_to_json(df_result[3], df_result[4], 'pedidos', ['IdViaje', 'idParada'], remove_cols=['idParada'])
            viajes  = self.consolidar_frame_to_json(df_result[1], paradas, 'destinos', ['IdViaje'], remove_cols=['idParada', 'IdViaje'])
            viajes  = self.consolidar_frame_to_json(viajes, df_result[2], 'itinerario', ['IdViaje'], remove_cols=['IdViaje'])
            viajes  = self.consolidar_frame_to_json(viajes, df_result[5], 'vehiculo', ['IdViaje', 'idVehiculo'], remove_cols=['idVehiculo'])
            result = viajes.to_json(orient='records')
            
            return json.loads(result)
        except Exception as exc:
            exc_output = sys.exc_info()
            logger.error('{},{}'.format(str(exc),str(exc_output[2].tb_lineno)))
            return False, str(exc)

    def consolidar_frame_to_json(self, left, right, right_title, merge_cols, single=False, remove_cols=[], join_type='inner'):
        logger.info('consolidar_frame_to_json')
        try:
            rigth_cols = right.columns.values.tolist()
            
            if right.shape[0] > 0:	# hay records
                df_right = pd.DataFrame(right)	# copia
                rigth_cols = [n for n in rigth_cols if n not in remove_cols]		# elimino columnas no deseadas
                df_right = df_right.groupby(merge_cols) \
                    .apply(lambda x: x[rigth_cols].to_dict('records')) \
                        .reset_index() \
                            .rename(columns={0:right_title})
                if single:	# no es lista sino dict
                    df_right[right_title] = df_right[right_title].apply(lambda x: x[0])
                df_right.reset_index(level=0, inplace=True)	# convierto en columnas los indices de agrupacion
                df_right.drop(['index'], axis=1, inplace=True)	# elimino la columna "index"
                df_consolidado = pd.merge(left, df_right, how=join_type, on=merge_cols)	# mergeo con left
            else:	# no hay records a integrar
                df_consolidado = pd.DataFrame(left)
                df_consolidado['right_title'] = None
            
            return df_consolidado
        
        except Exception as exc:
            exc_output = sys.exc_info()
            logger.error('{},{}'.format(str(exc),str(exc_output[2].tb_lineno)))
            raise Exception(exc)
    
    def desautorizar(self, id_viaje, id_usuario):
        """Actualizar status y escribir token de un viaje en mysql"""
        try:
            logger.info('reservar_viaje_mysql')
            args = [id_viaje, id_usuario]
            res  = self.my_sql_conn.spexec('sp_api_arms_desautorizar_viaje', args)
            
            if res[0][0] != 'OK':
                raise Exception(res[0][1])
            
            return True, res[0][1]
        except Exception as exc:
            exc_output = sys.exc_info()
            logger.error('{},{}'.format(str(exc),str(exc_output[2].tb_lineno)))
            return False, str(exc)

class ViajeMobility:
    """Clase de un viaje en mobility"""
    def persistir_info(self, json_mobility, id_viaje):
        """Metodo para persistir la info de mobility a firebase"""
        try:
            
            fbm = FirebaseMethods('config.ini')     # para interactuar con firebase
            status, info = self.persistir_json_mysql(id_viaje, json_mobility)

            if not status:
                raise Exception(info)
            
            status, info = self.obtener_id_cedi(id_viaje)

            if not status:
                raise Exception(info)

            id_cedi = info['idCedi']
            paradas = info['paradas']
            json_transacciones = {}
            json_incidentes = {}

            # para completar nodo transacciones
            status, json_transacciones = fbm.leer_transacciones(id_cedi, id_viaje)
            if not status:
                raise Exception(json_transacciones)

            status, info = self.completar_viaje(info, id_viaje, json_transacciones, json_mobility)

            if not status:
                raise Exception(info)

            status, info = self.completar_destinos(info, id_viaje, json_transacciones, json_mobility, paradas)

            if not status:
                raise Exception(info)

            status, info = self.completar_actividades(info, id_viaje, json_transacciones, json_mobility)

            if not status:
                raise Exception(info)

            status, info = fbm.escribir_transacciones(id_cedi, id_viaje, json_transacciones)

            if not status:
                raise Exception(info)

            status, json_incidentes = fbm.leer_incidentes(id_cedi, id_viaje)  

            if not status:
                raise Exception(json_incidentes)

            status, info = self.completar_incidentes(info, id_viaje, json_incidentes, json_mobility)

            if not status:
                raise Exception(info)

            status, info = fbm.escribir_incidentes(id_cedi, id_viaje, json_incidentes)

            if not status:
                raise Exception(info)            

            return True, ''
            
        except Exception as exc:
            exc_output = sys.exc_info()
            logger.error('Error {}, en la linea {}'.format(str(exc), str(exc_output[2].tb_lineno)))
            return False, exc
    
    def persistir_json_mysql(self, id_viaje, json_firebase):
        try:
            logger.info('persistir_json_mysql')

            my_sql_conn = MySQLConnection('config.ini')
            result = my_sql_conn.spexec('sp_api_mob_persistir_json', [
                id_viaje
                ,json.dumps(json_firebase)
            ])

            if result[0][0] != 'OK':
                raise Exception(result[0][1])

            return True, ''

        except Exception as exc:
            logger.error(exc)
            return False, str(exc) 

    def obtener_id_cedi(self, id_viaje):
        try:
            logger.info('obtener_id_cedi')

            my_sql_conn = MySQLConnection('config.ini')
            result = my_sql_conn.spexec('sp_api_mob_obtener_cedi_desde_viaje', [id_viaje])

            if result[0][0] != 'OK':
                raise Exception(result[0][1])

            return True, json.loads(result[0][1])

        except Exception as exc:
            logger.error(exc)
            return False, str(exc)
    
    def completar_viaje(self, id_cedi, id_viaje, json_firebase, json_mobility):
        try:
            logger.info('completar_viaje')

            # liquidacion
            if 'Liquidacion' in json_mobility:
                # no cargo la firma 'firma-liquidacion'
                json_firebase['disponibilidad-coordenadas-liquidacion'] = json_mobility.get('Liquidacion', {}).get('disponibilidad-coordenadas-liquidacion')
                json_firebase['exactitud-coordenadas-liquiadcion'] = json_mobility.get('Liquidacion', {}).get('exactitud-coordenadas-liquiadcion')
                json_firebase['fecha-liquidacion'] = json_mobility.get('Liquidacion', {}).get('fecha-liquidacion')
                json_firebase['hora-fecha-automaticas-liquidacion'] = json_mobility.get('Liquidacion', {}).get('hora-fecha-automaticas-liquidacion')
                json_firebase['info-red-liquidacion'] = json_mobility.get('Liquidacion', {}).get('info-red-liquidacion')
                json_firebase['latitud-liquidacion'] = json_mobility.get('Liquidacion', {}).get('latitud-liquidacion')
                json_firebase['longitud-liquidacion'] = json_mobility.get('Liquidacion', {}).get('longitud-liquidacion')
                json_firebase['odometro-final'] = json_mobility.get('Liquidacion', {}).get('odometro-final')
                json_firebase['tipo-red-liquidacion'] = json_mobility.get('Liquidacion', {}).get('tipo-red-liquidacion')

            # aterrizaje
            if 'RegCEDI' in json_mobility:
                json_firebase['disponibilidad-coordenadas-llegada'] = json_mobility.get('RegCEDI', {}).get('disponibilidad-coordenadas-llegada')
                json_firebase['exactitud-coordenadas-llegada'] = json_mobility.get('RegCEDI', {}).get('exactitud-coordenadas-llegada')
                json_firebase['fecha-llegada'] = json_mobility.get('RegCEDI', {}).get('fecha-llegada')
                json_firebase['hora-fecha-automaticas-llegada'] = json_mobility.get('RegCEDI', {}).get('hora-fecha-automaticas-llegada')
                json_firebase['info-red-llegada'] = json_mobility.get('RegCEDI', {}).get('info-red-llegada')
                json_firebase['latitud-llegada'] = json_mobility.get('RegCEDI', {}).get('latitud-llegada')
                json_firebase['longitud-llegada'] = json_mobility.get('RegCEDI', {}).get('longitud-llegada')
                json_firebase['tipo-red-llegada'] = json_mobility.get('RegCEDI', {}).get('tipo-red-llegada')        

            # despegue
            if 'T0' in json_mobility:
                # no cargo 'firma-salida'
                json_firebase['disponibilidad-coordenadas-salida'] = json_mobility.get('T0', {}).get('disponibilidad-coordenadas-salida')
                json_firebase['exactitud-coordenadas-salida'] = json_mobility.get('T0', {}).get('exactitud-coordenadas-salida')
                json_firebase['fecha-salida'] = json_mobility.get('T0', {}).get('fecha-salida')
                json_firebase['hora-fecha-automaticas-salida'] = json_mobility.get('T0', {}).get('hora-fecha-automaticas-salida')
                json_firebase['info-red-salida'] = json_mobility.get('T0', {}).get('info-red-salida')
                json_firebase['latitud-salida'] = json_mobility.get('T0', {}).get('latitud-salida')
                json_firebase['longitud-salida'] = json_mobility.get('T0', {}).get('longitud-salida')
                json_firebase['odometro-inicial'] = json_mobility.get('T0', {}).get('odometro-inicial')
                json_firebase['tipo-red-salida'] = json_mobility.get('T0', {}).get('tipo-red-salida')
                json_firebase['version-mobility'] = json_mobility.get('T0', {}).get('version-mobility')        

            return True, ''

        except Exception as exc:
            logger.error(exc)
            return False, str(exc)
    
    def completar_destinos(self, id_cedi, id_viaje, json_firebase, json_mobility, paradas):
        try:
            logger.info('completar_destinos')

            destinos = []           # lista de destinos final
            destinos.append(None)   # destino inicial 0
            destinos_firebase = json_firebase.get('destinos', [])

            for tirada_plan in range(1, paradas + 1):
                destino = {}       # reset

                # obtengo el destino desde el json_firebase
                if len(destinos_firebase) > tirada_plan:        # la lista de destinos es suf grande
                    if isinstance(destinos_firebase, list):     # es lista
                        destino = destinos_firebase[tirada_plan]
                    else:                                       # es diccionario
                        destino = destinos_firebase.get(str(tirada_plan))

                if not isinstance(destino, dict):               # no existe el destino especifico en firebase
                    destino = {} 

                # completo el destino
                key = 'T{}'.format(tirada_plan)

                if isinstance(json_mobility.get(key), dict):    # ...solo si existe en mobility
                    # no cargo la firma 'firma-salida'
                    # no cargo la firma 'foto-entrega'
                    destino['disponibilidad-coordenadas-llegada'] = json_mobility.get(key).get('disponibilidad-coordenadas-llegada')
                    destino['disponibilidad-coordenadas-salida'] = json_mobility.get(key).get('disponibilidad-coordenadas-salida')
                    destino['exactitud-coordenadas-llegada'] = json_mobility.get(key).get('exactitud-coordenadas-llegada')
                    destino['exactitud-coordenadas-salida'] = json_mobility.get(key).get('exactitud-coordenadas-salida')
                    destino['fecha-llegada'] = json_mobility.get(key).get('fecha-llegada')
                    destino['fecha-salida'] = json_mobility.get(key).get('fecha-salida')
                    destino['hora-fecha-automaticas-llegada'] = json_mobility.get(key).get('hora-fecha-automaticas-llegada')
                    destino['hora-fecha-automaticas-salida'] = json_mobility.get(key).get('hora-fecha-automaticas-salida')
                    destino['info-red-llegada'] = json_mobility.get(key).get('info-red-llegada')
                    destino['info-red-salida'] = json_mobility.get(key).get('info-red-salida')
                    destino['latitud-llegada'] = json_mobility.get(key).get('latitud-llegada')
                    destino['latitud-salida'] = json_mobility.get(key).get('latitud-salida')
                    destino['longitud-llegada'] = json_mobility.get(key).get('longitud-llegada')
                    destino['longitud-salida'] = json_mobility.get(key).get('longitud-salida')
                    destino['odometro-destino'] = json_mobility.get(key).get('odometro-destino')
                    destino['tipo-red-llegada'] = json_mobility.get(key).get('tipo-red-llegada')
                    destino['tipo-red-salida'] = json_mobility.get(key).get('tipo-red-salida')
                    destino['pof'] = {}
                    destino['pof']['docum-completa'] = json_mobility.get(key).get('pof/docum-completa')
                    destino['pof']['entrega'] = json_mobility.get(key).get('pof/entrega')
                    destino['pof']['sin-danos'] = json_mobility.get(key).get('pof/sin-danos')

                # acumulo el destino
                destinos.append(destino)

            json_firebase['destinos'] = destinos        # asigno el json de destinos completo

            return True, ''

        except Exception as exc:
            exc_response = sys.exc_info()
            logger.error('line: {}, {}'.format(exc_response[2].tb_lineno, str(exc))  )
            return False, str(exc)
    
    def completar_actividades(self, id_cedi, id_viaje, json_firebase, json_mobility):
        try:
            logger.info('completar_actividades')

            actividades = {}     # diccionario de actividades

            for item in json_mobility:
                
                if isinstance(item, str) and (item[-2:] == '_A'):
                    actividad_id = item[:-2]
                    actividades[actividad_id] = {
                        # no capturo 'categoria'
                        "disponibilidad-coordenadas-actividad" : json_mobility.get(item).get("disponibilidad-coordenadas-actividad")
                        ,"duracion-actividad" :  json_mobility.get(item).get("duracion-actividad")
                        ,"etapa-actividad" : json_mobility.get(item).get("etapa-actividad")
                        ,"exactitud-coordenadas-actividad" : json_mobility.get(item).get("exactitud-coordenadas-actividad")
                        ,"fecha-actividad" : json_mobility.get(item).get("fecha-actividad")
                        ,"hora-fecha-automaticas-actividad" : json_mobility.get(item).get("hora-fecha-automaticas-actividad")
                        ,"info-red-actividad" : json_mobility.get(item).get("info-red-actividad")
                        ,"latitud-actividad" : json_mobility.get(item).get("latitud-actividad")
                        ,"longitud-actividad" : json_mobility.get(item).get("longitud-actividad")
                        ,"tipo-actividad" : json_mobility.get(item).get("tipo-actividad")
                        ,"tipo-red-actividad" : json_mobility.get(item).get("tipo-red-actividad")
                        ,"tirada-actividad" : json_mobility.get(item).get("tirada-actividad")
                    }    

            json_firebase['actividades'] = actividades

            return True, ''

        except Exception as exc:
            exc_response = sys.exc_info()
            logger.error('line: {}, {}'.format(exc_response[2].tb_lineno, str(exc))  )
            return False, str(exc)

    
    def completar_incidentes(self, id_cedi, id_viaje, json_firebase, json_mobility):
        try:
            logger.info('completar_incidentes')

            if not isinstance(json_firebase, dict):
                json_firebase = {}

            for item in json_mobility:
                
                if isinstance(item, str):
                    if item[-2:] == '_I':
                        incidente_id = item[:-2]
                        json_firebase[incidente_id] = {
                            # no capturo 'fecha-firebase'
                            "categoria" : json_mobility.get(item).get('categoria')
                            ,"disponibilidad-coordenadas" : json_mobility.get(item).get('disponibilidad-coordenadas')
                            ,"exactitud-coordenadas" : json_mobility.get(item).get('exactitud-coordenadas')
                            ,"fecha" : json_mobility.get(item).get('fecha')
                            ,"hora-fecha-automaticas" : json_mobility.get(item).get('hora-fecha-automaticas')
                            ,"info-red" : json_mobility.get(item).get('info-red')
                            ,"latitud" : json_mobility.get(item).get('latitud')
                            ,"longitud" : json_mobility.get(item).get('longitud')
                            ,"nombreDestino" : json_mobility.get(item).get('nombreDestino')
                            ,"tipo" : json_mobility.get(item).get('tipo')
                            ,"tipo-red" : json_mobility.get(item).get('tipo-red')
                            ,"tiradaDestino" : json_mobility.get(item).get('tiradaDestino')
                        }

                    elif item[-2:] == 'I2':
                        incidente_id = item[:-3]                    
                        json_firebase[incidente_id] = {
                            # no capturo 'fecha-firebase'
                            "categoria" : json_mobility.get(item).get('categoria')
                            ,"comentario" : json_mobility.get(item).get('comentario')
                            ,"disponibilidad-coordenadas" : json_mobility.get(item).get('disponibilidad-coordenadas')
                            ,"etapas" : json_mobility.get(item).get('etapa')
                            ,"exactitud-coordenadas" : json_mobility.get(item).get('exactitud-coordenadas')
                            ,"fecha" : json_mobility.get(item).get('fecha')
                            ,"foto" : self.obtener_archivo(json_mobility.get(item).get('foto'))
                            ,"hora-fecha-automaticas" : json_mobility.get(item).get('hora-fecha-automaticas')
                            ,"info-red" : json_mobility.get(item).get('info-red')
                            ,"latitud" : json_mobility.get(item).get('latitud')
                            ,"longitud" : json_mobility.get(item).get('longitud')
                            ,"tipo" : json_mobility.get(item).get('tipo')
                            ,"tipo-red" : json_mobility.get(item).get('tipo-red')
                        }    

            return True, ''

        except Exception as exc:
            exc_response = sys.exc_info()
            logger.error('line: {}, {}'.format(exc_response[2].tb_lineno, str(exc))  )
            return False, str(exc)  

    def obtener_archivo(self, key):

        if isinstance(key, str):
            file_name = key.split('/')[-1]

        else:
            file_name = key

        return file_name


class ViajeIncidente:
    """Clase para incidentes de mobility"""
    def registrar_incidente(self, event):
        """Registrar incidente si las notificaciones estan activadas para el cedi"""
        try:
            logger.info('registrar_incidente')
            correo = event['correo_operador']
            comentario = event['comentario']
            viaje_id   = event['id_viaje']
            etapa = event['etapa']
            fecha = event['fecha']
            foto = event['foto']
            latitud = event['latitud']
            longitud = event['longitud']
            tipo = event['tipo']
            operador = event['operador']
            destino = event['destino']
            print(correo)
            status, result = self.notificacion_activa(viaje_id)
            if status:
                if result['NotificarIncidentes'] == 1:
                    uid = uuid.uuid4()
                    cedi_id = result['idCedi']
                    json_incidente = {
                        "comentario": comentario,
                        "etapa": etapa,
                        "fecha": fecha,
                        "foto": foto,
                        "latitud": float(latitud),
                        "longitud": float(longitud),
                        "tipo":tipo,
                        "operador":operador,
                        "correoOperador":correo,
                        "destino": destino
                    }
                    status, mensaje = self.escribir_notificacion(viaje_id, cedi_id, uid, json_incidente)
                    if status:
                        return True, 'OK'
                    else:
                        return False, mensaje
                else:
                    return True, "La empresa no tiene activadas las notificaciones"
            else:
                return True, "La empresa no tiene activadas las notificaciones"
        except Exception as exc:
            logger.error(exc)
            return False, str(exc)


    def notificacion_activa(self, viaje_id):
        """Verificar notificación activada para cedi"""
        try:
            logger.info('notificacion_activa')
            mysql_conn = MySQLConnection('config.ini')
            args = [viaje_id]
            result = mysql_conn.spexec('sp_api_incidentes_activo', args)  # LLAMADA AL SP
            if result[0][0] == 'OK':
                return True, json.loads(result[0][1])
            else:
                return False, result[0][1]
        except Exception as exc:
            logger.error(exc)
            return False, str(exc)


    def escribir_notificacion(self, viaje_id, cedi_id, uid, json_incidente):
        """Registrar notificación en Firebase"""
        # TODO: Registrar en Firebase la información que solicita
        try:
            logger.info('escribir_notificacion')
            fbm = FirebaseMethods('config.ini')
            status, mensaje = fbm.escribir_incidente(cedi_id, viaje_id, uid, json_incidente)
            return status, mensaje
        except Exception as exc:
            logger.error(exc)
            return False, str(exc)
