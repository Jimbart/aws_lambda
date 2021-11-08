# -*- coding: utf-8 -*-

# Rach viernes, 9 de agosto de 2019
# Para construir el ETL de MySQL y Firebase a BQ

from logging import getLogger, basicConfig
from database_services.mysql_service import MySQLConnection
from database_services.firebasemethods import FirebaseMethods
from database_services.bigqueryMethods import BigQueryMethods


import json, sys
import configparser
import pandas as pd
import numpy as np
import config
from haversine import haversine, Unit
from datetime import datetime as dt
from datetime import timedelta as td
from pytz import timezone as tz

logger = getLogger()


class ETL:
    def __init__(self,params):
        self.empresa_id = params.get('empresa_id')
        self.cedi_id = params.get('cedi_id')
        self.viaje = params.get('viaje',0)
        self.ignore_pof_completo = params.get('ignore_pof_completo',True)
        self.ignore_pof_danios = params.get('ignore_pof_danios',True)
        self.ignore_pof_doctos = params.get('ignore_pof_doctos',True)
        self.tolerancia = params.get('tolerancia',5)
        self.fbm = FirebaseMethods('config.ini')
        self.bqm = BigQueryMethods('config.ini')
        self.mysql_conn = MySQLConnection('config.ini')
    
    
    def transfer_viajes(self):
        """Transferir la info de los viajes a BQ"""
        try:
            logger.info('transfer_viajes')
            #TODO: Obtener df de viajes planeados status 9 y 9.5
            status, result = self.obtener_viajes_por_status()
            if not status:
                raise Exception(result)

            status, result = self.normaliza_dataframes_plan(result)

            if not status:
                raise Exception(result)

            df_pedidos_plan = result[4]
            df_paradas_plan = result[3]
            df_itinerario_plan = result[2]
            df_viajes_plan = result[1]

            if df_viajes_plan.shape[0] > 0:
                # ... inyecto hacia bigquery
                status, info = self.bqm.cargar_datos(
                    'arms4_viajes_plan', df_viajes_plan, ['idEmpresa', 'Viaje'], 'Viaje')
                self.validate_responses(status, info)

                status, info = self.bqm.cargar_datos(
                    'arms4_nodos_plan', df_paradas_plan, ['idEmpresa', 'Viaje', 'TiradaPlan'], 'TiradaPlan')
                self.validate_responses(status, info)

                status, info = self.bqm.cargar_datos(
                    'arms4_pedidos_plan', df_pedidos_plan, ['idEmpresa', 'Pedido', 'Viaje'], 'Pedido')
                self.validate_responses(status, info)

                status, info = self.bqm.cargar_datos(
                    'arms4_itinerario_plan', df_itinerario_plan, ['Viaje', 'idItinerario'], 'idItinerario')
                self.validate_responses(status, info)

                status, info = self.actualizar_escritoBQ(df_viajes_plan)
                self.validate_responses(status, info)


            status, result = self.obtener_viajes_expiracion()
            if not status:
                raise Exception(result)

            df_viajes_expiracion = result[1]

            print(df_viajes_expiracion)

            #df_trasacciones_real = pd.DataFrame()
            df_destinos_real = pd.DataFrame()
            df_gastos_real = pd.DataFrame()
            df_viajes_real_mysql = df_viajes_expiracion[df_viajes_expiracion['Status']==9.5]
            ids_viajes_real_mysql = df_viajes_real_mysql['Viaje'].to_list()
            trasacciones_real = []
            destinos_real = []
            df_viajes_real_mysql.set_index('Viaje', drop=True, inplace=True)
            df_viajes_real_mysql.loc[df_viajes_real_mysql[df_viajes_real_mysql['FechaRetornoPlan'].isna()].index, 'Expirado'] = 1
            #TODO: Obtener df de viajes reales de la lista ids_viajes_real_mysql
            for viaje in ids_viajes_real_mysql:
                viaje_series, destino_df = self.leer_real(viaje, df_viajes_real_mysql)



                if not viaje_series.empty:
                    df_actividades = self.actividades_real(viaje)

                    viaje_df = pd.DataFrame(data=[viaje_series], columns=viaje_series.index.values).merge(df_actividades
                                                                                           , on='Viaje')
                    trasacciones_real.append(viaje_df)
                if not destino_df.empty:
                    destinos_real.append(destino_df)
            #     status, df_trasacciones, df_destinos, df_gastos = self.obtener_viaje_real(viaje)
            #     if not status:
            #         raise Exception(df_trasacciones)
            #     df_trasacciones_real = pd.concat([df_trasacciones_real, df_trasacciones])
            #     df_destinos_real = pd.concat([df_destinos_real, df_destinos])
            #     df_gastos_real = pd.concat([df_gastos_real, df_gastos])
            # print(df_trasacciones_real)
            # print(df_destinos_real)
            df_trasacciones_real = pd.concat(trasacciones_real)
            df_destinos_real = pd.concat(destinos_real)
            print(df_trasacciones_real)
            print(df_destinos_real)
            status, results = self.obtener_pedidos(df_viajes_real_mysql)
            if not status:
                raise Exception(result)
            df_info_pedidos = results[1]
            df_pedidos_real = self.normalizar_real_pedidos(df_info_pedidos, df_trasacciones_real, df_destinos_real)
            # status, info = self.actualizar_status(df_viajes)


            return True, ''

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            import traceback
            traceback.print_exc()
            #fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            error = '{} - linea {}  ({})'.format(exc_type, exc_tb.tb_lineno, str(e))
            print(error)
            return False, 0, 0


            exc_output = sys.exc_info()
            logger.error('{},{}'.format(str(exc),str(exc_output[2].tb_lineno)))
            return False, str(exc)

    def obtener_viajes_por_status(self):
        """Obtener insumos de viajes por status"""
        try:
            logger.info('obtener_viajes_por_status')
            args = [self.cedi_id]

            result = self.mysql_conn.spexec_to_dfs('sp_api_etl_obtener_viajes', args)
            if result[0].loc[0, 'result'] != 'OK':
                raise Exception(result[0].loc[0, 'data'])

            return True, result

        except Exception as exc:
            exc_output = sys.exc_info()
            logger.error('{},{}'.format(str(exc),str(exc_output[2].tb_lineno)))
            return False, str(exc)

    def obtener_viajes_expiracion(self):
        """Obtener insumos de viajes por status"""
        try:
            logger.info('obtener_viajes_expiracion')
            args = [self.cedi_id]

            result = self.mysql_conn.spexec_to_dfs('sp_api_etl_viajes_expirar', args)
            if result[0].loc[0, 'result'] != 'OK':
                raise Exception(result[0].loc[0, 'data'])

            return True, result

        except Exception as exc:
            exc_output = sys.exc_info()
            logger.error('{},{}'.format(str(exc),str(exc_output[2].tb_lineno)))
            return False, str(exc)

    def normaliza_dataframes_plan(self, result):

        try:
            logger.info('normaliza_dataframes_plan')
            status = False

            # Los dataframes vienen en una lista en el result
            df_pedidos_plan = result[4]
            df_paradas_plan = result[3]
            df_itinerario_plan = result[2]
            df_viajes_plan = result[1]

            if df_viajes_plan.shape[0] > 0:

                # Obtiene costo por nodo y por pedido de la de viajes
                df_viajes_plan['P_Costo_Total_Nodo'] = df_viajes_plan['CostoTotalPlan'] / df_viajes_plan['Tiradas']
                df_viajes_plan['P_Costo_Total_Pedido'] = df_viajes_plan['CostoTotalPlan'] / df_viajes_plan['Pedidos']

                # Merge de nodos y viajes para completar campos en nodos
                df_paradas_plan = pd.merge(df_paradas_plan, df_viajes_plan[config.columnas_viajesplan_a_tiradas], on='Viaje', how='inner')

                # Merge de pedidos con nodos y viajes para completar campos en pedidos
                df_pedidos_plan = pd.merge(df_pedidos_plan, df_viajes_plan[config.columnas_viajesplan_a_tiradas], on='Viaje', how='inner')
                df_pedidos_plan = pd.merge(df_pedidos_plan, df_paradas_plan[config.columnas_tiradasplan_a_pedidos], on='idParada', how='inner')

                # Se tiran las columnas de costo que no corresponden a la agregación
                df_viajes_plan.drop(['P_Costo_Total_Nodo', 'P_Costo_Total_Pedido'], inplace=True, axis=1)
                df_paradas_plan.drop(['P_Costo_Total_Pedido'], inplace=True, axis=1)
                df_pedidos_plan.drop(['P_Costo_Total_Nodo'], inplace=True, axis=1)

                # Agrupa los campos de pedidos para sumar (para llevarlos a dataframe nodos)
                df_pedidos_plansum = df_pedidos_plan[config.columnas_pedidosplan_a_tiradas_sum].groupby(['idParada']).sum()
                df_pedidos_plansum.reset_index(inplace=True)

                # Agrupa los campos de pedidos para contar (para llevarlos a dataframe nodos)
                df_pedidos_plancount = df_pedidos_plan[['idParada', 'Pedido']].groupby(['idParada']).count()
                df_pedidos_plancount.reset_index(inplace=True)

                # Agrupa los campos de pedidos con el primero (para llevarlos a dataframe nodos)
                df_pedidos_planfirst = df_pedidos_plan[config.columnas_pedidosplan_a_tiradas_first].groupby(['idParada']).first()
                df_pedidos_planfirst.reset_index(inplace=True)

                # Agrupa los campos de pedidos para concatenar (para llevarlos a dataframe nodos)
                df_pedidos_plan['DestinoTR2'] = df_pedidos_plan['DestinoTR2'].astype('str')
                df_pedidos_planconcat = df_pedidos_plan[['idParada', 'DestinoTR2']].drop_duplicates(subset=['idParada', 'DestinoTR2'], keep='first', inplace=False).copy()
                df_pedidos_planconcat = df_pedidos_planconcat.groupby(['idParada'])['DestinoTR2'].apply(lambda x: "%s" % ', '.join(x))
                df_pedidos_planconcat = pd.DataFrame(df_pedidos_planconcat)
                df_pedidos_planconcat.reset_index(inplace=True)

                # Merge de las agrupaciones de pedidos con nodos
                df_paradas_plan = pd.merge(df_paradas_plan, df_pedidos_plansum, on='idParada', how='inner')
                df_paradas_plan = pd.merge(df_paradas_plan, df_pedidos_plancount, on='idParada', how='inner')
                df_paradas_plan = pd.merge(df_paradas_plan, df_pedidos_planfirst, on='idParada', how='inner')
                df_paradas_plan = pd.merge(df_paradas_plan, df_pedidos_planconcat, on='idParada', how='inner')

                # Renombrar columnas de las agrupaciones
                df_paradas_plan.rename(columns={'Pedido': 'Pedidos', 'VendedorPedido': 'VendedorDestino'}, inplace=True)

                # Obtiene los hitos de trayecto en el itinerario (para obtener el trayecto anterior y posterior sobre nodos)
                df_itinerario_trayecto = df_itinerario_plan.loc[df_itinerario_plan['Actividad'] == 'Trayecto', ['Viaje', 'TiradaPlan', 'Duracion']].copy()
                df_itinerario_trayecto = df_itinerario_trayecto.groupby(['Viaje', 'TiradaPlan']).sum()
                df_itinerario_trayecto.reset_index(inplace=True)

                # Renombre las sumas de trayectos y obtiene campo con la tirada anterior (para trayecto de la tirada siguiente)
                df_itinerario_trayecto.rename(columns={'Duracion': 'P_Duracion_Traslado_Destino_Anterior'}, inplace=True)
                df_itinerario_trayecto['P_Duracion_Traslado_Destino_Siguiente'] = df_itinerario_trayecto['P_Duracion_Traslado_Destino_Anterior']
                df_itinerario_trayecto['TiradaMinus'] = df_itinerario_trayecto['TiradaPlan'] - 1

                # Merge de nodos con itinerario (traslado destino anterior)
                df_paradas_plan = pd.merge(df_paradas_plan, df_itinerario_trayecto[['Viaje', 'TiradaPlan', 'P_Duracion_Traslado_Destino_Anterior']], on=['Viaje', 'TiradaPlan'], how='left')

                # Merge de nodos con itinerario (traslado destino siguiente)
                df_paradas_plan = pd.merge(df_paradas_plan, df_itinerario_trayecto[['Viaje', 'TiradaMinus', 'P_Duracion_Traslado_Destino_Siguiente']],
                                           left_on=['Viaje', 'TiradaPlan'], right_on=['Viaje', 'TiradaMinus'], how='left')
                df_paradas_plan.drop(['TiradaMinus'], inplace=True, axis=1)


                # Encuentra los servicios del itinerario para agruparlos en la actividad "Servicio"
                idxEntrega = df_itinerario_plan['Actividad'].str.contains('Entrega')
                df_itinerario_plan.loc[idxEntrega, 'Actividad'] = 'Servicio'

                # Se obtiene el nombre del destino de la tirada en cuestión para todas las actividades que llevan a ese destino
                df_itinerario_plan = pd.merge(df_itinerario_plan, df_paradas_plan[['Viaje', 'TiradaPlan', 'Destino']], on=['Viaje', 'TiradaPlan'], how='left')
                # Obtiene el total de tiradas por viaje
                df_itinerario_plan = pd.merge(df_itinerario_plan, df_viajes_plan[['Viaje', 'Tiradas']], on=['Viaje'], how='left')

                # Pone el nombre del destino "CEDI" tanto en la tirada 0 como en la tirada de "retorno"
                df_itinerario_plan.loc[df_itinerario_plan['TiradaPlan'] == 0, 'Destino'] = df_itinerario_plan.loc[df_itinerario_plan['TiradaPlan'] == 0, 'Cedis']
                df_itinerario_plan.loc[df_itinerario_plan['TiradaPlan'] > df_itinerario_plan['Tiradas'], 'Destino'] =\
                    df_itinerario_plan.loc[df_itinerario_plan['TiradaPlan'] > df_itinerario_plan['Tiradas'], 'P_Cedis_Aterriza']

                # Quita las columnas auxiliares al itinerario y lo ordena
                df_itinerario_plan.drop(['Cedis', 'P_Cedis_Aterriza', 'Tiradas'], inplace=True, axis=1)
                df_itinerario_plan.sort_values(by=['Viaje', 'FechaInicial', 'FechaFinal'], ascending=[True, True, True], inplace=True)

                # Reordena las columnas de los dataframes con los del modelo de datos
                df_viajes_plan = self.modifica_df_x_tipodato(df_viajes_plan, config.df_campos_viajes)
                df_paradas_plan = self.modifica_df_x_tipodato(df_paradas_plan,  config.df_campos_nodos)
                df_pedidos_plan = self.modifica_df_x_tipodato(df_pedidos_plan,  config.df_campos_pedidos)
                df_itinerario_plan = self.modifica_df_x_tipodato(df_itinerario_plan,  config.df_campos_itinerario)

                # Reordena las columnas de los dataframes con los del modelo de datos
                df_viajes_plan = df_viajes_plan[config.columnas_viajesplan]
                df_paradas_plan = df_paradas_plan[config.columnas_nodosplan]
                df_pedidos_plan = df_pedidos_plan[config.columnas_pedidosplan]
                df_itinerario_plan = df_itinerario_plan[config.columnas_itinerarioplan]

            status = True

            result2 = list()
            result2.append('OK')
            result2.append(df_viajes_plan)
            result2.append(df_itinerario_plan)
            result2.append(df_paradas_plan)
            result2.append(df_pedidos_plan)

            return status, result2


        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            # fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            error = '{} - linea {}  ({})'.format(exc_type, exc_tb.tb_lineno, str(e))
            print(error)
            return False, []

    def modifica_df_x_tipodato(self, df, df_columnas):

        try:

            logger.info("modifica_df_x_tipodato ")

            # Obtiene la lista de campos del df actual
            lista_campos = df.columns.tolist()

            # Por cada campo
            for i in lista_campos:

                #Busca el tipo de dato que deb de ser en el df de campos correspondiente
                tipo_dato = df_columnas.loc[df_columnas['Campo'] == i, 'Tipo']
                # Si si lo encuentra en el df de campos
                if tipo_dato.shape[0] > 0:
                    tipo_dato = tipo_dato.values[0]
                    tipo_actual = df[i].dtype

                    #Realiza la acción correspondiente según el tipo de dato dado
                    if tipo_dato == 'TIMESTAMP' or tipo_dato == 'DATETIME':
                        df[i] = pd.to_datetime(df[i], format='%Y-%m-%d %H:%M:%S', errors='coerce')
                        df[i].loc[df[i].notnull()] = df[i].loc[df[i].notnull()].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))

                    elif tipo_dato == 'DATE':
                        df[i] = pd.to_datetime(df[i], format='%Y-%m-%d', errors='coerce').dt.date
                        df[i].loc[df[i].notnull()] = df[i].loc[df[i].notnull()].apply(lambda x: x.strftime('%Y-%m-%d'))

                    elif tipo_dato == 'TIME':
                        df[i] = pd.to_datetime(df[i], format='%H:%M:%S', errors='coerce').dt.time
                        df[i].loc[df[i].notnull()] = df[i].loc[df[i].notnull()].apply(lambda x: x.strftime('%H:%M:%S'))

                    elif tipo_dato == 'FLOAT' and tipo_actual != 'float64':
                        df[i] = df[i].astype(float, errors='ignore')

                    elif tipo_dato == 'INTEGER' and tipo_actual != 'Int64':
                        df[i] = df[i].astype('Int64', errors='ignore')

                    # Finalmente pasa el tipo de dato a string y en caso de ser nulo a None
                    df[i] = df[i].astype(str).where(df[i].notnull(), None)

            return df

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            # fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            error = '{} - linea {}  ({})'.format(exc_type, exc_tb.tb_lineno, str(e))
            print(error)
            return pd.DataFrame(), []


    def actualizar_status(self, df_viajes):
        logger.info('actualizar_status')
        try:
            viajes_list = []
            df_viajes = df_viajes.reset_index(drop=True)

            for row in range(df_viajes.shape[0]):
                viajes_list.append(
                    {
                        'viaje': int(df_viajes.loc[row, 'Viaje'])
                        , 'status': df_viajes.loc[row, 'Status']
                    }
                )

            my_sql_conn = MySQLConnection('config.ini')
            print(json.dumps(viajes_list))
            args = [self.cedi_id, json.dumps(viajes_list)]
            result = my_sql_conn.spexec('sp_etl_actualizar_status_viajes', args)

            if result[0][0] != 'OK':
                raise Exception(result[0][1])

            return result[0][0], result[0][1]

        except Exception as exc:
            exc_output = sys.exc_info()
            logger.error('{},{}'.format(str(exc), str(exc_output[2].tb_lineno)))
            raise Exception(str(exc))

    def obtener_pedidos(self, df_viajes):
        logger.info('obtener_pedidos')
        try:
            viajes_list = []

            for viaje in df_viajes.index.values:
                if df_viajes.loc[viaje,'Status'] > 9.5:
                    viajes_list.append(
                        {
                        'viaje': int(viaje)
                        }
                    )

            my_sql_conn = MySQLConnection('config.ini')
            args = [self.cedi_id, json.dumps(viajes_list)]
            result = self.mysql_conn.spexec_to_dfs('sp_etl_obtener_pedidos_facturas', args)
            print(args, result)
            if result[0].loc[0, 'result'] != 'OK':
                raise Exception(result[0].loc[0, 'data'])

            return True, result

        except Exception as exc:
            exc_output = sys.exc_info()
            logger.error('{},{}'.format(str(exc), str(exc_output[2].tb_lineno)))
            raise Exception(str(exc))

    def actualizar_escritoBQ(self, df_viajes):
        logger.info('actualizar_escritoBQ')
        try:
            viajes_list = []
            df_viajes = df_viajes.reset_index(drop=True)

            for row in range(df_viajes.shape[0]):
                viajes_list.append(
                    {
                        'viaje': int(df_viajes.loc[row, 'Viaje'])
                        , 'escritoBQ': 1
                    }
                )

            my_sql_conn = MySQLConnection('config.ini')
            args = [self.cedi_id, json.dumps(viajes_list)]

            result = my_sql_conn.spexec('sp_etl_actualiza_col_escrito_bq', args)

            print(result)

            if result[0][0] != 'OK':
                raise Exception(result[0][1])

            return result[0][0], result[0][1]

        except Exception as exc:
            exc_output = sys.exc_info()
            logger.error('{},{}'.format(str(exc), str(exc_output[2].tb_lineno)))
            raise Exception(str(exc))

    def obtener_viaje_real(self, viaje_id):
        """Para obtener los viajes de firebase"""
        try:
            logger.info('obtener_viaje_real')
            df_gastos = pd.DataFrame()
            df_destinos = pd.DataFrame()
            df_trasacciones = pd.DataFrame()
            transacciones_firebase = self.fbm.get(self.fbm.nodo_transacciones(self.cedi_id,viaje_id), {})
            # incidentes_firebase = self.fbm.get(self.fbm.nodo_incidentes(self.cedi_id,viaje_id), {}) -- cloudFunctions
            gastos_firebase = self.fbm.get(self.fbm.nodo_gastos(self.cedi_id,viaje_id), {})
            if gastos_firebase != {}:
                df_gastos = pd.DataFrame(list(gastos_firebase.items()))
                df_gastos = df_gastos.set_index(0).T
                df_gastos['idViaje'] = viaje_id
            if transacciones_firebase != {}:
                df_trasacciones = pd.DataFrame(list(transacciones_firebase.items())) 
                df_trasacciones = df_trasacciones.set_index(0).T
                df_trasacciones['idViaje'] = viaje_id
                status, df_destinos = self.destinos_real(transacciones_firebase)
                if not status:
                    raise Exception(df_destinos)
                df_destinos['idViaje'] = viaje_id
            return True, df_trasacciones, df_destinos, df_gastos
        except Exception as exc:
            exc_output = sys.exc_info()
            logger.error('{},{}'.format(str(exc),str(exc_output[2].tb_lineno)))
            return False, str(exc), '', ''
    
    def destinos_real(self, transacciones):
        try:
            logger.info('destinos_real')
            #TODO: Limpiar lista
            status, destinos = self.quitar_none_list(transacciones['destinos'])
            if not status:
                raise Exception(destinos)
            df_destinos = pd.DataFrame(destinos) 
            return True, df_destinos
        except Exception as exc:
            exc_output = sys.exc_info()
            logger.error('{},{}'.format(str(exc),str(exc_output[2].tb_lineno)))
            return False, str(exc)

    def actividades_real(self, viaje):

        try:

            # Lee el nodo de actividades
            actividades_firebase = self.fbm.get(self.fbm.nodo_actividades(self.cedi_id, viaje), {})
            # Crea el dataframe de actividades
            df_actividades = pd.DataFrame.from_dict(actividades_firebase, orient='index')

            with pd.option_context('display.max_rows', None, 'display.max_columns', 1000):
                print(df_actividades)

            # Enlista las columnas que deben estar si o si en el dataframe (aunque no hayan tenido una actividad al respecto
            columnas_viaje_agregacion_actividades_sum = list(config.diccionario_cambio_actvidadduracion_fb_df.values())
            columnas_viaje_agregacion_actividades_count = list(config.diccionario_cambio_actvidadcount_fb_df.values())

            # Si si existen actividades para el viaje
            if df_actividades.shape[0] > 0:

                # Solo obtiene el tipo de actividad y su fecha de fin de actividad para la agrupación
                df_actividades = df_actividades.loc[:, ['tipo-actividad', 'fecha-actividad', 'tirada-actividad']]
                df_actividades.reset_index(inplace=True, drop=True)
                # Pasa la fecha de string a datetime
                df_actividades['fecha-actividad'] = df_actividades['fecha-actividad'].str[:19]
                df_actividades['fecha-actividad'] = pd.to_datetime(df_actividades['fecha-actividad'], format='%Y-%m-%d %H:%M:%S', errors='coerce')

                # Ordena el dataframe por fecha de actividad
                df_actividades.sort_values(by=['fecha-actividad'], inplace=True)
                df_actividades.reset_index(inplace=True, drop=True)
                # Obtiene la fecha de inicio de la actividad (es el fin de la actividad anterior)
                df_actividades['fecha-ini-actividad'] = df_actividades['fecha-actividad'].shift(1)

                # El inicio de la primera actividad (generalmente, "VerViaje") le asignamos fecha de inicio igual a la de fin
                # (aunque esta se ve con duracion 0 no hay consecuencia practica
                df_actividades.loc[0, 'fecha-ini-actividad'] = df_actividades.loc[0, 'fecha-actividad']

                # Se obtiene la duracion de cada actividad
                df_actividades['duracion-actividad'] = (df_actividades['fecha-actividad'] - df_actividades['fecha-ini-actividad'])
                # En segundos
                df_actividades['duracion-actividad'] = df_actividades['duracion-actividad'].dt.total_seconds()

                # Se obtiene el total de duración y el conteo por tipo de actividad
                df_actividades_sum = df_actividades[['tipo-actividad', 'duracion-actividad']].groupby(['tipo-actividad']).sum()
                df_actividades_count = df_actividades[['tipo-actividad', 'duracion-actividad']].groupby(['tipo-actividad']).count()


                # Se obtiene el total de duración y el conteo por tipo de actividad para destinos también
                df_actividades_nodos_sum = df_actividades[config.columnas_agrupacion_dfactividades_nodos].groupby(['tipo-actividad', 'tirada-actividad']).sum()

                df_actividades_nodos_sum.reset_index(inplace=True)
                df_actividades_nodos_sum['tirada-actividad'] = df_actividades_nodos_sum['tirada-actividad'].astype(int, errors='ignore')

                with pd.option_context('display.max_rows', None, 'display.max_columns', 1000):
                    print("df_actividades_nodos_sum ----------------------------------------------------------------")
                    print(df_actividades_nodos_sum)
                    print(df_actividades_nodos_sum.dtypes)

                df_act_nodos_trayecto = df_actividades_nodos_sum.loc[df_actividades_nodos_sum['tipo-actividad'].isin(['RegresoCEDI', 'Trayecto']), :].copy()

                df_reg_cedi = df_act_nodos_trayecto.loc[df_act_nodos_trayecto['tipo-actividad'] == 'RegresoCEDI', :]

                if df_reg_cedi.shape[0] > 0:

                    df_act_nodos_trayecto.loc[df_act_nodos_trayecto['tipo-actividad'] == 'RegresoCEDI', 'tirada-actividad'] =\
                        df_act_nodos_trayecto.loc[df_act_nodos_trayecto['tipo-actividad'] == 'RegresoCEDI', 'tirada-actividad'] + 1

                    df_act_nodos_trayecto.loc[df_act_nodos_trayecto['tipo-actividad'] == 'RegresoCEDI', 'tipo-actividad'] = 'Trayecto'

                df_act_nodos_trayecto.sort_values('tirada-actividad', inplace=True)
                df_act_nodos_trayecto['duracion-actividadpos'] = df_act_nodos_trayecto['duracion-actividad'].shift(-1)

                if df_reg_cedi.shape[0] > 0:
                    df_act_nodos_trayecto = df_act_nodos_trayecto.iloc[:-1, :]

                df_act_nodos_servicio = df_actividades_nodos_sum.loc[df_actividades_nodos_sum['tipo-actividad'] == 'Servicio', :]
                df_act_nodos_espera = df_actividades_nodos_sum.loc[df_actividades_nodos_sum['tipo-actividad'] == 'Espera', :]

                df_act_nodos_trayecto.rename(columns=config.diccionario_cambio_actividad_fb_dfnodos_tray, inplace=True)
                df_act_nodos_servicio.rename(columns=config.diccionario_cambio_actividad_fb_dfnodos_servicio, inplace=True)
                df_act_nodos_espera.rename(columns=config.diccionario_cambio_actividad_fb_dfnodos_espera, inplace=True)

                df_act_nodos_trayecto.drop(['tipo-actividad'], inplace=True, axis=1)
                df_act_nodos_servicio.drop(['tipo-actividad'], inplace=True, axis=1)
                df_act_nodos_espera.drop(['tipo-actividad'], inplace=True, axis=1)

                df_act_nodos = pd.merge(df_act_nodos_trayecto, df_act_nodos_servicio, on='tirada-actividad', how='outer')
                df_act_nodos = pd.merge(df_act_nodos, df_act_nodos_espera, on='tirada-actividad', how='outer')

                df_act_nodos.rename(columns={'tirada-actividad': 'TiradaPlan'}, inplace=True)
                df_act_nodos['R_Duracion_EsperaTrayecto'] = 0
                df_act_nodos['Viaje'] = viaje

                '''
                with pd.option_context('display.max_rows', None, 'display.max_columns', 1000):
                    print(df_act_nodos_trayecto)
                    print(df_act_nodos_servicio)
                    print(df_act_nodos_espera)
                    print("-------------o-o-o-o-o-o-o--o-o-o--o-o")
                    print(df_act_nodos)
                '''

                # Se transpone el df ya que se quiere una columna por cada actividad
                df_actividades_sum = df_actividades_sum.transpose()
                df_actividades_count = df_actividades_count.transpose()

                # Se renombran las columnas de actividades para coincidir con el modelo de datos de la base
                df_actividades_sum.rename(columns=config.diccionario_cambio_actvidadduracion_fb_df, inplace=True)
                df_actividades_count.rename(columns=config.diccionario_cambio_actvidadcount_fb_df, inplace=True)

                # Reset index para quitar el nombre del indice
                df_actividades_sum.reset_index(inplace=True, drop=True)
                df_actividades_count.reset_index(inplace=True, drop=True)
                df_actividades_sum.columns.name = ''
                df_actividades_count.columns.name = ''

                # Por cada columna en el dataframe de duración que debe de estar, la agrega si no existe
                for i in columnas_viaje_agregacion_actividades_sum:

                    if i not in df_actividades_sum.columns:
                        df_actividades_sum.insert(loc=len(df_actividades_sum.columns), column=i, value=0)

                # Por cada columna en el dataframe de conteo de actividades que debe de estar, la agrega si no existe
                for i in columnas_viaje_agregacion_actividades_count:

                    if i not in df_actividades_count.columns:
                        df_actividades_count.insert(loc=len(df_actividades_count.columns), column=i, value=0)

                # Dado que el trayecto de retorno se escribe regesocedi en firebase, duracion traslado sería la suma de regreso cedi y traslado
                if 'RegresoCEDI' in df_actividades_sum.columns:
                    if 'R_Duracion_Traslado' in df_actividades_sum.columns:
                        df_actividades_sum['R_Duracion_Traslado'] = df_actividades_sum['R_Duracion_Traslado'] + df_actividades_sum['RegresoCEDI']
                    else:
                        df_actividades_sum['R_Duracion_Traslado'] = df_actividades_sum['RegresoCEDI']

                # Mismo caso en el conteo de actividades
                if 'RegresoCEDI' in df_actividades_count.columns:
                    if 'R_Actividad_Traslado' in df_actividades_sum.columns:
                        df_actividades_count['R_Actividad_Traslado'] = df_actividades_count['R_Actividad_Traslado'] + df_actividades_count['RegresoCEDI']
                    else:
                        df_actividades_count['R_Actividad_Traslado'] = df_actividades_count['RegresoCEDI']

                # Se queda solamente con las columnas que debe tener el dataframe
                df_actividades_sum = df_actividades_sum[columnas_viaje_agregacion_actividades_sum]
                df_actividades_count = df_actividades_count[columnas_viaje_agregacion_actividades_count]

                # Columnas que no vienen directamente en las actividades de firebase
                df_actividades_sum['R_Duracion_Ajuste'] = 0
                df_actividades_sum['R_Duracion_EsperaTrayecto'] = 0
                df_actividades_count['R_Actividad_Ajuste'] = 0
                df_actividades_count['R_Actividad_EsperaTrayecto'] = 0

                # La columna de total es la suma de las otras
                df_actividades_sum['R_Duracion_Total_Viaje'] = df_actividades_sum.sum(axis=1)
                df_actividades_count['R_Actividad_Total'] = df_actividades_count.sum(axis=1)

                # Para solo regresar 1 dataframe de actividades
                df_actividades = pd.concat([df_actividades_sum, df_actividades_count], axis=1)

                # Agrega la columna de viaje a el dataframe (para hacer merge con el dataframe principal)
                df_actividades['Viaje'] = viaje

                '''
                with pd.option_context('display.max_rows', None, 'display.max_columns', 1000):
                    print("-------------o-o-o-o-o-o-o--o-o-o--o-o")
                    print(df_actividades_sum)
                    print(df_actividades_count)
                    print("-------------o-o-o-o-o-o-o--o-o-o--o-o")
                    print(df_actividades)
                '''

            else:

                # Columnas que no vienen directamente en las actividades de firebase
                columnas_viaje_agregacion_actividades_sum += ['R_Duracion_Ajuste', 'R_Duracion_EsperaTrayecto', 'R_Duracion_Total_Viaje']
                columnas_viaje_agregacion_actividades_count += ['R_Actividad_Ajuste', 'R_Actividad_EsperaTrayecto', 'R_Actividad_Total']

                df_actividades = pd.DataFrame(columns=['Viaje'] + columnas_viaje_agregacion_actividades_sum + columnas_viaje_agregacion_actividades_count)


            return df_actividades, df_act_nodos

        except Exception as exc:
            exc_output = sys.exc_info()
            logger.error('{},{}'.format(str(exc), str(exc_output[2].tb_lineno)))
            return False, str(exc)
    
    def quitar_none_list(self, lista):
        try:
            logger.info('quitar_none_list')
            lista_final = list()
            for elemento in lista:
                if elemento is not None:
                    lista_final.append(elemento)
            return True, lista_final
        except Exception as exc:
            exc_output = sys.exc_info()
            logger.error('{},{}'.format(str(exc),str(exc_output[2].tb_lineno)))
            return False, str(exc)
    
    def leer_real(self,viaje_id, info_viajes):
        logger.info('leer_real')
        try:
            # Obtener datos reales del viaje desde firebase
            transacciones_firebase = self.fbm.get(self.fbm.nodo_transacciones(self.cedi_id, viaje_id), {})
            if transacciones_firebase:
                print(transacciones_firebase.get('fecha-llegada'), transacciones_firebase.get('fecha-llegada-calendario'))
                if transacciones_firebase.get('fecha-llegada') or transacciones_firebase.get('fecha-llegada-calendario'):
                    info_viajes.loc[viaje_id, 'StatusAnterior'] = info_viajes.loc[viaje_id, 'Status']
                    info_viajes.loc[viaje_id, 'Status'] = 10
                    info_viajes.loc[viaje_id, 'Expirado'] = 0
                elif info_viajes.loc[viaje_id, 'Expirado'] == 1:
                    info_viajes.loc[viaje_id, 'StatusAnterior'] = info_viajes.loc[viaje_id, 'Status']
                    info_viajes.loc[viaje_id, 'Status'] = 13
                else:
                    #Viaje en tránsito
                    #print('Viaje en tránsito', info_viajes.loc[viaje_id])
                    return pd.Series(), pd.DataFrame()
            # incidentes_firebase = self.fbm.get(self.fbm.nodo_incidentes(self.cedi_id,viaje_id), {}) # Esto se va a registrar desde las CloudFuntions
                # Obtener el nodo correspondiente a los gastos registrados en mobility
                gastos_firebase = self.fbm.get(self.fbm.nodo_gastos(self.cedi_id, viaje_id), {})
                #Obtener toda la info planeada del viaje registrada en firebase
                planeado_firebase = self.fbm.get(self.fbm.nodo_viajes(self.cedi_id, viaje_id), {})
                # obtengo los destinos,facturas y encuestas
                destinos, facturas = self.leer_real_destinos(viaje_id, transacciones_firebase) #, facturas, encuestas

                # Diccionario con los campos con información a nivel de viaje
                real = {
                    'Viaje': viaje_id
                    , 'FechaSalidaReal': self.estandarizar_fecha(transacciones_firebase.get('fecha-salida'))
                    , 'FechaRetornoReal': self.estandarizar_fecha(transacciones_firebase.get('fecha-llegada'))
                    , 'FechaLiquidacion': self.estandarizar_fecha(transacciones_firebase.get('fecha-liquidacion'))
                    , 'FirmaSalida': self.estandarizar_tipo(transacciones_firebase.get('firma-salida'), 'string')
                    , 'FirmaLiquidacion': self.estandarizar_tipo(transacciones_firebase.get('firma-liquidacion'), 'string')
                    , 'OdometroInicial': self.estandarizar_odometro(transacciones_firebase.get('odometro-inicial'))
                    , 'OdometroFinal': self.estandarizar_odometro(transacciones_firebase.get('odometro-final'))
                    , 'LatitudSalidaReal': self.estandarizar_tipo(transacciones_firebase.get('latitud-salida'), 'float')
                    , 'LongitudSalidaReal': self.estandarizar_tipo(transacciones_firebase.get('longitud-salida'), 'float')
                    , 'LatitudRetornoReal': self.estandarizar_tipo(transacciones_firebase.get('latitud-llegada'), 'float')
                    , 'LongitudRetornoReal': self.estandarizar_tipo(transacciones_firebase.get('longitud-llegada'), 'float')
                    , 'ExactitudCoordenadasSalida': self.estandarizar_tipo(
                        transacciones_firebase.get('exactitud-coordenadas-salida'), 'float')
                    , 'DisponibilidadCoordenadasSalida': self.estandarizar_tipo(
                        transacciones_firebase.get('disponibilidad-coordenadas-salida'), 'integer')
                    , 'FechaHoraAutomaticasSalida': self.estandarizar_tipo(
                        transacciones_firebase.get('hora-fecha-automaticas-salida'), 'integer')
                    , 'ExactitudCoordenadasRetorno': self.estandarizar_tipo(
                        transacciones_firebase.get('exactitud-coordenadas-llegada'), 'float')
                    , 'DisponibilidadCoordenadasRetorno': self.estandarizar_tipo(
                        transacciones_firebase.get('disponibilidad-coordenadas-llegada'), 'integer')
                    , 'FechaHoraAutomaticasRetorno': self.estandarizar_tipo(
                        transacciones_firebase.get('hora-fecha-automaticas-llegada'), 'integer')
                    , 'IsMockedSalida': self.estandarizar_tipo(transacciones_firebase.get('ismocked-salida'), 'integer')
                    , 'IsMockedRetorno': self.estandarizar_tipo(transacciones_firebase.get('ismocked-llegada'), 'integer')
                    , 'InfoRedSalida': self.estandarizar_tipo(transacciones_firebase.get('info-red-llegada'), 'string')
                    , 'InfoRedRetorno': self.estandarizar_tipo(transacciones_firebase.get('ismocked-llegada'), 'string')
                    , 'FechaCancelacionViaje': self.estandarizar_fecha(transacciones_firebase.get('fecha-cancelacion-viaje', None))
                # ,'destinos': destinos
                    ,'facturas': facturas
                # ,'encuestas': encuestas
                # ,'incidentes': self.leer_real_incidentes(viaje_id,incidentes_firebase)
                # , 'actividades': self.leer_real_actividades(viaje_id, transacciones_firebase.get('actividades', []))
                }

                # Añadir otros datos a nivel de viaje
                colat = self.estandarizar_tipo(planeado_firebase.get('cedi-origen-latitud'), 'float')
                colng = self.estandarizar_tipo(planeado_firebase.get('cedi-origen-longitud'), 'float')
                cdlat = self.estandarizar_tipo(planeado_firebase.get('cedi-destino-latitud'), 'float')
                cdlng = self.estandarizar_tipo(planeado_firebase.get('cedi-destino-longitud'), 'float')
                info_viaje = transacciones_firebase.get('info-viaje')
                if info_viaje:
                    real['ProveedorFlete'] = info_viaje.get('fletera')
                    real['OperadorReal'] = info_viaje.get('operador-nombre')
                    real['OperadorCorreo'] = info_viaje.get('operador-correo')
                    real['R_Vehiculo_Placas'] = info_viaje.get('placas')
                    real['R_Numero_Economico'] = info_viaje.get('numero-economico')
                else:
                    real['ProveedorFlete'] = None
                    real['OperadorReal'] = None
                    real['OperadorCorreo'] = None
                    real['R_Vehiculo_Placas'] = None
                    real['R_Numero_Economico'] = None
                    real['KmReal'] = None
                kmf = transacciones_firebase.get('odometro-final')
                kmi = transacciones_firebase.get('odometro-inicial')
                if kmi and kmf:
                    real['KmReal'] = self.estandarizar_odometro(kmf) - self.estandarizar_odometro(kmi)
                else:
                    real['KmReal'] = None

                if real.get('LongitudSalidaReal'):
                    real['FueraDeGeocercaSalida'] = haversine((colat, colng), (
                    real.get('LatitudSalidaReal'), real.get('LongitudSalidaReal')), Unit.KILOMETERS)
                else:
                    real['FueraDeGeocercaSalida'] = None
                if real.get('LongitudRetornoReal'):
                    real['FueraDeGeocercaLlegada'] = haversine((cdlat, cdlng), (
                    real.get('LatitudRetornoReal'), real.get('LongitudRetornoReal')), Unit.KILOMETERS)
                else:
                    real['FueraDeGeocercaLlegada'] = None
            #El resto de la info a nivel de viaje está en el nodo de gastos
                real = self.leer_real_gastos(viaje_id, gastos_firebase, real)
            #Cada viaje se regresa en una serie
                viaje_df = pd.Series(real)
                viaje_df['StatusAnterior'] = info_viajes.loc[viaje_id, 'StatusAnterior']
                viaje_df['Status'] = info_viajes.loc[viaje_id, 'Status']
                if not destinos.empty:
                    info_viaje = viaje_df[['Viaje', 'FechaSalidaReal', 'FechaRetornoReal', 'ProveedorFlete', 'OperadorReal', 'R_Vehiculo_Placas',
                                       'R_Numero_Economico']]
                    temp = pd.DataFrame([info_viaje], columns=info_viaje.index)
                    dest_viaje = destinos.merge(temp, how='left', left_on='Viaje', right_on='Viaje')
                    dest_viaje.loc[:,'FechaCancelaciondestino'] = viaje_df['FechaCancelacionViaje']
                    dest_viaje.loc[destinos['FechaEntregaDestinoReal'].isna(), 'FechaCancelacionDestino'] = viaje_df['FechaCancelacionViaje']
                    viaje_df['R_Tiradas_Efectuadas'] = len(destinos[~destinos['FechaEntregaDestinoReal'].isnull()])
                    viaje_df['PofTiempo'] = destinos.loc[:,'PofTiempo'].sum() / viaje_df['R_Tiradas_Efectuadas']
                    viaje_df['PofCompleto'] = destinos.loc[:,'PofCompleto'].sum() / viaje_df['R_Tiradas_Efectuadas']
                    viaje_df['PofSD'] = destinos.loc[:,'PofSD'].sum() / viaje_df['R_Tiradas_Efectuadas']
                    viaje_df['PofDocs'] = destinos.loc[:,'PofDocs'].sum() / viaje_df['R_Tiradas_Efectuadas']
                    viaje_df['Pof'] = destinos.loc[:,'Pof'].sum() / viaje_df['R_Tiradas_Efectuadas']
                else:
                    dest_viaje = destinos
                return viaje_df, dest_viaje
            else:
                #print('Viaje desconocido', info_viajes.loc[viaje_id])
                if info_viajes.loc[viaje_id,'Expirado']==1:
                    info_viajes.loc[viaje_id, 'Status'] = 13
                return pd.Series(), pd.DataFrame()

        except Exception as exc:
            exc_output = sys.exc_info()
            import traceback
            print(traceback.print_exc())
            logger.error('{},{}'.format(str(exc),str(exc_output[2].tb_lineno)))
            raise Exception(str(exc))

    def leer_real_destinos(self,viaje_id,transacciones_firebase):
        logger.info('leer_real_destinos')
        try:
            destinos_planeados = self.fbm.get(self.fbm.nodo_destinos_plan(self.cedi_id, viaje_id), {})
            destinos = []
            facturas = []
            #encuestas = []
            
            for i,destino in enumerate(transacciones_firebase.get('destinos',[])):
                if isinstance(destino, str):    # convierto de indice a dict
                    if destino.isdigit():
                        i = int(destino)
                        destino = transacciones_firebase['destinos'][destino]
                    else:
                        destino = None
                
                if isinstance(destino,dict) and i > 0:   # tengo un dict
                    json_destino = {
                        'Viaje': int(viaje_id)
                        ,'TiradaPlan': i
                        ,'FechaEntregaDestinoReal': self.estandarizar_fecha(destino.get('fecha-llegada'))
                        ,'FechaSalidaDestinoReal': self.estandarizar_fecha(destino.get('fecha-salida'))
                        ,'FirmaEntrega': self.estandarizar_tipo(destino.get('firma-salida'),'string')
                        ,'FotoEntrega': self.estandarizar_foto(destino.get('foto-entrega'))
                        ,'LatitudEntregaReal': self.estandarizar_tipo(destino.get('latitud-llegada'),'float')
                        ,'LongitudEntregaReal': self.estandarizar_tipo(destino.get('longitud-llegada'),'float')
                        ,'FechaLlegadaFirebase': self.estandarizar_tipo(destino.get('fecha-llegada-firebase'),'integer')
                        ,'FechaLlegadaAmazon': self.estandarizar_fecha(destino.get('fecha-llegada-Amazon'))
                        ,'TipoRedLlegada': self.estandarizar_tipo(destino.get('tipo-red-llegada'),'string')
                        ,'InfoRedLlegada': self.estandarizar_tipo(destino.get('info-red-llegada'),'string')
                        ,'ExactitudCoordenadasLlegada': self.estandarizar_tipo(destino.get('exactitud-coordenadas-llegada'),'float')
                        ,'DisponibilidadCoordenadasLlegada': self.estandarizar_tipo(destino.get('disponibilidad-coordenadas-llegada'),'integer')
                        ,'FechaHoraAutomaticasLlegada': self.estandarizar_tipo(destino.get('hora-fecha-automaticas-llegada'),'integer')
                        ,'LatitudSalidaReal': self.estandarizar_tipo(destino.get('latitud-salida'),'float')
                        ,'LongitudSalidaReal': self.estandarizar_tipo(destino.get('longitud-salida'),'float')
                        ,'FechaSalidaFirebase': self.estandarizar_tipo(destino.get('fecha-salida-firebase'),'integer')
                        ,'FechaSalidaAmazon': self.estandarizar_fecha(destino.get('fecha-salida-Amazon'))
                        ,'TipoRedSalida': self.estandarizar_tipo(destino.get('tipo-red-salida'),'string')
                        ,'InfoRedSalida': self.estandarizar_tipo(destino.get('info-red-salida'),'string')
                        ,'ExactitudCoordenadasSalida': self.estandarizar_tipo(destino.get('exactitud-coordenadas-salida'),'float')
                        ,'DisponibilidadCoordenadasSalida': self.estandarizar_tipo(destino.get('disponibilidad-coordenadas-salida'),'integer')
                        ,'FechaHoraAutomaticasSalida': self.estandarizar_tipo(destino.get('hora-fecha-automaticas-salida'),'integer')
                        ,'PofCompleto': self.evaluar_pof(self.ignore_pof_completo,destino,'entrega')
                        ,'PofSD': self.evaluar_pof(self.ignore_pof_danios,destino,'sin-danos')
                        ,'PofDocs': self.evaluar_pof(self.ignore_pof_doctos,destino,'docum-completa')
                        ,'IsMockedEntrega': self.estandarizar_tipo(destino.get('ismocked-llegada'),'integer')
                        ,'IsMockedSalida': self.estandarizar_tipo(destino.get('ismocked-salida'),'integer')
                    }
                    facturas += self.leer_real_facturas(viaje_id, destino, i)
                    planeado_destino = destinos_planeados[i]
                    if planeado_destino:
                        dest_latitud = planeado_destino.get('latitud')
                        dest_longitud = planeado_destino.get('longitud')
                        if json_destino.get('LongitudSalidaReal'):
                            json_destino['FueraDeGeocercaSalida'] = haversine((dest_latitud, dest_longitud), (
                                json_destino.get('LatitudSalidaReal'), json_destino.get('LongitudSalidaReal')), Unit.KILOMETERS)
                        else:
                            json_destino['FueraDeGeocercaSalida'] = None
                        if json_destino.get('LongitudRetornoReal'):
                            json_destino['FueraDeGeocercaLlegada'] = haversine((dest_latitud, dest_longitud), (
                                json_destino.get('LatitudRetornoReal'), json_destino.get('LongitudRetornoReal')), Unit.KILOMETERS)
                        else:
                            json_destino['FueraDeGeocercaLlegada'] = None

                        json_destino['PofTiempo'] = self.pof_tiempo(json_destino.get('FechaEntregaDestinoReal'), planeado_destino.get('fecha-entrega'))
                    destinos.append(json_destino)
                    #facturas += self.leer_real_facturas(viaje_id,destino,i)
                    #encuestas += self.leer_real_encuestas(viaje_id,destino,i)
            res = pd.DataFrame(destinos)
            if destinos:
                res['FechaEntregaDestinoReal'] = pd.to_datetime(res['FechaEntregaDestinoReal'], format="%Y-%m-%d %H:%M:%S")
                res.sort_values('FechaEntregaDestinoReal', inplace=True)
                res.reset_index(drop=True, inplace=True)
                res['TiradaReal'] = res.index.values+1
            return res, facturas#,encuestas
        
        except Exception as exc:
            import traceback
            print(traceback.print_exc())
            exc_output = sys.exc_info()
            logger.error('{},{}'.format(str(exc),str(exc_output[2].tb_lineno)))
            raise Exception(str(exc))

    def pof_tiempo(self, freal, fplan):
        fecha_real = pd.to_datetime(freal)
        fecha_plan = pd.to_datetime(fplan)
        if fecha_real and fecha_plan:
            if fecha_real <= fecha_plan:
                return 1
            else:
                return 0
        else:
            return 0

    def estandarizar_fecha(self,fecha):
        try:
            if fecha:

                if fecha not in ['API call failed', 'Response null']:
                        
                    if len(fecha) > 19:
                        fecha_utc = dt.strptime(fecha[:19],"%Y-%m-%d %H:%M:%S")
                        desfase = fecha[20:23]
                        fecha_utc = fecha_utc + td(hours=int(desfase) * -1)
                        fecha_utc = tz('UTC').localize(fecha_utc)
                        fecha_cdmx = fecha_utc.astimezone(tz('America/Mexico_City'))
                    else:
                        fecha_cdmx = dt.strptime(fecha,"%Y-%m-%d %H:%M:%S")
                        fecha_cdmx = tz('America/Mexico_City').localize(fecha_cdmx)
                        
                    return str(fecha_cdmx)[:19]
                else:
                    return None
            else:
                return None

        except:
            import traceback
            print(traceback.print_exc())
            return None

    def estandarizar_tipo(self,dato,tipo):
        try:
            if not dato is None:
                if tipo == 'integer':
                    dato = int(dato)
                
                elif tipo == 'string':
                    dato = str(dato)
                
                elif tipo == 'float':
                    dato = float(dato)
                
                elif tipo == 'boolean':
                    dato = bool(dato)
                    
                    if dato:
                        dato = 1
                    
                    else:
                        dato = 0
                
                return dato
            
            else:
                return None
        
        except Exception:
            return None

    def estandarizar_foto(self,key):
        try:
            if isinstance(key,unicode):
                
                if key == 'Sin foto':
                    return None
                
                else:
                    return key.split('/')[-1]
            else:
                None
        except:
            return None

    def evaluar_pof(self,ignore,destino,key):
        if ignore:
            pof = 1
        else:
            pof = self.estandarizar_tipo(destino.get('pof',{}).get(key,False),'boolean')
        return pof

    def leer_real_facturas(self,viaje_id,destino,tirada_id):
        logger.info('leer_real_facturas')
        facturas = []
        for factura in destino.get('facturas',[]):
            json_factura = {
                'Viaje': viaje_id,
                'Factura':self.estandarizar_tipo(factura,'string'),
                'TiradaPlan': tirada_id,
                'entregado':self.estandarizar_tipo(destino['facturas'][factura].get('entregada',False),'boolean'),
                'FotoFactura':self.estandarizar_foto(destino['facturas'][factura].get('fotoRemision'))
                }
            facturas.append(json_factura)
        return facturas
    
    def leer_real_encuestas(self,viaje_id,destino,tirada_id):
        logger.info('leer_real_encuestas')
        try:
            encuestas = []
            for encuesta in destino.get('encuestas',[]):
                for j,respuesta in enumerate(self.remove_none_elements_from_list(destino['encuestas'][encuesta])):
                    if 'respuesta' in respuesta:     # una unica respuesta
                        json_respuesta ={
                            'Encuesta': encuesta
                            ,'Tirada': tirada_id
                            ,'idPregunta': j
                            ,'idRespuesta': 1
                            ,'Respuesta': self.estandarizar_tipo(respuesta['respuesta'].encode('utf-8'),'string')
                            ,'Metrica': self.estandarizar_tipo(respuesta['respuesta'].encode('utf-8'),'string')
                            ,'FechaRespuesta': self.estandarizar_fecha(respuesta.get('fecha'))
                        }
                        encuestas.append(json_respuesta)
                    
                    else:                           # lista de respuestas
                        for k,respuesta in enumerate(self.remove_none_elements_from_list(respuesta['respuestas'])):
                            json_respuesta = {
                                'Encuesta': encuesta
                                ,'Tirada': tirada_id
                                ,'idPregunta': j
                                ,'idRespuesta': k
                                ,'Metrica': self.estandarizar_tipo(respuesta.encode('utf-8'),'string')
                                ,'FechaRespuesta': self.estandarizar_fecha(respuesta.get('fecha'))
                                }
                            encuestas.append(json_respuesta)
            return encuestas
        
        except Exception as exc:
            exc_output = sys.exc_info()
            logger.error('{},{}'.format(str(exc),str(exc_output[2].tb_lineno)))
            return []

    def estandarizar_odometro(self,odometro):
        try:
            return int(odometro.replace(',',''))
        except:
            return None

    def leer_real_incidentes(self,viaje_id,incidentes_firebase):
        logger.info('leer_real_incidentes')    
        try:
            incidentes = []
            
            for incidente in self.ifnull(incidentes_firebase,{}):
                json_incidente = {
                    'Viaje':viaje_id
                    ,'etapa':self.estandarizar_incidente_parada(incidentes_firebase[incidente])
                    ,'fecha':self.estandarizar_fecha(incidentes_firebase[incidente].get('fecha'))
                    ,'latitud':self.estandarizar_tipo(incidentes_firebase[incidente].get('latitud'),'float')
                    ,'longitud':self.estandarizar_tipo(incidentes_firebase[incidente].get('longitud'),'float')
                    ,'tipo':self.estandarizar_tipo(incidentes_firebase[incidente].get('tipo'),'string')
                    ,'comentario':self.estandarizar_tipo(incidentes_firebase[incidente].get('comentario'),'string')
                    ,'foto':self.estandarizar_foto(incidentes_firebase[incidente].get('foto'))
                    }
                incidentes.append(json_incidente)
            
            return incidentes
        
        except Exception as exc:
            exc_output = sys.exc_info()
            logger.error('{},{}'.format(str(exc),str(exc_output[2].tb_lineno)))
            raise Exception(str(exc))
    
    def leer_real_actividades(self,viaje_id,actividades_firebase):
        logger.info('leer_real_actividades')        
        actividades = []
        for actividad in actividades_firebase:
            json_actividad = {
                'IdEmpresa': self.empresa_id
                ,'Viaje': viaje_id
                ,'TiradaReal': self.estandarizar_tipo(actividades_firebase[actividad].get('tirada-actividad'),'integer')
                ,'Actividad': self.estandarizar_tipo(actividades_firebase[actividad].get('tipo-actividad'),'string')
                ,'FechaActividad': self.estandarizar_fecha(actividades_firebase[actividad].get('fecha-actividad'))
                ,'HorasActividad': self.estandarizar_tipo(actividades_firebase[actividad].get('duracion-actividad'),'integer')
                ,'Latitud': self.estandarizar_tipo(actividades_firebase[actividad].get('latitud-actividad'),'float')
                ,'Longitud': self.estandarizar_tipo(actividades_firebase[actividad].get('longitud-actividad'),'float')
                ,'InfoRed': self.estandarizar_tipo(actividades_firebase[actividad].get('info-red-salida'),'string')
                ,'ExactitudCoordenadas': self.estandarizar_tipo(actividades_firebase[actividad].get('exactitud-coordenadas-actividad'),'float')
                ,'DisponibilidadCoordenadas': self.estandarizar_tipo(actividades_firebase[actividad].get('disponibilidad-coordenadas-actividad')
                ,'integer')
                ,'FechaHoraAutomaticas': self.estandarizar_tipo(actividades_firebase[actividad].get('hora-fecha-automaticas-actividad'),'integer')
                ,'FechaActividadFirebase': self.estandarizar_tipo(actividades_firebase[actividad].get('fecha-actividad-firebase'),'integer')
            }
            actividades.append(json_actividad)
        return actividades

    def leer_real_gastos(self, viaje_id, gastos_firebase, viaje):
        logger.info('leer_real_gastos')
        gastos_firebase = self.ifnull(gastos_firebase, {})
        gastos = {
            'R_Costo_Flete': ['flete-efectivo', 'flete-electronico']
            , 'R_Costo_Ferries': ['ferries-efectivo', 'ferries-electronico']
            , 'R_Costo_Pernocta': ['hoteles-efectivo', 'hoteles-electronico']
            , 'R_Costo_Otros_Gastos': ['otros-efectivo', 'otros-electronico']
            , 'R_Costo_Alimentos': ['comida-efectivo', 'comida-electronico']
            , 'R_Costo_ThermoReal': ['thermo-efectivo', 'thermo-electronico']
            , 'R_Costo_Casetas': ['casetas-efectivo', 'casetas-electronico']
            , 'R_Costo_Combustible': ['combustible-efectivo', 'combustible-electronico']
        }

        viaje['R_Costo_Total_Viaje'] = 0
        for key, val in gastos.items():
            efec = gastos_firebase.get(val[0])
            elec = gastos_firebase.get(val[1])
            if efec and elec:
                gasto = self.estandarizar_tipo(gastos_firebase.get(val[0]), 'float') + \
                        self.estandarizar_tipo(gastos_firebase.get(val[1]), 'float')
                viaje[key] = gasto
                viaje['R_Costo_Total_Viaje'] += gasto
            elif efec:
                gasto = self.estandarizar_tipo(gastos_firebase.get(val[0]), 'float')
                viaje[key] = gasto
                viaje['R_Costo_Total_Viaje'] += gasto
            elif elec:
                gasto = self.estandarizar_tipo(gastos_firebase.get(val[1]), 'float')
                viaje[key] = gasto
                viaje['R_Costo_Total_Viaje'] += gasto
            else:
                viaje[key] = None

        return viaje 
    
    def remove_none_elements_from_list(self,my_list):
        return [e for e in my_list if e != None]
    
    def ifnull(self,value,default=None):
        if value:
            try:
                is_nan = np.isnan(value)
            except:
                is_nan = False
                
            if not is_nan:
                return value
            else:
                return default
        else:
            return default
    
    def estandarizar_incidente_parada(self, incidente):
        try:
            parada = 1
            if 'tiradaDestino' in incidente:
                parada = int(incidente['tiradaDestino'])
            elif 'etapa' in incidente:
                parada = int(incidente['etapa'].replace('destino ', ''))
            else:
                parada = 1
            
            return parada
        
        except Exception:
            return 1

    def validate_responses(self, status, result):
        if not status:
            raise Exception(result)

    def pedidos_pof(self, df_destinos, df_pedidos):
        logger.info('pedidos_pof')

        try:
            df_pof = pd.merge(
                df_pedidos[['TiradaPlan', 'Pedido', 'entregado']]
                , df_destinos[['TiradaPlan', 'PofTiempo', 'PofCompleto', 'PofSD', 'PofDocs']]
                , how='inner'
                , on=['TiradaPlan'])
            df_pof['PofCompleto'] = df_pof['PofCompleto'].apply(lambda a: 0 if np.isnan(a) else a)
            df_pof['PofSD'] = df_pof['PofSD'].apply(lambda a: 0 if np.isnan(a) else a)
            df_pof['PofDocs'] = df_pof['PofDocs'].apply(lambda a: 0 if np.isnan(a) else a)

            if not self.ignore_pof_completo:
                df_pof['PofCompleto'] = df_pof.apply(
                    lambda row: row['PofCompleto'] if pd.isnull(row['entregado']) else row['entregado'], axis=1)

            df_pof['Pof'] = df_pof.PofTiempo + df_pof.PofCompleto + df_pof.PofSD + df_pof.PofDocs
            df_pof['Pof'] = df_pof.Pof.map(lambda a: 1 if a == 4 else 0)

            return df_pof

        except Exception as exc:
            exc_output = sys.exc_info()
            logger.error('{},{}'.format(str(exc), str(exc_output[2].tb_lineno)))
            raise Exception(exc)

    def normalizar_real_pedidos(self, planeado, real, df_destinos):
        logger.info('normalizar_real_pedidos')

        try:
            df_pedidos = pd.DataFrame()

            if df_destinos.shape[0] > 0:  # solo proceso si hay entregas

                # elementos fundamentales
                df_pedidos = planeado[[
                    'Viaje', 'TiradaPlan', 'Pedido', 'Piezas'
                    , 'Volumen', 'Peso', 'Factura', 'Valor']]
                df_pedidos.columns = [
                    'Viaje', 'TiradaPlan', 'Pedido', 'PiezasEntregadas'
                    , 'VolumenEntregado', 'PesoEntregado', 'Factura', 'ValorEntregado'
                ]

                # entregas
                if len(real.loc[0, 'facturas']) > 0:
                    df_pedidos = pd.merge(df_pedidos,
                                          pd.DataFrame(real.loc[0, 'facturas'])[[
                                              'Viaje', 'TiradaPlan', 'Factura', 'entregado'
                                              , 'FotoFactura'
                                          ]], how='left', on=['Viaje', 'TiradaPlan'])
                else:
                    df_pedidos['entregado'] = 0
                    df_pedidos['FotoFactura'] = None

                df_pedidos.entregado = df_pedidos.entregado.map(
                    lambda a: 0 if np.math.isnan(float(a)) else a)

                # fecha entrega real del pedido
                df_pedidos = pd.merge(df_pedidos,
                                      df_destinos[[
                                          'Viaje', 'TiradaPlan', 'FechaEntregaDestinoReal',
                                          'FechaSalidaDestinoReal'
                                      ]], how='left', on=['Viaje', 'TiradaPlan'])
                df_pedidos.rename(columns={
                    'FechaEntregaDestinoReal': 'FechaEntregaPedidoReal',
                    'FechaSalidaDestinoReal': 'FechaSalidaPedidoReal'
                }, inplace=True)
                df_pedidos.PiezasEntregadas *= df_pedidos.entregado
                df_pedidos.VolumenEntregado *= df_pedidos.entregado
                df_pedidos.PesoEntregado *= df_pedidos.entregado
                df_pedidos.ValorEntregado *= df_pedidos.entregado

                # pof
                df_pedidos = pd.merge(
                    df_pedidos,
                    self.pedidos_pof(df_destinos, df_pedidos),
                    how='inner',
                    on=['TiradaPlan', 'Pedido', 'entregado'])

            return df_pedidos

        except Exception as exc:
            exc_output = sys.exc_info()
            logger.error('{},{}'.format(str(exc), str(exc_output[2].tb_lineno)))
            raise Exception(exc)