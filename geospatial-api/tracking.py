# -*- coding: utf-8 -*-

# Eduardo martes, 10 de septiembre de 2019
# Para obtener la informacion necesaria para el tracking

from logging import getLogger, basicConfig
from database_services.firebasemethods import FirebaseMethods
from database_services.mysql_service import MySQLConnection


import json, sys
import configparser
import pandas as pd
import numpy as np
import config

logger = getLogger()


class Tracking:
    def __init__(self):
        self.fbm = FirebaseMethods('config.ini')
        self.my_sql_conn = MySQLConnection('config.ini')

        self.type_vehicle = list()
        self.state_truck = list()
        self.drivers = list()
        self.trips = list()
        self.regions = list()
        self.states = list()
        self.type_packages = list()
        self.truck_size = list()
        self.ware_house_size = list()

    def login(self, correo, password):
        """Para hacer login"""
        try:
            logger.info('login')
            args = [correo]
            status = True

            res  = self.my_sql_conn.spexec('sp_api_tracking_obtener_cedis', args)
          
            if res[0][0] != 'OK':
                raise Exception(res[0][1])
            
            result = dict()
            result["cedis"] = json.loads(res[0][1])
            result["company"] = res[0][2]
            result["user"] = res[0][3]

            if not password == "arms2019":
                status = False
                result = "You're company is not exist"
            
            return status, result
            
        except Exception as exc:
            logger.info('Error en el login')
            logger.error(exc)
            return False, str(exc)
    
    def obtener_viajes(self, cedis):
        try:
            logger.info('obtener_viajes')
            viajes_res = dict()
            cedis_string = str(cedis)
            cedis_string = cedis_string.replace(" ", "")
            cedis_string = cedis_string.replace("[", "")
            cedis_string = cedis_string.replace("]", "")
            args = [cedis_string]

            res  = self.my_sql_conn.spexec('sp_api_tracking_obtener_viajes_transito', args)
            
            if res[0][0] != 'OK':
                raise Exception(res[0][1])

            if res[0][1]:
                for cedi in json.loads(res[0][1]):
                    id_cedi = cedi['idCedi']
                    viajes = json.loads(cedi['idViajes'])
                    viajes_res = self.obtener_datos_viaje_fb(id_cedi, viajes)


            return True, viajes_res
            
        except Exception as exc:
            logger.info('Error en obtener viajes')
            logger.error(exc)
            return False, str(exc)
    
    def obtener_datos_viaje_fb(self, cedi, viajes):
        res_final = dict()
        viajes_completo = list()

        for viaje in viajes:
            viaje_real = dict()
            viaje_planeado = dict()

            logger.info('OBTENIENDO DATOS DE FIREBASE')
            viajes_firebase = self.fbm.get(self.fbm.nodo_viajes(cedi, viaje), {})
            transacciones_firebase = self.fbm.get(self.fbm.nodo_transacciones(cedi, viaje), {})
            tracking_firebase = self.fbm.get(self.fbm.nodo_tracking_viajes(cedi, viaje), {})
            info_viaje_firebase = self.fbm.get(self.fbm.nodo_info_viaje(cedi, viaje), {})
            nombre_operador = info_viaje_firebase["operador-nombre"]
            operador_datos_firebase = self.fbm.get(self.fbm.nodo_operador_datos(cedi, nombre_operador), {})
            actividades_firebase = self.fbm.get(self.fbm.nodo_actividades(cedi, viaje), {})

            logger.info('PASANDO A DATAFRAME')
            dfTracking = pd.DataFrame(tracking_firebase)
            dfTracking = dfTracking.T
            dfTracking['fecha'] = pd.to_datetime(dfTracking['fecha'])
            dfTracking = dfTracking.sort_values(by='fecha', ascending=False)
            
            dfActividades = pd.DataFrame(actividades_firebase)
            dfActividades = dfActividades.T

            latitud_mobility = dfTracking.iloc[0].loc['latitud']
            longitud_mobility = dfTracking.iloc[0].loc['longitud']

            ranking_operador = None
            if operador_datos_firebase:
                logger.info('OBTENIENDO RANKING DDEL OPERADOR SI EXISTE')
                ranking_operador = operador_datos_firebase["ranking"]
            
            logger.info('OBTENIENDO LOS CONTACTOS')
            contactos = str(viajes_firebase["contactos"])
            contactos = contactos.replace('None, ', '').replace('[', '').replace(']', '').replace('{', '').replace('}', '').replace("'", '')

            logger.info('OBTENIENDO INFORMACION DE LOS DESTINOS')
            destinos_reales, destinos_planeados = self.obtener_datos_destino_fb(cedi, viaje, viajes_firebase, dfActividades)
            
            # ------------------- VIAJES REALES ------------------- #
            logger.info('ARMANDO JSON VIAJES REALES')
            viaje_real["IdViaje"] = viajes_firebase["id-viaje"]
            viaje_real["Planeado"] = 0
            viaje_real["SizeTruckShow"] = "L" # NO EN FIREBASE
            viaje_real["LatitudMobility"] = latitud_mobility
            viaje_real["LongitudMobility"] = longitud_mobility
            viaje_real["LastDateMobility"] = "2/7/2019 14:34" # NO EN FIREBASE
            viaje_real["FechaHoraAutomaticasActivity"] = "2/7/2019 14:34" # NO EN FIREBASE
            viaje_real["ExactitudCoordenadasActivity"] = "25.59000015" # NO EN FIREBASE
            viaje_real["DisponibilidadCoordenadasActivity"] = "1" # NO EN FIREBASE
            viaje_real["IsMockedActivity"] = "1" # NO EN FIREBASE
            viaje_real["WarehouseLeave"] = "Silodisa" # NO EN FIREBASE
            viaje_real["LatitudWarehouseLeave" ]= transacciones_firebase["latitud-salida"]
            viaje_real["LongitudWarehouseLeave"] = transacciones_firebase["longitud-salida"]
            viaje_real["WarehouseReturn"] =  "Silodisa" # NO EN FIREBASE
            viaje_real["LatitudWarehouseReturn"] = viajes_firebase["cedi-destino-latitud"]
            viaje_real["LongitudWarehouseReturn"] = viajes_firebase["cedi-destino-longitud"]
            viaje_real["WarehouseSize"] =  "L" # NO EN FIREBASE
            viaje_real["TypeVehicle"] =  "FL360" # NO EN FIREBASE
            viaje_real["StateTruck"] =  "Trayecto" # NO EN FIREBASE
            viaje_real["Km-h"] =  "34.5" # NO EN FIREBASE
            viaje_real["Placas"] =  info_viaje_firebase["placas"]
            viaje_real["NumberTruck"] =  info_viaje_firebase["numero-economico"]
            viaje_real["MaxWeight"] =  "4950.00" # NO EN FIREBASE
            viaje_real["MaxVolume"] =  "15.88" # NO EN FIREBASE
            viaje_real["MaxCostMerchadise"] =  "18000000.00" # NO EN FIREBASE
            viaje_real["Km"] =  viajes_firebase["km"]
            viaje_real["Cost"] =  "1234" # NO EN FIREBASE
            viaje_real["PorcentajeVolumen"] =  "23" # NO EN FIREBASE
            viaje_real["PorcentajeWeight"] =  "23" # NO EN FIREBASE
            viaje_real["Driver"] =  nombre_operador
            viaje_real["RankingDriver"] =  ranking_operador
            viaje_real["Contact"] =  contactos
            viaje_real["ActualWeight"] =  "2,022.16" # NO EN FIREBASE
            viaje_real["ActualVolumen"] =  "10.50" # NO EN FIREBASE
            viaje_real["ActualCostMerchandise"] =  "2081234.72" # NO EN FIREBASE
            viaje_real["NumberDestinations"] =  "2" # NO EN FIREBASE
            viaje_real["NumberPackages"] =  "21" # NO EN FIREBASE
            # viaje_real["TypePackages"] =  "Ordinary" # NO EN FIREBASE
            viaje_real["NumberPieces"] =  "28022" # NO EN FIREBASE
            viaje_real["NextDestination"] =  "UMU 1234" # NO EN FIREBASE
            viaje_real["NextDestinationArrival"] =  "2/7/2019 14:34:00" # NO EN FIREBASE
            viaje_real["KmRecorridos"] =  "123" # NO EN FIREBASE
            viaje_real["KmPorRecorrer"] =  "123" # NO EN FIREBASE
            viaje_real["ToDeliverNumberDestinations"] =  "1" # NO EN FIREBASE
            viaje_real["ToDeliverNumberPackages"] =  "11" # NO EN FIREBASE
            viaje_real["EstimatedFinishRoute"] = viajes_firebase["fecha-retorno"]
            viaje_real["MailMessages"] = "aaa@qwer.com; CCCC@abc.com" # NO EN FIREBASE
            viaje_real["WahtsAppMessages"] = "+555234567689; +525599887766" # NO EN FIREBASE
            viaje_real["SendMailIn10K"] = "1" # NO EN FIREBASE
            viaje_real["SendWahtsAppIn10K"] = "1" # NO EN FIREBASE
            viaje_real["FechaSalida"] = transacciones_firebase["fecha-salida"]
            viaje_real["FechaRetorno"] = viajes_firebase["fecha-retorno"]

            logger.info('DESTINOS REALES OBTENIDOS')
            viaje_real["Destinations"] = destinos_reales
            
            itinerario = self.obtener_datos_itinerario_fb(cedi, viaje, dfActividades)
            logger.info('ITINERARIO OBTENIDO')
            viaje_real["Itinerary"] = itinerario

            viaje_real["FotoRemisiones"] = list()
            viaje_real["FotoEntrega"] = list()

            logger.info('CATALOGO PARA FILTRO CON REAL')
            self.type_vehicle.append(viaje_real["TypeVehicle"])
            self.state_truck.append(viaje_real["StateTruck"])
            self.drivers.append(viaje_real["Driver"])
            self.trips.append(viaje_real["IdViaje"])
            # self.type_packages.append(viaje_real["TypePackages"])
            self.truck_size.append(viaje_real["SizeTruckShow"])
            self.ware_house_size.append(viaje_real["WarehouseSize"])
            
            logger.info('AGREGANDO VIAJE REAL A RESULTADO FINAL')
            viajes_completo.append(viaje_real)

            # ------------------- VIAJES PLANEADOS ------------------- #
            logger.info('ARMANDO JSON VIAJES PLANEADOS')
            viaje_planeado["IdViaje"] = viajes_firebase["id-viaje"]
            viaje_planeado["Planeado"] = 1
            viaje_planeado["SizeTruckShow"] = "L" # NO EN FIREBASE
            viaje_planeado["LatitudMobility"] = None
            viaje_planeado["LongitudMobility"] = None
            viaje_planeado["LastDateMobility"] = "2/7/2019 14:34" # NO EN FIREBASE
            viaje_planeado["FechaHoraAutomaticasActivity"] = "2/7/2019 14:34" # NO EN FIREBASE
            viaje_planeado["ExactitudCoordenadasActivity"] = "25.59000015" # NO EN FIREBASE
            viaje_planeado["DisponibilidadCoordenadasActivity"] = "1" # NO EN FIREBASE
            viaje_planeado["IsMockedActivity"] = "1" # NO EN FIREBASE
            viaje_planeado["WarehouseLeave"] = "Silodisa" # NO EN FIREBASE
            viaje_planeado["LatitudWarehouseLeave" ]= transacciones_firebase["latitud-salida"]
            viaje_planeado["LongitudWarehouseLeave"] = transacciones_firebase["longitud-salida"]
            viaje_planeado["WarehouseReturn"] =  "Silodisa" # NO EN FIREBASE
            viaje_planeado["LatitudWarehouseReturn"] = viajes_firebase["cedi-destino-latitud"]
            viaje_planeado["LongitudWarehouseReturn"] = viajes_firebase["cedi-destino-longitud"]
            viaje_planeado["WarehouseSize"] =  "L" # NO EN FIREBASE
            viaje_planeado["TypeVehicle"] =  "FL360" # NO EN FIREBASE
            viaje_planeado["StateTruck"] =  None
            viaje_planeado["Km-h"] =  "34.5" # NO EN FIREBASE
            viaje_planeado["Placas"] =  None
            viaje_planeado["NumberTruck"] =  info_viaje_firebase["numero-economico"]
            viaje_planeado["MaxWeight"] =  "4950.00" # NO EN FIREBASE
            viaje_planeado["MaxVolume"] =  "15.88" # NO EN FIREBASE
            viaje_planeado["MaxCostMerchadise"] =  "18000000.00" # NO EN FIREBASE
            viaje_planeado["Km"] =  viajes_firebase["km"]
            viaje_planeado["Cost"] =  "1234" # NO EN FIREBASE
            viaje_planeado["PorcentajeVolumen"] =  "23" # NO EN FIREBASE
            viaje_planeado["PorcentajeWeight"] =  "23" # NO EN FIREBASE
            viaje_planeado["Driver"] =  None
            viaje_planeado["RankingDriver"] =  ranking_operador
            viaje_planeado["Contact"] =  contactos
            viaje_planeado["ActualWeight"] =  "2,022.16" # NO EN FIREBASE
            viaje_planeado["ActualVolumen"] =  "10.50" # NO EN FIREBASE
            viaje_planeado["ActualCostMerchandise"] =  "2081234.72" # NO EN FIREBASE
            viaje_planeado["NumberDestinations"] =  "2" # NO EN FIREBASE
            viaje_planeado["NumberPackages"] =  "21" # NO EN FIREBASE
            # viaje_planeado["TypePackages"] =  "Ordinary" # NO EN FIREBASE
            viaje_planeado["NumberPieces"] =  "28022" # NO EN FIREBASE
            viaje_planeado["NextDestination"] =  "UMU 1234" # NO EN FIREBASE
            viaje_planeado["NextDestinationArrival"] =  "2/7/2019 14:34:00" # NO EN FIREBASE
            viaje_planeado["KmRecorridos"] =  "123" # NO EN FIREBASE
            viaje_planeado["KmPorRecorrer"] =  "123" # NO EN FIREBASE
            viaje_planeado["ToDeliverNumberDestinations"] =  "1" # NO EN FIREBASE
            viaje_planeado["ToDeliverNumberPackages"] =  "11" # NO EN FIREBASE
            viaje_planeado["EstimatedFinishRoute"] = viajes_firebase["fecha-retorno"]
            viaje_planeado["MailMessages"] = "aaa@qwer.com; CCCC@abc.com" # NO EN FIREBASE
            viaje_planeado["WahtsAppMessages"] = "+555234567689; +525599887766" # NO EN FIREBASE
            viaje_planeado["SendMailIn10K"] = "1" # NO EN FIREBASE
            viaje_planeado["SendWahtsAppIn10K"] = "1" # NO EN FIREBASE
            viaje_planeado["FechaSalida"] = transacciones_firebase["fecha-salida"]
            viaje_planeado["FechaRetorno"] = viajes_firebase["fecha-retorno"]

            logger.info('DESTINOS PLANEADOS OBTENIDOS')
            viaje_planeado["Destinations"] = destinos_planeados

            logger.info('ITINERARIO Y FOTOS NO DISPONIBLES PARA PLANEADO')
            viaje_planeado["Itinerary"] = list()
            viaje_planeado["FotoRemisiones"] = list()
            viaje_planeado["FotoEntrega"] = list()

            logger.info('CATALOGO PARA FILTRO CON PLANEADO')
            self.type_vehicle.append(viaje_planeado["TypeVehicle"])
            self.state_truck.append(viaje_planeado["StateTruck"])
            self.drivers.append(viaje_planeado["Driver"])
            self.trips.append(viaje_planeado["IdViaje"])
            # self.type_packages.append(viaje_planeado["TypePackages"])
            self.truck_size.append(viaje_planeado["SizeTruckShow"])
            self.ware_house_size.append(viaje_planeado["WarehouseSize"])
            
            logger.info('AGREGANDO VIAJE PLANEADO A RESULTADO FINAL')
            viajes_completo.append(viaje_planeado)

        logger.info('AGREGANDO VIAJES A JSON COMPLETO')
        res_final["viajes"] = viajes_completo

        logger.info('OBTENIENDO DATOS UNICOS PARA CATALOGO DE FILTROS')
        _type_vehicle = list(set(self.type_vehicle))
        _state_truck = list(set(self.state_truck))
        _drivers = list(set(self.drivers))
        _trips = list(set(self.trips))
        _regions = list(set(self.regions))
        _states = list(set(self.states))
        _type_packages = list(set(self.type_packages))
        _truck_size = list(set(self.truck_size))
        _ware_house_size = list(set(self.ware_house_size))

        logger.info('AGREGANDO TODA DE LOS CATALOGOS AL RESULTADO FINAL')
        res_final["typeVehicles"] = _type_vehicle
        res_final["stateTrucks"] = _state_truck
        res_final["drivers"] = _drivers
        res_final["trips"] = _trips
        res_final["regions"] = _regions
        res_final["states"] = _states
        res_final["typePackages"] = _type_packages
        res_final["TruckSize"] = _truck_size
        res_final["WareHouseSize"] = _ware_house_size
        
        return res_final
    
    def obtener_datos_destino_fb(self, cedi, viaje, viajes_firebase, dfActividades):
        logger.info('OBTENIENDO LOS DESTINOS DE FIREBASE')
        print (viaje)
        destinos_transacciones_firebase = self.fbm.get(self.fbm.nodo_destinos_real(cedi, viaje), {})
        destinos_reales = list()
        destinos_planeados = list()
        
        logger.info('QUITA EL NODO 0 QUE SIEMPRE VIENE VACIO')
        
        viajes_firebase["destinos"].pop(0)
        dfViajes = pd.DataFrame(viajes_firebase["destinos"])
        print (json.dumps(destinos_transacciones_firebase))
        print (",,,")
        for idx, destino_item in enumerate(destinos_transacciones_firebase):
            destino_real = dict()
            print (idx)
            print (json.dumps(destino_item))
            print ("....")
            if idx != 0:
                logger.info('OBTENIENDO ID DE DESTINO PARA EXTRAER SUS DATOS')
                if destino_item and "id-destino" in destino_item:
                    id_destino = destino_item["id-destino"]
                    dfDestinoSelect = dfViajes.loc[dfViajes['id-destino'] == int(id_destino)]
                    
                    duracion_servicio = dfActividades.loc[(dfActividades['tirada-actividad'] == str(idx)) & (dfActividades['tipo-actividad'] == "Servicio")].iloc[0].loc['duracion-actividad']
                    
                    # with pd.option_context('display.max_rows', None, 'display.max_columns', None):
                    #     print (dfDestinoSelect)
                    #     print (dfDestinoSelect['contactos'].iloc[0])
                    contactos = str(dfDestinoSelect['contactos'].iloc[0])
                    contactos = contactos.replace('None, ', '').replace('[', '').replace(']', '').replace('{', '').replace('}', '').replace("'", '')

                    pof = 1
                    pof_tiempo = 1
                    pof_doc_completa = 1
                    pof_entrega = 1
                    pof_sin_danos = 1

                    if not destino_item["pof"]["docum-completa"]:
                        pof = 0
                        pof_doc_completa = 0
                    if not destino_item["pof"]["entrega"]:
                        pof = 0
                        pof_entrega = 0
                    if not destino_item["pof"]["sin-danos"]:
                        pof = 0
                        pof_sin_danos = 0

                    logger.info('ARMANDO JSON DE DESTINOS')
                    destino_real["Tirada"] = idx
                    destino_real["SizeDestinationShow"] = "S" # NO EN FIREBASE
                    destino_real["LatitudDestinationTR1"] = destino_item["latitud-llegada"]
                    destino_real["LongitudDestinationTR1"] = destino_item["longitud-llegada"]
                    destino_real["LastDateMobility"] = "2/7/2019 14:34" # NO EN FIREBASE
                    destino_real["FechaHoraAutomaticasActivity"] = 1 if destino_item["hora-fecha-automaticas-llegada"] else 0
                    destino_real["ExactitudCoordenadasActivity"] = destino_item["exactitud-coordenadas-llegada"]
                    destino_real["DisponibilidadCoordenadasActivity"] = 1 if destino_item["disponibilidad-coordenadas-llegada"] else 0
                    destino_real["IsMockedActivity"] = 1 if destino_item["ismocked-llegada"] else 0
                    destino_real["IdDestino"] = id_destino
                    destino_real["DestinationTR1"] = dfDestinoSelect['destino-entrega'].iloc[0]
                    destino_real["Address"] = dfDestinoSelect['calle-numero'].iloc[0]
                    destino_real["Region"] = "ORIENTE" # NO EN FIREBASE
                    destino_real["State"] = dfDestinoSelect['estado'].iloc[0]
                    destino_real["City"] = dfDestinoSelect['ciudad'].iloc[0]
                    # destino["FinalDestinationTR2"] = "274-UMF CHALCO DE DIAZ COVARRUBIAS" NO DEBE VENIR A NIVEL DESTINO
                    # destino["LatitudDestinationTR2"] = "19.268545" NO DEBE VENIR A NIVEL DESTINO
                    # destino["LongitudDestinationTR2"] = "-98.889883" NO DEBE VENIR A NIVEL DESTINO
                    destino_real["Recepcionist"] = "Jane Lopez" # NO EN FIREBASE
                    destino_real["Contact"] = contactos
                    # destino["RankingDestination"] = "1" NO SABEMOS COMO MEDIR EL NIVEL DEL DESTINO
                    destino_real["TimeWindow"] = dfDestinoSelect["ventana"].iloc[0]
                    destino_real["ServiceDuration"] = duracion_servicio
                    destino_real["TypePackages"] = "ORDINARIO"
                    destino_real["ActualWeight"] = "2002.89" # NO EN FIREBASE
                    destino_real["ActualVolume"] = "10.28" # NO EN FIREBASE
                    destino_real["ActualCostMerchandise"] = "2051521.20" # NO EN FIREBASE
                    destino_real["NumberPackages"] = int(dfDestinoSelect["pedidos"].iloc[0])
                    destino_real["NumberPieces"] = "27478" # NO EN FIREBASE
                    destino_real["Delivery"] = destino_item["fecha-llegada"]
                    destino_real["Trip"] = viaje
                    destino_real["Sequence"] = idx
                    # destino["StateDelivered"] = "1" ELIMINADO
                    destino_real["POF"] = pof
                    destino_real["POF-OnTime"] = pof_tiempo
                    destino_real["POF-Complete"] = pof_entrega
                    destino_real["POF-NoDamage"] = pof_sin_danos
                    destino_real["POF-DocsOK"] = pof_doc_completa
                    destino_real["MailMessages"] = "aaa@qwer.com; CCCC@abc.com" # NO EN FIREBASE
                    destino_real["WahtsAppMessages"] = "+555234567689; +525599887766" # NO EN FIREBASE
                    destino_real["SendMailIn10K"] = "1" # NO EN FIREBASE
                    destino_real["SendWahtsAppIn10K"] = "1" # NO EN FIREBASE
                    
                    destinos_reales.append(destino_real)
                    
                    logger.info('AGREGANDO REGIONES Y ESTADOS A LOS CATALOGOS')
                    self.type_packages.append(destino_real["TypePackages"])
                    self.regions.append(destino_real["Region"])
                    self.states.append(destino_real["State"])
        
        for idx, destino_planeado in enumerate(viajes_firebase["destinos"]):
            destino_plan = dict()

            contactos = str(destino_planeado['contactos'])
            contactos = contactos.replace('None, ', '').replace('[', '').replace(']', '').replace('{', '').replace('}', '').replace("'", '')

            pof = 1
            pof_tiempo = 1
            pof_doc_completa = 1
            pof_entrega = 1
            pof_sin_danos = 1

            logger.info('ARMANDO JSON DE DESTINOS')
            destino_plan["Tirada"] = idx +1
            destino_plan["SizeDestinationShow"] = "S" # NO EN FIREBASE
            destino_plan["LatitudDestinationTR1"] = destino_planeado["latitud"]
            destino_plan["LongitudDestinationTR1"] = destino_planeado["longitud"]
            destino_plan["LastDateMobility"] = "2/7/2019 14:34" # NO EN FIREBASE
            destino_plan["FechaHoraAutomaticasActivity"] = None
            destino_plan["ExactitudCoordenadasActivity"] = None
            destino_plan["DisponibilidadCoordenadasActivity"] = None
            destino_plan["IsMockedActivity"] = None
            destino_plan["IdDestino"] = destino_planeado['id-destino']
            destino_plan["DestinationTR1"] = destino_planeado['destino-entrega']
            destino_plan["Address"] = destino_planeado['calle-numero']
            destino_plan["Region"] = "ORIENTE" # NO EN FIREBASE
            destino_plan["State"] = destino_planeado['estado']
            destino_plan["City"] = destino_planeado['ciudad']
            # destino["FinalDestinationTR2"] = "274-UMF CHALCO DE DIAZ COVARRUBIAS" NO DEBE VENIR A NIVEL DESTINO
            # destino["LatitudDestinationTR2"] = "19.268545" NO DEBE VENIR A NIVEL DESTINO
            # destino["LongitudDestinationTR2"] = "-98.889883" NO DEBE VENIR A NIVEL DESTINO
            destino_plan["Recepcionist"] = "Jane Lopez" # NO EN FIREBASE
            destino_plan["Contact"] = contactos
            # destino["RankingDestination"] = "1" NO SABEMOS COMO MEDIR EL NIVEL DEL DESTINO
            destino_plan["TimeWindow"] = destino_planeado["ventana"]
            destino_plan["ServiceDuration"] = 1 # HAY QUE SACARLO DE MySQL
            destino_plan["TypePackages"] = "ORDINARIO"
            destino_plan["ActualWeight"] = "2002.89" # NO EN FIREBASE
            destino_plan["ActualVolume"] = "10.28" # NO EN FIREBASE
            destino_plan["ActualCostMerchandise"] = "2051521.20" # NO EN FIREBASE
            destino_plan["NumberPackages"] = int(destino_planeado["pedidos"])
            destino_plan["NumberPieces"] = "27478" # NO EN FIREBASE
            destino_plan["Delivery"] = destino_planeado["fecha-llegada"]
            destino_plan["Trip"] = viaje
            destino_plan["Sequence"] = idx +1
            # destino["StateDelivered"] = "1" ELIMINADO
            destino_plan["POF"] = pof
            destino_plan["POF-OnTime"] = pof_tiempo
            destino_plan["POF-Complete"] = pof_entrega
            destino_plan["POF-NoDamage"] = pof_sin_danos
            destino_plan["POF-DocsOK"] = pof_doc_completa
            destino_plan["MailMessages"] = "aaa@qwer.com; CCCC@abc.com" # NO EN FIREBASE
            destino_plan["WahtsAppMessages"] = "+555234567689; +525599887766" # NO EN FIREBASE
            destino_plan["SendMailIn10K"] = "1" # NO EN FIREBASE
            destino_plan["SendWahtsAppIn10K"] = "1" # NO EN FIREBASE
            
            destinos_planeados.append(destino_plan)
            
            logger.info('AGREGANDO REGIONES Y ESTADOS A LOS CATALOGOS')
            self.type_packages.append(destino_plan["TypePackages"])
            self.regions.append(destino_plan["Region"])
            self.states.append(destino_plan["State"])

        logger.info('REGRESANDO TODA LA INFORMACION DE LOS DESTINOS')
        return destinos_reales, destinos_planeados
    
    def obtener_datos_itinerario_fb(self, cedi, viaje, dfActividades):
        logger.info('OBTENIENDO ITINERARIO')
        dfActividades = dfActividades.loc[dfActividades['tirada-actividad'] >= "1"]
        dfActividades['fecha-actividad'] = pd.to_datetime(dfActividades['fecha-actividad'])
        dfActividades = dfActividades.sort_values(by='fecha-actividad', ascending=True).reset_index(drop=True)
        dfActividades = dfActividades[['tirada-actividad','latitud-actividad','longitud-actividad','duracion-actividad','tipo-actividad']]
        json_itinerario = dfActividades.to_json(orient='records')
        
        logger.info('REGRESANDO ITINERARIO')
        return json.loads(json_itinerario)
