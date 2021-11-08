#-*- coding: utf-8 -*-

from logging import getLogger, basicConfig
import json
import pandas as pd
import sys
import os
import operator
from datetime import datetime, date, time, timedelta
import numpy as np
import math

logger = getLogger()

# respuesta = calcula_ruta(origen, destino, tirada_origen, talla_vehiculo, dfviaje)

class Itinerario():
    def __init__(self):
        self.ItinFull = []
        self.CdfConfiguracion = None
        self.datos = None

    def recalcular(self, datos):
        self.datos = datos
        self.CdfConfiguracion = pd.DataFrame(self.datos['dfConfiguracion'])

        for viaje in self.datos['viajes']:
            viajeN = self.datos['viajes'][viaje]

            for arco in self.datos['arcos']:
                dfviaje = pd.DataFrame(viajeN)
                dfarco = pd.DataFrame([arco])
                dfviaje['FechaEntregaPedido'] = pd.to_datetime(dfviaje['FechaEntregaPedido'])
                dfviaje['FechaSalidaPedido'] = pd.to_datetime(dfviaje['FechaSalidaPedido'])
                dfviaje['FechaRetorno'] = pd.to_datetime(dfviaje['FechaRetorno'])
                dfviaje['FechaSalida'] = pd.to_datetime(dfviaje['FechaSalida'])

                if viajeN[0]['tipo_ruta'] == 'Subida':
                    self.calcula_ruta_subida(self.datos["origen"], self.datos["destino"], self.datos["tirada_origen"], self.datos["talla_vehiculo"], dfviaje, dfarco, viajeN[0]['tipo_ruta'])
                    # print(itinerario)
                    # ItinFull.append(itinerario)
                else:
                    print('hola')
                    self.calcula_ruta(self.datos["origen"], self.datos["destino"], self.datos["tirada_origen"], self.datos["talla_vehiculo"], dfviaje, dfarco, viajeN[0]['tipo_ruta'])
                    # print(itinerario)
                    # ItinFull.append(itinerario)
        print('\n')
        print('\n')
        print(self.ItinFull)
        print('\n')
        print('\n')

        return True, self.ItinFull


    def calcula_ruta(self, origen, destino, tirada_origen, talla_vehiculo, dfviaje, arco, tipo_ruta, dummy=False, verbose=False, respetarventana=False, numHoteles=None, numComidas=None, numDescansos=None):
        try:
            #verbose = True
            viaje = dfviaje['Viaje'].iloc[0]
            if self.datos['usuario']['funciones']:
                if "sin trayecto retorno" in self.datos['usuario']['funciones'] and tipo_ruta == 'Retorno':
                    numHoteles = 0
                    numComidas = 0
                    numDescansos = 0

            if numHoteles is None and tipo_ruta == 'Retorno' and 'catalogo hoteles' in self.datos['usuario']['funciones']:
                numHoteles = dfviaje['HotelesRetorno']

                if self.CdfConfiguracion['FacHoteles'].values[0] == 0:
                    numHoteles = 0

                print("Aqui numHoteles retorno", numHoteles)
                #if numHoteles is None or np.isnan(numHoteles):
                #    numHoteles = None

            tirada_origen = int(tirada_origen)
            ban_cambio_dia = False
            tirada_recalculo = 0
            viaje = dfviaje['Viaje'].iloc[0]
            fecha_dummy = datetime(9999, 12, 12, 23, 59)
            count_hitos = 0

            if verbose:
                print("calcula_ruta funcion", viaje, tirada_origen, ":", origen,
                    "->", tirada_origen+1, ":", destino, talla_vehiculo, "dummy = ", dummy)

            array_itinerario = pd.DataFrame(columns=self.datos["columns_itinerario"])

            # Variables de self.datosuracion
            duracion_comida = int(self.CdfConfiguracion['DuracionComida'].values[0]) * 60
            duracion_hotel = int(self.CdfConfiguracion['DuracionHotel'].values[0]) * 60
            hora_comida = self.CdfConfiguracion['HoraComida'].values[0].split(':')
            intervalo_descanso = int(self.CdfConfiguracion['IntervaloDescanso'].values[0]) * 60
            trayecto_comida_prioritaria = int(self.CdfConfiguracion['DuracionTrayectoComidaPrioritaria'].values[0]*60)
            # intervalo_descanso = 120*60
            duracion_descanso = int(self.CdfConfiguracion['DuracionDescanso'].values[0]) * 60

            if tipo_ruta == 'Intermedio':
                tiempo_servicio = int(self.calcula_tiempo_servicio(dfviaje, tirada_origen + 1))
            else:
                tiempo_servicio = 0
            tiempo_servicio_numpy = np.timedelta64(tiempo_servicio, 'm')

            tiempo_servicio_origen = int(self.calcula_tiempo_servicio(dfviaje, tirada_origen))
            tiempo_servicio__origen_numpy = np.timedelta64(tiempo_servicio_origen, 'm')

            # obtiene la fecha de entrega del origen
            fecha_entrega_origen = dfviaje.loc[(dfviaje['Tirada'] == tirada_origen), 'FechaEntregaPedido'].unique()[0]
            fecha_entrega_origen_datetime = datetime.utcfromtimestamp(fecha_entrega_origen.astype('O') / 1e9)

            fecha_llegada_viaje = dfviaje.loc[(dfviaje['Tirada'] == tirada_origen), 'FechaRetorno'].unique()[0]
            fecha_llegada_viaje_datetime = datetime.utcfromtimestamp(fecha_llegada_viaje.astype('O') / 1e9)

            # obtiene la fecha de entrega del destino
            if tipo_ruta == 'Intermedio':
                fecha_entrega_destino = \
                    dfviaje.loc[(dfviaje['Tirada'] == tirada_origen + 1), 'FechaEntregaPedido'].unique()[0]
            else:
                fecha_entrega_destino = dfviaje.loc[(dfviaje['Tirada'] == tirada_origen), 'FechaEntregaPedido'].unique()[0]
            fecha_entrega_destino_datetime = datetime.utcfromtimestamp(fecha_entrega_destino.astype('O') / 1e9)

            if verbose:
                print("fecha_entrega_origen_datetime", fecha_entrega_origen_datetime)

            # obtiene la fecha de salida del origen
            fecha_salida_origen = fecha_entrega_origen + tiempo_servicio__origen_numpy
            dfviaje.loc[(dfviaje['Tirada'] == tirada_origen), 'FechaSalidaPedido'] = fecha_salida_origen
            fecha_salida_origen_datetime = datetime.utcfromtimestamp(fecha_salida_origen.astype('O') / 1e9)

            if verbose:
                print("fecha_salida_origen", fecha_salida_origen_datetime)

            # obtiene la fecha de comida
            fecha_comida = fecha_salida_origen_datetime
            fecha_comida = fecha_comida.replace(hour=int(hora_comida[0]), minute=int(hora_comida[1]), second=0)
            # Si la fecha de comida es anterior a la salida del origen
            if fecha_comida < fecha_salida_origen_datetime:
                # Y la fecha de comida es mayot a la entrega del origen, se activa bandera de comida ...
                if fecha_comida > fecha_entrega_origen_datetime:
                    if self.CdfConfiguracion['FacComidas'].values[0] == 1 and int(duracion_comida) != 0:
                        self.datos['bandera_comida'] = True
                        if verbose:
                            print("La comida cayo en el servicio del origen")
                # La siguiente comida caería hasta el siguiente día ...
                fecha_comida = fecha_comida + timedelta(days=1)

            fecha_hotel = fecha_salida_origen_datetime
            fecha_hotel = fecha_hotel.replace(hour=21, minute=1, second=0)
            # Si la fecha de comida es anterior a la salida del origen
            if fecha_hotel < fecha_salida_origen_datetime:

                if fecha_hotel > fecha_entrega_origen_datetime:
                    if self.CdfConfiguracion['FacHoteles'].values[0] == 1 and int(duracion_hotel) != 0:
                            self.datos['bandera_hotel'] = True
                            if verbose:
                                print("El hotel  cayo en el servicio del origen")
                # El siguiente hotel caería hasta el siguiente día ...
                fecha_hotel = fecha_hotel + timedelta(days=1)

            # Por ahora el ultimo hito fue la salida del destino
            ultimo_hito = fecha_salida_origen_datetime

            # Se calcula el tiempo de ruta sin descansos ni comidas

            arco = pd.DataFrame(arco)
            campo_factor = 'Factor' + tipo_ruta + talla_vehiculo

            if tipo_ruta != 'Retorno':
                factor_ruta = self.datos['factor_ruta']
            else:
                factor_ruta = self.datos['factor_ruta']


            if factor_ruta is None:
                factor_ruta = 0
            if math.isnan(factor_ruta):
                factor_ruta = 0

            tiempo_ruta = arco.TIEMPO.unique()[0] * (1 + (float(factor_ruta) / 100))
            tiempo_arco = tiempo_ruta

            if verbose:
                print("tiempo_ruta", tiempo_ruta)

            if self.datos['bandera_comida'] or self.datos['bandera_hotel']:

                if self.datos['bandera_comida']:
                    duracion_Ac = duracion_comida
                    hito_Ac = 'Comida'
                else:
                    duracion_Ac = duracion_hotel
                    hito_Ac = 'Hotel'

                ban_agregar = 1
                if dummy and tipo_ruta == 'Intermedio':
                    if fecha_entrega_destino_datetime < (ultimo_hito + timedelta(seconds=duracion_Ac)):
                        ban_agregar = 0

                if self.datos['bandera_comida'] and numComidas == 0:
                    ban_agregar = 0

                if self.datos['bandera_hotel'] and numHoteles == 0:
                    ban_agregar = 0

                if ban_agregar:
                    dataframe_hito = self.crea_diccionario_itinerario(hito_Ac, ultimo_hito.strftime('%d/%m/%Y %H:%M'),
                                                (ultimo_hito + timedelta(seconds=duracion_Ac)).strftime('%d/%m/%Y %H:%M'), viaje, tirada_origen + 1
                                                )
                    ultimo_hito = ultimo_hito + timedelta(seconds=duracion_Ac)
                    array_itinerario = array_itinerario.append(dataframe_hito, ignore_index=True,sort=False)

                    if self.datos['bandera_comida'] and not numComidas is None:
                        numComidas = numComidas - 1
                    if self.datos['bandera_hotel'] and not numHoteles is None:
                        numHoteles = numHoteles - 1

                    count_hitos = count_hitos + 1

                    self.datos['bandera_comida'] = False
                    self.datos['bandera_hotel'] = False
                    if verbose:
                        print("Agrego comida u hotel: hora", ultimo_hito)
                        print("Fin comida u hotel: ", ultimo_hito)
                    tiempo_ruta = tiempo_ruta + duracion_Ac

            # Diccionario de prioridades segun tipo de ruta
            if not dummy:
                if tipo_ruta == 'Retorno':
                    if tiempo_ruta >= self.CdfConfiguracion['IntervaloHotel'].values[0] * 60 or (not numHoteles is None and numHoteles > 0):
                        Prioridades = {'Descanso': 0, 'Comida': 2, 'Hotel': 3, 'Entrega': 1}
                        Prioridades_index = ['Descanso', 'Entrega', 'Comida', 'Hotel']
                    else:
                        Prioridades = {'Descanso': 1, 'Comida': 3, 'Hotel': 0, 'Entrega': 2}
                        Prioridades_index = ['Hotel', 'Descanso', 'Entrega', 'Comida']
                else:
                    if tiempo_ruta >= trayecto_comida_prioritaria:
                        Prioridades = {'Descanso': 0, 'Comida': 2, 'Hotel': 3, 'Entrega': 1}
                        Prioridades_index = ['Descanso', 'Entrega', 'Comida', 'Hotel']
                    else:
                        Prioridades = {'Descanso': 0, 'Comida': 1, 'Hotel': 3, 'Entrega': 2}
                        Prioridades_index = ['Descanso', 'Comida', 'Entrega', 'Hotel']
            else:
                Prioridades = {'Descanso': 0, 'Comida': 1, 'Hotel': 3, 'Entrega': 2}
                Prioridades_index = ['Descanso', 'Comida', 'Entrega', 'Hotel']

            # Calcula la lista de hitos ....
            hitos = [0] * len(Prioridades)
            hitos_final = [0] * len(Prioridades)
            while True:
                if not numHoteles is None:
                    if numHoteles <= 0:
                        duracion_hotel = 0
                if not numComidas is None:
                    if numComidas <= 0:
                        duracion_comida = 0
                if not numDescansos is None:
                    if numDescansos <= 0:
                        duracion_descanso = 0

                todoshitos = [numHoteles, numComidas, numDescansos]

                if self.CdfConfiguracion['FacDescansos'].values[0] == 1 and int(intervalo_descanso) != 0 and int(duracion_descanso) != 0:
                    if tiempo_ruta > intervalo_descanso or (not numDescansos is None and numDescansos > 0):
                        hitos[Prioridades['Descanso']] = ultimo_hito + timedelta(seconds=int(intervalo_descanso))
                        hitos_final[Prioridades['Descanso']] = hitos[Prioridades['Descanso']] + timedelta(seconds=int(duracion_descanso))
                    else:
                        hitos[Prioridades['Descanso']] = fecha_dummy
                        hitos_final[Prioridades['Descanso']] = fecha_dummy
                else:
                    hitos[Prioridades['Descanso']] = fecha_dummy
                    hitos_final[Prioridades['Descanso']] = fecha_dummy

                if self.CdfConfiguracion['FacComidas'].values[0] == 1 and int(duracion_comida) != 0:
                    hitos[Prioridades['Comida']] = fecha_comida
                    hitos_final[Prioridades['Comida']] = fecha_comida + timedelta(seconds=int(duracion_comida))
                else:
                    hitos[Prioridades['Comida']] = fecha_dummy
                    hitos_final[Prioridades['Comida']] = fecha_dummy

                if dummy:
                    if tipo_ruta == "Intermedio":
                        hitos[Prioridades['Entrega']] = fecha_entrega_destino_datetime
                    else:
                        hitos[Prioridades['Entrega']] = fecha_llegada_viaje_datetime
                else:

                    if todoshitos == [None, None, None]:
                        print("Aqui define tiempo de llegada")
                        print("fecha_salida_origen_datetime", fecha_salida_origen_datetime)
                        print("tiempo_ruta", tiempo_ruta, "en horas", tiempo_ruta/3600)
                        tiempo_llegada = fecha_salida_origen_datetime + timedelta(seconds=tiempo_ruta)
                    elif (todoshitos == [0, None, None]) or (todoshitos == [0, 0, None]) or (todoshitos == [0, 0, 0]):
                        if count_hitos > 0:
                            tiempo_llegada = max([fecha_salida_origen_datetime + timedelta(seconds=tiempo_ruta),
                                                                       datetime.strptime(array_itinerario.iloc[-1, array_itinerario.columns.get_loc('Fin')], '%d/%m/%Y %H:%M')])
                        else:
                            tiempo_llegada = fecha_salida_origen_datetime + timedelta(seconds=tiempo_ruta)
                    else:
                        tiempo_llegada = fecha_dummy

                    if not respetarventana or tipo_ruta == 'Retorno':
                        hitos[Prioridades['Entrega']] = tiempo_llegada
                    else:
                        hora_tentativa = (tiempo_llegada).time()
                        inicio_ventana = dfviaje.loc[dfviaje['Tirada'] == tirada_origen+1, 'VentanaInicioDestino'].values[0]
                        fin_ventana = dfviaje.loc[dfviaje['Tirada'] == tirada_origen + 1, 'VentanaFinDestino'].values[0]
                        inicio_ventana = datetime.strptime(inicio_ventana, "%H:%M").time()
                        fin_ventana = datetime.strptime(fin_ventana, "%H:%M").time()
                        if verbose:
                            print("hora_tentativa", hora_tentativa, "inicio_ventana", inicio_ventana, "fin_ventana", fin_ventana)
                        if hora_tentativa >= inicio_ventana and hora_tentativa <= fin_ventana:
                            hitos[Prioridades['Entrega']] = tiempo_llegada
                        else:
                            fecha_tentativa_inicial = tiempo_llegada
                            fecha_con_ventanas = fecha_tentativa_inicial.replace(hour=inicio_ventana.hour,
                                                                                 minute=inicio_ventana.minute, second=0)
                            if verbose:
                                print("fecha_tentativa_inicial", fecha_tentativa_inicial, "fecha_con_ventanas", fecha_con_ventanas)
                            if fecha_con_ventanas < fecha_tentativa_inicial:
                                fecha_con_ventanas = fecha_con_ventanas + timedelta(days=1)
                            hitos[Prioridades['Entrega']] = fecha_con_ventanas
                hitos_final[Prioridades['Entrega']] = hitos[Prioridades['Entrega']]
                tiempo_ruta_ac = (hitos[Prioridades['Entrega']] - fecha_salida_origen_datetime).total_seconds()

                if self.CdfConfiguracion['FacHoteles'].values[0] == 1 and int(duracion_hotel) != 0:
                    hitos[Prioridades['Hotel']] = fecha_hotel
                    hitos_final[Prioridades['Hotel']] = fecha_hotel + timedelta(seconds=int(duracion_hotel))

                    if hitos_final[Prioridades['Hotel']].time() > hitos[Prioridades['Entrega']].time() and hitos_final[Prioridades['Hotel']].date() == hitos[Prioridades['Entrega']].date() and dummy:
                        hitos_final[Prioridades['Hotel']] = hitos_final[Prioridades['Hotel']].replace(hour=hitos[Prioridades['Entrega']].hour, minute=hitos[Prioridades['Entrega']].minute)

                else:
                    hitos[Prioridades['Hotel']] = fecha_dummy
                    hitos_final[Prioridades['Hotel']] = fecha_dummy

                hitos_eliminar = 0
                hitos_eliminar_ac = 0
                while True:
                    hitos = hitos[hitos_eliminar:]
                    hitos_final = hitos_final[hitos_eliminar:]

                    if verbose:
                        print("hitos", hitos)
                        print("hitos_final", hitos_final)

                    indice_hito, proximo_hito = min(enumerate(hitos), key=operator.itemgetter(1))
                    hito_comp = [0]
                    hito_comp[0] = hitos_final[indice_hito]
                    hito_comp.extend(hitos[indice_hito + 1:])

                    if verbose:
                        print("indice_hito cual fue", indice_hito)
                        print("hito_comp = final vs inicio otros", hito_comp)

                    if indice_hito == Prioridades_index.index("Comida"):
                        self.datos['bandera_comida'] = True

                    if verbose:
                        print("proximo_hito_inicial", proximo_hito, "tipo: ",
                            Prioridades_index[hitos_eliminar_ac + indice_hito])

                    if Prioridades_index[hitos_eliminar_ac + indice_hito] == 'Comida':
                        self.datos['bandera_comida'] = True

                    indice_hito_f, proximo_hito_f = min(enumerate(hito_comp), key=operator.itemgetter(1))

                    if verbose:
                        print("hito_final vs inicial otros", proximo_hito, "tipo: ",
                            Prioridades_index[hitos_eliminar_ac + indice_hito])

                    if indice_hito_f == 0 or len(hito_comp) < 2:
                        hito_siguiente = hitos_eliminar_ac + indice_hito
                        tipo_hito = Prioridades_index[hito_siguiente]
                        if verbose:
                            print("El hito fue", Prioridades_index[hito_siguiente])
                            print("tiempo_ruta_ac", tiempo_ruta_ac, "intervalo_comida_prioritaria", trayecto_comida_prioritaria)
                        if tipo_hito == 'Comida' and (tiempo_ruta_ac < trayecto_comida_prioritaria) and tipo_ruta == 'Intermedio' and dummy==False:
                            if verbose:
                                print("Fue comida pero no alcanza a meterla ")
                            print("Fue comida pero no alcanza a meterla ")
                            hitos_eliminar = indice_hito + 1
                            hitos_eliminar_ac = hitos_eliminar_ac + hitos_eliminar
                        else:
                            break
                    else:
                        if verbose:
                            print("El hito no finalizo antes del inicio de los otros, se va a eliminar los menores = prioridad")
                        hitos_eliminar = indice_hito + 1
                        hitos_eliminar_ac = hitos_eliminar_ac + hitos_eliminar

                tipo_hito = Prioridades_index[hito_siguiente]

                if tipo_hito != 'Entrega':

                    dataframe_hito = self.crea_diccionario_itinerario(tipo_hito, hitos[indice_hito].strftime('%d/%m/%Y %H:%M'),
                                                                 hitos_final[indice_hito].strftime('%d/%m/%Y %H:%M'), viaje, tirada_origen + 1)

                    array_itinerario = array_itinerario.append(dataframe_hito, ignore_index=True,sort=False)
                    count_hitos += 1

                    # array_itinerario.append(dict([(tipo_hito, ultimo_hito)]))
                    if verbose:
                        print(array_itinerario)
                        print("Agregue " + tipo_hito, "Fecha: = ", hitos[indice_hito])
                    ultimo_hito = hitos_final[indice_hito]
                    if verbose:
                        print("Fin " + tipo_hito, "Fecha: = ", ultimo_hito)
                        print("Tiempo ruta ant: ", tiempo_ruta)
                    tiempo_ruta = tiempo_ruta + (hitos_final[indice_hito] - hitos[indice_hito]).seconds
                    if verbose:
                        print("Tiempo ruta post: ", tiempo_ruta)
                    if tipo_hito == 'Descanso':
                        if not numDescansos is None:
                            numDescansos = numDescansos - 1
                    if tipo_hito == 'Comida':
                        if not numComidas is None:
                            numComidas = numComidas -1
                        self.datos['bandera_comida'] = False
                    if tipo_hito == 'Hotel':
                        if not numHoteles is None:
                            numHoteles = numHoteles -1
                        self.datos['bandera_hotel'] = False

                    if tiempo_ruta >= tiempo_ruta_ac:
                        tiempo_restante = tiempo_ruta - (ultimo_hito - fecha_salida_origen_datetime).total_seconds()
                    else:
                        tiempo_restante = tiempo_ruta_ac - (ultimo_hito - fecha_salida_origen_datetime).total_seconds()
                    if verbose:
                        print("fecha_entrega_origen_datetime - ultimo_hito", ultimo_hito - fecha_salida_origen_datetime)
                        print("tiempo_restante", tiempo_restante, "En horas ", tiempo_restante/3600)

                else:

                    break

                if ultimo_hito > fecha_comida:
                    fecha_comida = fecha_comida + timedelta(days=1)

                if ultimo_hito > fecha_hotel:
                    fecha_hotel = fecha_hotel + timedelta(days=1)

                hitos = [0] * len(Prioridades)
                hitos_final = [0] * len(Prioridades)

            if tiempo_ruta >= tiempo_ruta_ac:
                delta_ruta = pd.Timedelta(timedelta(seconds=tiempo_ruta))
            else:
                delta_ruta = pd.Timedelta(timedelta(seconds=tiempo_ruta_ac))

            if tipo_ruta == 'Intermedio':
                if not dummy:
                    dfviaje.loc[
                        (dfviaje['Tirada'] == tirada_origen + 1), 'FechaEntregaPedido'] = fecha_salida_origen + delta_ruta

                    fecha_entrega_destino = dfviaje.loc[
                        (dfviaje['Tirada'] == tirada_origen + 1), 'FechaEntregaPedido'].unique()[0]
                    dfviaje.loc[(dfviaje['Tirada'] == tirada_origen + 1), 'FechaSalidaPedido'] = fecha_entrega_destino + \
                                                                                                 tiempo_servicio_numpy


                    fecha_entrega_destino_datetime = datetime.utcfromtimestamp(fecha_entrega_destino.astype('O') / 1e9)

                    if fecha_entrega_origen_datetime.date() != fecha_entrega_destino_datetime.date():
                        if verbose:
                            print("El recalculo afecta otro dia")
                        ban_cambio_dia = True
                        tirada_recalculo = tirada_origen + 1
            else:
                if not dummy:
                    if self.datos['usuario']['funciones']:
                        if "sin trayecto retorno"  in self.datos['usuario']['funciones']:
                            dfviaje.loc[:, 'FechaRetorno'] = fecha_salida_origen + pd.Timedelta(timedelta(seconds=0))
                            fecha_entrega_destino_datetime = fecha_salida_origen + pd.Timedelta(timedelta(seconds=0))
                        else:
                            dfviaje.loc[:, 'FechaRetorno'] = fecha_salida_origen + delta_ruta
                            fecha_entrega_destino_datetime = fecha_salida_origen + delta_ruta
                    else:
                        dfviaje.loc[:, 'FechaRetorno'] = fecha_salida_origen + delta_ruta
                        fecha_entrega_destino_datetime = fecha_salida_origen + delta_ruta

            array_itinerario = self.inserta_hitos_trayectos(array_itinerario, fecha_salida_origen_datetime,
                                                       fecha_entrega_destino_datetime, viaje, tirada_origen, tipo_ruta, tiempo_arco)

            itinerarioJson = json.loads(array_itinerario.to_json(orient='records'))
            viaje = json.loads(dfviaje.to_json(orient='records'))
            arco = json.loads(arco.to_json(orient='records'))
            itinerario = {
                'viaje' : viaje[0]['Viaje'],
                'origen' : arco[0]['ORIGEN'],
                'destino' : arco[0]['DESTINO'],
                'itinerario' : itinerarioJson,
                'tipo_ruta' : tipo_ruta
            }
            self.ItinFull.append(itinerario)
            return itinerario

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            error = '{} - {} - linea {}  ({})'.format(exc_type, fname, exc_tb.tb_lineno, str(e))
            logger.error(error)
            return False, False, 0

    def calcula_ruta_subida(self, origen, destino, tirada_origen, talla_vehiculo, dfviaje, arco, tipo_ruta, verbose=False, dummy=False, numHoteles=None, numComidas=None, numDescansos=None):
        try:
            viaje = dfviaje['Viaje'].iloc[0]
            fecha_dummy = datetime(1900, 1, 1, 1, 1)
            count_hitos = 0

            if numHoteles is None and 'catalogo hoteles' in self.datos['usuario']['funciones']:
                numHoteles = 0

                if self.CdfConfiguracion['FacHoteles'].values[0] == 0:
                    numHoteles = 0

                # print("Aqui numHoteles subida", numHoteles)
                #if numHoteles is None or np.isnan(numHoteles):
                #    numHoteles = 0

            if verbose:
                print("calcula_ruta_subida funcion", viaje, tirada_origen, ":", origen,
                    "->", tirada_origen+1, ":", destino, tirada_origen, talla_vehiculo)

            # Variables de configuracion
            duracion_comida = int(self.CdfConfiguracion['DuracionComida'].values[0]) * 60
            duracion_hotel = int(self.CdfConfiguracion['DuracionHotel'].values[0]) * 60
            hora_comida = self.CdfConfiguracion['HoraComida'].values[0].split(':')
            intervalo_descanso = int(self.CdfConfiguracion['IntervaloDescanso'].values[0]) * 60
            # intervalo_descanso = 120*60
            duracion_descanso = int(self.CdfConfiguracion['DuracionDescanso'].values[0]) * 60

            tiempo_servicio = 0
            tiempo_servicio_numpy = np.timedelta64(tiempo_servicio, 'm')

            tiempo_servicio_origen = int(self.calcula_tiempo_servicio(dfviaje, tirada_origen))
            tiempo_servicio__origen_numpy = np.timedelta64(tiempo_servicio_origen, 'm')

            # obtiene la fecha de entrega del origen
            fecha_entrega_origen = dfviaje.loc[(dfviaje['Tirada'] == tirada_origen), 'FechaEntregaPedido'].unique()[0]
            fecha_entrega_origen_datetime = datetime.utcfromtimestamp(fecha_entrega_origen.astype('O') / 1e9)

            fecha_salida_viaje = dfviaje.loc[(dfviaje['Tirada'] == tirada_origen), 'FechaSalida'].unique()[0]
            fecha_salida_viaje_datetime = datetime.utcfromtimestamp(fecha_salida_viaje.astype('O') / 1e9)

            if verbose:
                print("fecha_entrega_origen_datetime", fecha_entrega_origen_datetime)
                print("fecha_salida_viaje_datetime", fecha_salida_viaje_datetime)

            # obtiene la fecha de salida del origen
            fecha_salida_origen = fecha_entrega_origen + tiempo_servicio__origen_numpy
            dfviaje.loc[(dfviaje['Tirada'] == tirada_origen), 'FechaSalidaPedido'] = fecha_salida_origen

            if verbose:
                print("fecha_salida_origen", fecha_salida_origen)

            # obtiene la fecha de comida
            fecha_comida = fecha_entrega_origen_datetime
            fecha_comida = fecha_comida.replace(hour=int(hora_comida[0]), minute=int(hora_comida[1]), second=0)
            # Si la fecha de comida es anterior a la salida del origen
            if fecha_comida > fecha_entrega_origen_datetime:
                # La siguiente comida caería el día anteriro
                fecha_comida = fecha_comida + timedelta(days=-1)

            fecha_hotel = fecha_entrega_origen_datetime
            fecha_hotel = fecha_hotel.replace(hour=21, minute=1, second=0)
            if verbose:
                print("fecha hotel inicial", fecha_hotel)
            # Si la fecha de comida es anterior a la salida del origen
            if fecha_hotel > fecha_entrega_origen_datetime:
                # El siguiente hotel caería el día anterior
                fecha_hotel = fecha_hotel + timedelta(days=-1)
                if verbose:
                    print("fecha hotel movida", fecha_hotel)

            # Por ahora el ultimo hito fue la salida del destino
            ultimo_hito = fecha_entrega_origen_datetime

            # Se calcula el tiempo de ruta sin descansos ni comidas
            arco = pd.DataFrame(arco)
            campo_factor = 'FactorSubida' + talla_vehiculo

            factor_ruta = self.datos['factor_ruta']
            if factor_ruta is None:
                factor_ruta = 0
            if math.isnan(factor_ruta):
                factor_ruta = 0

            tiempo_ruta = arco.TIEMPO.unique()[0] * (1 + (float(factor_ruta) / 100))
            tiempo_arco = tiempo_ruta

            if verbose:
                print("tiempo_ruta", tiempo_ruta)
                print(ultimo_hito, type(ultimo_hito))

            # Diccionario de prioridades segun tipo de ruta

            if tiempo_ruta >= self.CdfConfiguracion['IntervaloHotel'].values[0]*60 or (not numHoteles is None and numHoteles > 0):
                Prioridades = {'Descanso': 0, 'Comida': 2, 'Hotel': 3, 'Entrega': 1}
                Prioridades_index = ['Descanso', 'Entrega', 'Comida', 'Hotel']
            else:
                Prioridades = {'Descanso': 1, 'Comida': 3, 'Hotel': 0, 'Entrega': 2}
                Prioridades_index = ['Hotel', 'Descanso', 'Entrega', 'Comida']

            # Calcula la lista de hitos ....
            hitos = [0] * len(Prioridades)
            hitos_final = [0] * len(Prioridades)
            array_itinerario = pd.DataFrame(columns=self.datos["columns_itinerario"])
            while True:
                if not numHoteles is None:
                    if numHoteles <= 1:
                        duracion_hotel = 0
                if not numComidas is None:
                    if numComidas <= 0:
                        duracion_comida = 0
                if not numDescansos is None:
                    if numDescansos <= 0:
                        duracion_descanso = 0

                todoshitos = [numHoteles, numComidas, numDescansos]

                if self.CdfConfiguracion['FacDescansos'].values[0] == 1 and int(intervalo_descanso) != 0 and int(duracion_descanso) != 0:
                    if tiempo_ruta > intervalo_descanso and (numDescansos is None or numDescansos > 0):
                        hitos_final[Prioridades['Descanso']] = ultimo_hito - timedelta(seconds=int(intervalo_descanso))
                        hitos[Prioridades['Descanso']] = hitos_final[Prioridades['Descanso']] - timedelta(seconds=int(duracion_descanso))
                    else:
                        hitos[Prioridades['Descanso']] = fecha_dummy
                        hitos_final[Prioridades['Descanso']] = fecha_dummy
                else:
                    hitos[Prioridades['Descanso']] = fecha_dummy
                    hitos_final[Prioridades['Descanso']] = fecha_dummy

                if self.CdfConfiguracion['FacComidas'].values[0] == 1 and int(duracion_comida) != 0:
                    hitos[Prioridades['Comida']] = fecha_comida
                    hitos_final[Prioridades['Comida']] = fecha_comida + timedelta(seconds=int(duracion_comida))
                else:
                    hitos[Prioridades['Comida']] = fecha_dummy
                    hitos_final[Prioridades['Comida']] = fecha_dummy

                if todoshitos == [None, None, None]:
                    hitos_final[Prioridades['Entrega']] = fecha_entrega_origen_datetime - timedelta(seconds=tiempo_ruta)
                elif (todoshitos == [0, None, None]) or (todoshitos == [0, 0, None]) or (todoshitos == [0, 0, 0]):
                    if count_hitos >0:
                        hitos_final[Prioridades['Entrega']] = min([fecha_entrega_origen_datetime - timedelta(seconds=tiempo_ruta),
                                                                   datetime.strptime(array_itinerario.iloc[-1, array_itinerario.columns.get_loc('Inicio')], '%d/%m/%Y %H:%M')])
                    else:
                        hitos_final[Prioridades['Entrega']] = fecha_entrega_origen_datetime - timedelta(seconds=tiempo_ruta)
                else:
                    hitos_final[Prioridades['Entrega']] = fecha_dummy
                hitos[Prioridades['Entrega']] = hitos_final[Prioridades['Entrega']]

                if self.CdfConfiguracion['FacHoteles'].values[0] == 1 and int(duracion_hotel) != 0:
                    hitos[Prioridades['Hotel']] = fecha_hotel
                    hitos_final[Prioridades['Hotel']] = fecha_hotel + timedelta(seconds=int(duracion_hotel))

                    if hitos_final[Prioridades['Hotel']].time() > fecha_entrega_origen_datetime.time():
                        hitos_final[Prioridades['Hotel']] = hitos_final[Prioridades['Hotel']].replace(hour=fecha_entrega_origen_datetime.hour, minute=fecha_entrega_origen_datetime.minute)

                else:
                    hitos[Prioridades['Hotel']] = fecha_dummy
                    hitos_final[Prioridades['Hotel']] = fecha_dummy

                hitos_eliminar = 0
                hitos_eliminar_ac = 0
                while True:
                    hitos = hitos[hitos_eliminar:]
                    hitos_final = hitos_final[hitos_eliminar:]

                    if verbose:
                        print("hitos", hitos)
                        print("hitos_final", hitos_final)

                    indice_hito, proximo_hito = max(enumerate(hitos_final), key=operator.itemgetter(1))
                    hito_comp = [0]
                    hito_comp[0] = hitos[indice_hito]
                    hito_comp.extend(hitos_final[indice_hito + 1:])

                    if verbose:
                        print("hito_comp = final vs inicio otros", hito_comp)
                        print("proximo_hito_final", proximo_hito, "tipo: ", Prioridades_index[hitos_eliminar_ac + indice_hito])

                    if Prioridades_index[hitos_eliminar_ac + indice_hito] == 'Comida':
                        self.datos['bandera_comida'] = True

                    indice_hito_f, proximo_hito_f = max(enumerate(hito_comp), key=operator.itemgetter(1))

                    if indice_hito_f == 0 or len(hito_comp) < 2:
                        hito_siguiente = hitos_eliminar_ac + indice_hito
                        tipo_hito = Prioridades_index[hito_siguiente]
                        if verbose:
                            print("El hito fue", Prioridades_index[hito_siguiente])
                        break
                    else:
                        if verbose:
                            print("El hito no finalizo antes del inicio de los otros, se va a eliminar los menores = prioridad")
                        hitos_eliminar = indice_hito + 1
                        hitos_eliminar_ac = hitos_eliminar_ac + hitos_eliminar

                tipo_hito = Prioridades_index[hito_siguiente]

                if verbose:
                    print("hitos restantes", hitos)

                if tipo_hito != 'Entrega':


                    dataframe_hito = self.crea_diccionario_itinerario(tipo_hito, hitos[indice_hito].strftime('%d/%m/%Y %H:%M'),
                                                                 hitos_final[indice_hito].strftime('%d/%m/%Y %H:%M'), viaje, tirada_origen)

                    array_itinerario = array_itinerario.append(dataframe_hito, ignore_index=True,sort=False)
                    count_hitos += 1

                    if verbose:
                        print(array_itinerario)
                        print("Agregue " + tipo_hito, "Fecha: = ", hitos[indice_hito])
                    ultimo_hito = hitos_final[indice_hito]
                    if verbose:
                        print("fIN " + tipo_hito, "Fecha: = ", ultimo_hito)
                    ultimo_hito = hitos[indice_hito]
                    if verbose:
                        print("Tiempo ruta ant: ", tiempo_ruta)
                    tiempo_ruta = tiempo_ruta + (hitos_final[indice_hito] - hitos[indice_hito]).seconds
                    if verbose:
                        print("Tiempo ruta post: ", tiempo_ruta)
                    if tipo_hito == 'Descanso':
                        if not numDescansos is None:
                            numDescansos = numDescansos - 1
                    if tipo_hito == 'Comida':
                        if not numComidas is None:
                            numComidas = numComidas - 1
                        self.datos['bandera_comida'] = False
                    if tipo_hito == 'Hotel':
                        if not numHoteles is None:
                            numHoteles = numHoteles - 1
                        self.datos['bandera_hotel'] = False

                    tiempo_restante = tiempo_ruta - (fecha_entrega_origen_datetime - ultimo_hito).total_seconds()
                    if verbose:
                        print("fecha_entrega_origen_datetime - ultimo_hito", fecha_entrega_origen_datetime - ultimo_hito)
                        print("tiempo_restante", tiempo_restante)
                    #if tiempo_restante < self.CdfConfiguracion['IntervaloHotel'].values[0]*60:
                    #    Prioridades = {'Descanso': 1, 'Comida': 3, 'Hotel': 0, 'Entrega': 2}
                    #    Prioridades_index = ['Hotel', 'Descanso', 'Entrega', 'Comida']

                else:

                    break

                if ultimo_hito < (fecha_comida + timedelta(seconds=int(duracion_comida))):
                    fecha_comida = fecha_comida + timedelta(days=-1)

                if ultimo_hito < (fecha_hotel + timedelta(seconds=int(duracion_hotel))):
                    fecha_hotel = fecha_hotel + timedelta(days=-1)

                hitos = [0] * len(Prioridades)
                hitos_final = [0] * len(Prioridades)

            if not dummy:
                delta_ruta = pd.Timedelta(timedelta(seconds=tiempo_ruta))
                if count_hitos > 0:
                    fecha_actualizacion = min([fecha_entrega_origen_datetime - timedelta(seconds=tiempo_ruta),
                                              datetime.strptime(array_itinerario.iloc[-1, array_itinerario.columns.get_loc('Inicio')], '%d/%m/%Y %H:%M')])
                else:
                    fecha_actualizacion = fecha_entrega_origen_datetime - timedelta(seconds=tiempo_ruta)
                print("fecha_actualizacion", fecha_actualizacion, fecha_entrega_origen_datetime - timedelta(seconds=tiempo_ruta), )
                fecha_salida_viaje_datetime = fecha_actualizacion

                datetime_pandas = pd.to_datetime(fecha_actualizacion)
                dfviaje.loc[:, 'FechaSalida'] = datetime_pandas

            array_itinerario = self.inserta_hitos_trayectos(array_itinerario, fecha_salida_viaje_datetime,
                                                       fecha_entrega_origen_datetime, viaje, 0, 'Subida', tiempo_arco)

            itinerarioJson = json.loads(array_itinerario.to_json(orient='records'))
            viaje = json.loads(dfviaje.to_json(orient='records'))
            arco = json.loads(arco.to_json(orient='records'))
            itinerario = {
                'viaje' : viaje[0]['Viaje'],
                'origen' : arco[0]['ORIGEN'],
                'destino' : arco[0]['DESTINO'],
                'itinerario' : itinerarioJson,
                'tipo_ruta' : tipo_ruta
            }
            # print(itinerario)
            self.ItinFull.append(itinerario)
            return itinerario

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            error = '{} - {} - linea {}  ({})'.format(exc_type, fname, exc_tb.tb_lineno, str(e))
            logger.error(error)
            return False, 0, 0


    def calcula_tiempo_servicio(self, dfviaje, tirada):
        try:
            #dftirada = dfviaje.loc[dfviaje['Tirada'] == tirada, ['TiemServicio', 'TiemDescarga']]
            dftirada = dfviaje.loc[dfviaje['Tirada'] == tirada, ['TiemServicioTirada', 'TiemDescarga']]


            tiempo_descarga = dftirada['TiemDescarga'].sum(axis=0)
            #tiempo_servicio = dftirada['TiemServicio'].iloc[0]
            tiempo_servicio = dftirada['TiemServicioTirada'].iloc[0]

            tiempo_servicio = tiempo_servicio + tiempo_descarga

            return tiempo_servicio

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            error = '{} - {} - linea {}  ({})'.format(exc_type, fname, exc_tb.tb_lineno, str(e))
            logger.error(error)
            return False, 0, 0

    #Función que crea un diccionario y dataframe del itinerario
    def crea_diccionario_itinerario(self, Actividad, Inicio, Fin, viaje, tirada, idActividad=0):

       try:
            #Creacion y llenado de valores
            dict_hito = dict()
            dict_hito['Viaje'] = viaje
            dict_hito['TiradaPlan'] = tirada
            dict_hito['Actividad'] = Actividad
            dict_hito['Inicio'] = Inicio
            dict_hito['Fin'] = Fin
            dict_hito['idActividad'] = idActividad

            # Pasa a dataframe
            dataframe_hito = pd.DataFrame([dict_hito])

            return dataframe_hito

       except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            error = '{} - {} - linea {}  ({})'.format(exc_type, fname, exc_tb.tb_lineno, str(e))
            logger.error(error)
            return False, False, 0
    def inserta_hitos_trayectos(self, array_itinerario, fecha_salida_origen_datetime, fecha_entrega_destino_datetime, viaje, tirada_origen, tipo_ruta, tiempo_arco):

        '''
        Inputs
            Array_itinerario: dataframe con los hitos de comidas, hoteles, entregas y descansos
            ej:
            Actividad      Fin                      Inicio                  TiradaPlan      Viaje       idActividad     idPlan
            Hotel          25 / 04 / 2019 07: 01    24 / 04 / 2019 21: 01   3               59570       0               2930
            Descanso       25 / 04 / 2019 12: 31    25 / 04 / 2019 12: 01   3               59570       0               2930
            Comida         25 / 04 / 2019 15: 00    25 / 04 / 2019 14: 00   3               59570       0               2930
            fecha_salida_origen_datetime: fecha de salida del origen
            fecha_entrega_destino_datetime: fecha de entrega en el destino
            viaje = id viaje
            tirada_origen: tirada origen del trayecto
            idplan: id plan del viaje a modificar
            tipo_ruta: 'Subida, 'Intermedio' o 'Retorno'
            tiempo_arco: tiempo (en segundos) del arco entre el origen y el destino

        Output
            Array_itinerario: dataframe con los hitos de comidas, hoteles, entregas, descansos, esperas y trayectos
        '''
        try:

            # Inserta hitos de trayecto y espera en el trayecto

            tipo_hito = 'Trayecto'

            # Obtiene el itinerario actual (hoteles, comidas, descanso) y lo ordena cronológicamente
            array_itinerario['Inicio'] = pd.to_datetime(array_itinerario['Inicio'], dayfirst=True)
            array_itinerario['Fin'] = pd.to_datetime(array_itinerario['Fin'], dayfirst=True)
            array_itinerario.sort_values(['Inicio', 'Fin'], ascending=[True, True], inplace=True)

            # Copia el itinerario
            array_itinerario_ret = array_itinerario.copy()

            # Inicializa la variable de trayecto (contabiliza las horas de trayecto)
            tiempo_trayeto = timedelta(seconds=0)
            # Timedelta del tiempo del arco, ya multiplicado por los factores
            tiempo_arco = timedelta(seconds=tiempo_arco)

            # Si no está vacio el itinerario
            if not array_itinerario.empty:

                # Se obtiene el inicio del primer hito (se comparará con la salida del "origen" (destino anterior)
                hito_ini = array_itinerario.iloc[0, array_itinerario.columns.get_loc('Inicio')].to_pydatetime()

                # Si la fecha de salida anterior es menor al hito (como debería de ser)
                if fecha_salida_origen_datetime < hito_ini:

                    # Inserta hito de trayecto o espera

                    # Contabiliza el tiempo de trayecto
                    tiempo_trayeto = tiempo_trayeto + hito_ini - fecha_salida_origen_datetime
                    # print("tiempo_arco", tiempo_arco, "tiempo_trayeto", tiempo_trayeto)

                    # Compara el tiempo de trayecto actual, vs el arco definido
                    if tiempo_trayeto > tiempo_arco and tipo_hito == 'Trayecto':
                        # Si ya es mayor, a partir de aqui se pondrán puras "Esperas"
                        tipo_hito = 'Espera'

                        # Se calcula la fracción de trayecto y la fracción de espera
                        frontera_espera = hito_ini - (tiempo_trayeto - tiempo_arco)
                        # print(frontera_espera, type(frontera_espera), 'frontera_espera')

                        # Inserta hitos de trayecto y espera "de frontera"
                        dataframe_hito = self.crea_diccionario_itinerario('Trayecto', fecha_salida_origen_datetime, frontera_espera, viaje, tirada_origen + 1)
                        array_itinerario_ret = array_itinerario_ret.append(dataframe_hito, ignore_index=True, sort=False)
                        dataframe_hito = self.crea_diccionario_itinerario('Espera', frontera_espera, hito_ini, viaje, tirada_origen + 1)
                        array_itinerario_ret = array_itinerario_ret.append(dataframe_hito, ignore_index=True, sort=False)

                    else:
                        # Si el tiempo de trayecto actual es menor al arco inserta trayecto, si es mayor inserta espera
                        dataframe_hito = self.crea_diccionario_itinerario(tipo_hito, fecha_salida_origen_datetime, hito_ini, viaje, tirada_origen + 1)
                        array_itinerario_ret = array_itinerario_ret.append(dataframe_hito, ignore_index=True, sort=False)

                # Para los hitos subsecuentes se realiza el mismo proceso (comparativa del fin del 1er hito y el inicio del 2do hito)
                for i in range(array_itinerario.shape[0]-1):

                    hito_fin = array_itinerario.iloc[i, array_itinerario.columns.get_loc('Fin')].to_pydatetime()
                    hito2_ini = array_itinerario.iloc[i+1, array_itinerario.columns.get_loc('Inicio')].to_pydatetime()

                    if hito_fin < hito2_ini:

                        tiempo_trayeto = tiempo_trayeto + hito2_ini - hito_fin
                        # print("tiempo_arco", tiempo_arco, "tiempo_trayeto", tiempo_trayeto)

                        if tiempo_trayeto > tiempo_arco and tipo_hito == 'Trayecto':
                            tipo_hito = 'Espera'
                            frontera_espera = hito2_ini - (tiempo_trayeto - tiempo_arco)
                            # print(frontera_espera, type(frontera_espera), 'frontera_espera')
                            dataframe_hito = self.crea_diccionario_itinerario('Trayecto', hito_fin, frontera_espera, viaje, tirada_origen + 1)
                            array_itinerario_ret = array_itinerario_ret.append(dataframe_hito, ignore_index=True, sort=False)
                            dataframe_hito = self.crea_diccionario_itinerario('Espera', frontera_espera, hito2_ini, viaje, tirada_origen + 1)
                            array_itinerario_ret = array_itinerario_ret.append(dataframe_hito, ignore_index=True, sort=False)

                        else:
                            dataframe_hito = self.crea_diccionario_itinerario(tipo_hito, hito_fin, hito2_ini, viaje, tirada_origen + 1)
                            array_itinerario_ret = array_itinerario_ret.append(dataframe_hito, ignore_index=True, sort=False)

                # Para el hito final se realiza el mismo proceso, (comparativa del fin del hito vs entrega en el destino)
                hito2_fin = array_itinerario.iloc[-1, array_itinerario.columns.get_loc('Fin')].to_pydatetime()

                if fecha_entrega_destino_datetime > hito2_fin:

                    tiempo_trayeto = tiempo_trayeto + fecha_entrega_destino_datetime - hito2_fin
                    # print("tiempo_arco", tiempo_arco, "tiempo_trayeto", tiempo_trayeto)

                    if tiempo_trayeto > tiempo_arco and tipo_hito == 'Trayecto':
                        tipo_hito = 'Espera'
                        frontera_espera = fecha_entrega_destino_datetime - (tiempo_trayeto - tiempo_arco)
                        # print(frontera_espera, type(frontera_espera), 'frontera_espera')
                        dataframe_hito = self.crea_diccionario_itinerario('Trayecto', hito2_fin, frontera_espera, viaje, tirada_origen + 1)
                        array_itinerario_ret = array_itinerario_ret.append(dataframe_hito, ignore_index=True, sort=False)
                        dataframe_hito = self.crea_diccionario_itinerario('Espera', frontera_espera, fecha_entrega_destino_datetime, viaje, tirada_origen + 1)
                        array_itinerario_ret = array_itinerario_ret.append(dataframe_hito, ignore_index=True, sort=False)

                    else:
                        dataframe_hito = self.crea_diccionario_itinerario(tipo_hito, hito2_fin, fecha_entrega_destino_datetime, viaje, tirada_origen + 1)
                        array_itinerario_ret = array_itinerario_ret.append(dataframe_hito, ignore_index=True,sort=False)

            # Si el trayecto no tiene hitos asociados
            else:

                # Se inserta el trayecto (o espera) entre la salida del origen y la entrega del destino
                if fecha_salida_origen_datetime < fecha_entrega_destino_datetime:

                    tiempo_trayeto = tiempo_trayeto + fecha_entrega_destino_datetime - fecha_salida_origen_datetime
                    # print("tiempo_arco", tiempo_arco, "tiempo_trayeto", tiempo_trayeto)

                    if tiempo_trayeto > tiempo_arco and tipo_hito == 'Trayecto':
                        tipo_hito = 'Espera'
                        frontera_espera = fecha_entrega_destino_datetime - (tiempo_trayeto - tiempo_arco)
                        # print(frontera_espera, 'frontera_espera')
                        dataframe_hito = self.crea_diccionario_itinerario('Trayecto', fecha_salida_origen_datetime, frontera_espera, viaje, tirada_origen + 1)
                        array_itinerario_ret = array_itinerario_ret.append(dataframe_hito, ignore_index=True, sort=False)
                        dataframe_hito = self.crea_diccionario_itinerario('Espera', frontera_espera, fecha_entrega_destino_datetime, viaje, tirada_origen + 1)
                        array_itinerario_ret = array_itinerario_ret.append(dataframe_hito, ignore_index=True, sort=False)

                    else:
                        dataframe_hito = self.crea_diccionario_itinerario('Trayecto', fecha_salida_origen_datetime, fecha_entrega_destino_datetime, viaje, tirada_origen + 1)
                        array_itinerario_ret = array_itinerario_ret.append(dataframe_hito, ignore_index=True, sort=False)

            # Se inserta el hito de aterrizaje para el ultimo hito del retorno
            if tipo_ruta == 'Retorno':
                dataframe_hito = self.crea_diccionario_itinerario('Aterrizaje', fecha_entrega_destino_datetime,
                                                             fecha_entrega_destino_datetime, viaje,
                                                             tirada_origen + 1)
                array_itinerario_ret = array_itinerario_ret.append(dataframe_hito, ignore_index=True, sort=False)

            # Se inserta el hito de despegue para el primer hito de subida
            if tipo_ruta == 'Subida':
                dataframe_hito = self.crea_diccionario_itinerario('Despegue', fecha_salida_origen_datetime,
                                                                 fecha_salida_origen_datetime, viaje,
                                                                 tirada_origen + 1)

                array_itinerario_ret = array_itinerario_ret.append(dataframe_hito, ignore_index=True, sort=False)

            # Se reacomodan cronologicamente los hitos
            array_itinerario_ret.sort_values(['Inicio', 'Fin'], ascending=[True, True],inplace=True)
            array_itinerario_ret.reset_index(inplace=True, drop=True)

            # Proceso para eliminar los hitos (Espera - Descanso - Espera) e insertar una Espera larga ....
            # Se obtienen los indices de los descansos
            idx_descansos = array_itinerario_ret[array_itinerario_ret['Actividad'] == 'Descanso'].index.values

            # Para cada descanso
            for i in range(len(idx_descansos)):

                # Se analizarán los hitos anterior y posterior al descanso
                ant_des = idx_descansos[i]-1
                pos_des = idx_descansos[i]+1

                # Si el hito anterior y posterior son espera, hay que modificar (Espera - Descanso - Espera) por (Espera grandota)
                if array_itinerario_ret.iloc[ant_des, array_itinerario_ret.columns.get_loc('Actividad')] == 'Espera' and \
                        array_itinerario_ret.iloc[pos_des, array_itinerario_ret.columns.get_loc('Actividad')] == 'Espera':

                    array_itinerario_ret.iloc[ant_des, array_itinerario_ret.columns.get_loc('Fin')] = array_itinerario_ret.iloc[pos_des, array_itinerario_ret.columns.get_loc('Fin')]
                    array_itinerario_ret.drop(array_itinerario_ret.index[ant_des+1:pos_des+1], inplace=True)
                    array_itinerario_ret.reset_index(inplace=True, drop=True)

                    idx_descansos[idx_descansos > i] = idx_descansos[idx_descansos > i] - 2

            # Pasa el dataframe de itinerarios a string
            array_itinerario_ret['Inicio'] = array_itinerario_ret['Inicio'].dt.strftime('%d/%m/%Y %H:%M')
            array_itinerario_ret['Fin'] = array_itinerario_ret['Fin'].dt.strftime('%d/%m/%Y %H:%M')

            return array_itinerario_ret

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            error = '{} - {} - linea {}  ({})'.format(exc_type, fname, exc_tb.tb_lineno, str(e))
            logger.error(error)
            return False, False, 0