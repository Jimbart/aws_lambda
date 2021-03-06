
import pandas as pd

diccionario_campos_viajes =\
    [
        {'Campo': 'Viaje', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'Status', 'Plan_Real': 'Real', 'Tipo': 'FLOAT'},
        {'Campo': 'StatusAnterior', 'Plan_Real': 'Real', 'Tipo': 'FLOAT'},
        {'Campo': 'P_Nombre_Sesion', 'Plan_Real': 'Plan', 'Tipo': 'STRING'},
        {'Campo': 'P_Token', 'Plan_Real': 'Plan', 'Tipo': 'STRING'},
        {'Campo': 'P_Fecha_Sesion', 'Plan_Real': 'Plan', 'Tipo': 'TIMESTAMP'},
        {'Campo': 'P_Fecha_Autorizacion', 'Plan_Real': 'Plan', 'Tipo': 'TIMESTAMP'},
        {'Campo': 'P_Fecha_Predespegue', 'Plan_Real': 'Plan', 'Tipo': 'TIMESTAMP'},
        {'Campo': 'FechaSalidaViajePlan', 'Plan_Real': 'Plan', 'Tipo': 'TIMESTAMP'},
        {'Campo': 'FechaRetornoViajePlan', 'Plan_Real': 'Plan', 'Tipo': 'TIMESTAMP'},
        {'Campo': 'P_Duracion_Traslado', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'P_Duracion_Servicio', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'P_Duracion_Alimentos', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'P_Duracion_Pernocta', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'P_Duracion_EsperaTrayecto', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'P_Duracion_Descanso', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'P_Duracion_Combustible', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'P_Duracion_Ferry', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'P_Duracion_Liquidacion', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'P_Duracion_Ajuste', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'P_Duracion_Total_Viaje', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'idEmpresa', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'P_Empresa', 'Plan_Real': 'Plan', 'Tipo': 'STRING'},
        {'Campo': 'Planeador', 'Plan_Real': 'Plan', 'Tipo': 'STRING'},
        {'Campo': 'P_Autorizador', 'Plan_Real': 'Plan', 'Tipo': 'STRING'},
        {'Campo': 'Despegador', 'Plan_Real': 'Plan', 'Tipo': 'STRING'},
        {'Campo': 'Tiradas', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'Pedidos', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'Valor', 'Plan_Real': 'Plan', 'Tipo': 'FLOAT'},
        {'Campo': 'Piezas', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'Peso', 'Plan_Real': 'Plan', 'Tipo': 'FLOAT'},
        {'Campo': 'Volumen', 'Plan_Real': 'Plan', 'Tipo': 'FLOAT'},
        {'Campo': 'Cedi', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'Cedis', 'Plan_Real': 'Plan', 'Tipo': 'STRING'},
        {'Campo': 'P_Cedis_Intermedio', 'Plan_Real': 'Plan', 'Tipo': 'STRING'},
        {'Campo': 'CediAterrizaje', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'P_Cedis_Aterriza', 'Plan_Real': 'Plan', 'Tipo': 'STRING'},
        {'Campo': 'P_Num_CEDIS', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'P_Numero_Regiones', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'P_Numero_Entidades', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'P_Numero_Destinos_TR1', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'P_Numero_Destinos_TR2', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'TipoVehiculo', 'Plan_Real': 'Plan', 'Tipo': 'STRING'},
        {'Campo': 'P_Activ_Traslado', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'P_Activ_Servicio', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'P_Activ_Alimentos', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'P_Activ_Pernocta', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'P_Activ_EsperaTrayecto', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'P_Activ_Descanso', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'P_Activ_Combustible', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'P_Activ_Ferry', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'P_Activ_Liquidacion', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'P_Activ_Ajuste', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'P_Activ_Total', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'CostoCombustiblePlan', 'Plan_Real': 'Plan', 'Tipo': 'FLOAT'},
        {'Campo': 'CostoCasetaPlan', 'Plan_Real': 'Plan', 'Tipo': 'FLOAT'},
        {'Campo': 'CostoComidasPlan', 'Plan_Real': 'Plan', 'Tipo': 'FLOAT'},
        {'Campo': 'CostoHotelPlan', 'Plan_Real': 'Plan', 'Tipo': 'FLOAT'},
        {'Campo': 'CostoFerryPlan', 'Plan_Real': 'Plan', 'Tipo': 'FLOAT'},
        {'Campo': 'CostoThermoPlan', 'Plan_Real': 'Plan', 'Tipo': 'FLOAT'},
        {'Campo': 'CostoOtrosPlan', 'Plan_Real': 'Plan', 'Tipo': 'FLOAT'},
        {'Campo': 'CostoTotalPlan', 'Plan_Real': 'Plan', 'Tipo': 'FLOAT'},
        {'Campo': 'Km', 'Plan_Real': 'Plan', 'Tipo': 'FLOAT'},
        {'Campo': 'Co2', 'Plan_Real': 'Plan', 'Tipo': 'FLOAT'},
        {'Campo': 'LitrosCombustible', 'Plan_Real': 'Plan', 'Tipo': 'FLOAT'},
        {'Campo': 'LitrosThermo', 'Plan_Real': 'Plan', 'Tipo': 'FLOAT'},
        {'Campo': 'Rendimiento', 'Plan_Real': 'Plan', 'Tipo': 'FLOAT'},
        {'Campo': 'RendimientoThermo', 'Plan_Real': 'Plan', 'Tipo': 'FLOAT'},
        {'Campo': 'PesoPermitido', 'Plan_Real': 'Plan', 'Tipo': 'FLOAT'},
        {'Campo': 'VolumenPermitido', 'Plan_Real': 'Plan', 'Tipo': 'FLOAT'},
        {'Campo': 'VolumenSecoPermitido', 'Plan_Real': 'Plan', 'Tipo': 'FLOAT'},
        {'Campo': 'VolumenFrioPermitido', 'Plan_Real': 'Plan', 'Tipo': 'FLOAT'},
        {'Campo': 'P_Ocupacion_Peso', 'Plan_Real': 'Plan', 'Tipo': 'FLOAT'},
        {'Campo': 'OcupacionVolumen', 'Plan_Real': 'Plan', 'Tipo': 'FLOAT'},
        {'Campo': 'P_Ocupacion_Volumen_Seco', 'Plan_Real': 'Plan', 'Tipo': 'FLOAT'},
        {'Campo': 'P_Ocupacion_Volumen_Frio', 'Plan_Real': 'Plan', 'Tipo': 'FLOAT'},
        {'Campo': 'FechaSalidaReal', 'Plan_Real': 'Real', 'Tipo': 'TIMESTAMP'},
        {'Campo': 'FechaRetornoReal', 'Plan_Real': 'Real', 'Tipo': 'TIMESTAMP'},
        {'Campo': 'R_Tiradas_Efectuadas', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'R_Duracion_Traslado', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'R_Duracion_Servicio', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'R_Duracion_Alimentos', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'R_Duracion_Pernocta', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'R_Duracion_EsperaServicio', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'R_Duracion_EsperaTrayecto', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'R_Duracion_Descanso', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'R_Duracion_Combustible', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'R_Duracion_Ferry', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'R_Duracion_Liquidacion', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'R_Duracion_Ajuste', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'R_Duracion_Total_Viaje', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'ProveedorFlete', 'Plan_Real': 'Real', 'Tipo': 'STRING'},
        {'Campo': 'OperadorReal', 'Plan_Real': 'Real', 'Tipo': 'STRING'},
        {'Campo': 'R_Primer_Entrega_Region', 'Plan_Real': 'Real', 'Tipo': 'STRING'},
        {'Campo': 'R_Primer_Entrega_Entidad', 'Plan_Real': 'Real', 'Tipo': 'STRING'},
        {'Campo': 'R_Primer_Entrega_Ciudad', 'Plan_Real': 'Real', 'Tipo': 'STRING'},
        {'Campo': 'R_Primer_Entrega_Destino_TR1', 'Plan_Real': 'Real', 'Tipo': 'STRING'},
        {'Campo': 'R_Primer_Entrega_Latitud_Logitud_TR1', 'Plan_Real': 'Real', 'Tipo': 'STRING'},
        {'Campo': 'R_Vehiculo_Placas', 'Plan_Real': 'Real', 'Tipo': 'STRING'},
        {'Campo': 'R_Numero_Economico', 'Plan_Real': 'Real', 'Tipo': 'INTEGER'},
        {'Campo': 'R_Viaje_salio_a_Tiempo', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'R_Viaje_Respeto_ruta', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'R_Registro_Despegue_Geocerca', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'R_Registro_Aterriza_Geocerca', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'R_Contesto_Encuesta_Viaje', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'R_Registro_Liquidacion', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'R_Recabo_Firma_Digital_Liquidacion', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'R_Buen_Uso', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'R_Actividad_Traslado', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'R_Actividad_Servicio', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'R_Actividad_Alimentos', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'R_Actividad_Pernocta', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'R_Actividad_EsperaServicio', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'R_Actividad_EsperaTrayecto', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'R_Actividad_Descanso', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'R_Actividad_Combustible', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'R_Actividad_Ferry', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'R_Actividad_Liquidacion', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'R_Actividad_Ajuste', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'R_Actividad_Total', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'R_Costo_Combustible', 'Plan_Real': 'Real', 'Tipo': 'FLOAT'},
        {'Campo': 'R_Costo_Casetas', 'Plan_Real': 'Real', 'Tipo': 'FLOAT'},
        {'Campo': 'R_Costo_Alimentos', 'Plan_Real': 'Real', 'Tipo': 'FLOAT'},
        {'Campo': 'R_Costo_Pernocta', 'Plan_Real': 'Real', 'Tipo': 'FLOAT'},
        {'Campo': 'R_Costo_Ferries', 'Plan_Real': 'Real', 'Tipo': 'FLOAT'},
        {'Campo': 'R_Costo_Otros_Gastos', 'Plan_Real': 'Real', 'Tipo': 'FLOAT'},
        {'Campo': 'R_Costo_Thermo', 'Plan_Real': 'Real', 'Tipo': 'FLOAT'},
        {'Campo': 'R_Costo_Flete', 'Plan_Real': 'Real', 'Tipo': 'FLOAT'},
        {'Campo': 'R_Costo_Total_Viaje', 'Plan_Real': 'Real', 'Tipo': 'FLOAT'},
        {'Campo': 'KmReal', 'Plan_Real': 'Real', 'Tipo': 'FLOAT'},
        {'Campo': 'Co2Real', 'Plan_Real': 'Mixto', 'Tipo': 'FLOAT'},
        {'Campo': 'RendimientoReal', 'Plan_Real': 'Mixto', 'Tipo': 'FLOAT'},
        {'Campo': 'Pof', 'Plan_Real': 'Real', 'Tipo': 'INTEGER'},
        {'Campo': 'PofTiempo', 'Plan_Real': 'Real', 'Tipo': 'INTEGER'},
        {'Campo': 'PofCompleto', 'Plan_Real': 'Real', 'Tipo': 'INTEGER'},
        {'Campo': 'PofSD', 'Plan_Real': 'Real', 'Tipo': 'INTEGER'},
        {'Campo': 'PofDocs', 'Plan_Real': 'Real', 'Tipo': 'INTEGER'},
        {'Campo': 'E_Fecha_Salida', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'E_Fecha_Retorno', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'E_Duracion_Traslado', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'E_Duracion_Servicio', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'E_Duracion_Alimentos', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'E_Duracion_Pernocta', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'E_Duracion_EsperaTrayecto', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'E_Duracion_Descanso', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'E_Duracion_Combustible', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'E_Duracion_Ferry', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'E_Duracion_Liquidacion', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'E_Duracion_Ajuste', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'E_Duracion_Total', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'E_Duracion_Viaje_Traslado_Subestimado', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'E_Duracion_Viaje_Traslado_Sobreestimado', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'E_Duracion_Viaje_Servicio_Subestimado', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'E_Duracion_Viaje_Servicio_Sobreestimado', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'E_Activ_Traslado', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'E_Activ_Servicio', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'E_Activ_Alimentos', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'E_Activ_Pernocta', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'E_Activ_Espera', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'E_Activ_Descanso', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'E_Activ_Combustible', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'E_Activ_Ferry', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'E_Activ_Liquidacion', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'E_Activ_Ajuste', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'E_Activ_Total', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'E_Costo_Combustible', 'Plan_Real': 'Mixto', 'Tipo': 'FLOAT'},
        {'Campo': 'E_Costo_Casetas', 'Plan_Real': 'Mixto', 'Tipo': 'FLOAT'},
        {'Campo': 'E_Costo_Alimentos', 'Plan_Real': 'Mixto', 'Tipo': 'FLOAT'},
        {'Campo': 'E_Costo_Pernocta', 'Plan_Real': 'Mixto', 'Tipo': 'FLOAT'},
        {'Campo': 'E_Costo_Ferries', 'Plan_Real': 'Mixto', 'Tipo': 'FLOAT'},
        {'Campo': 'E_Costo_Otros_Gastos', 'Plan_Real': 'Mixto', 'Tipo': 'FLOAT'},
        {'Campo': 'E_Costo_Total_Viaje', 'Plan_Real': 'Mixto', 'Tipo': 'FLOAT'},
        {'Campo': 'E_Km', 'Plan_Real': 'Mixto', 'Tipo': 'FLOAT'},
        {'Campo': 'E_Co2', 'Plan_Real': 'Mixto', 'Tipo': 'FLOAT'},
        {'Campo': 'E_Rendimiento', 'Plan_Real': 'Mixto', 'Tipo': 'FLOAT'},
        {'Campo': 'idSesion', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'idPlaneador', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'idAutorizador', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'idDespegador', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'idFleteraPlan', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'idFletera', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'idOperador', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'idVehiculoPlan', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'idVehiculo', 'Plan_Real': 'Mixto', 'Tipo': 'INTEGER'},
        {'Campo': 'InfraccionReglaPeso', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'InfraccionReglaVolumen', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'InfraccionReglaValor', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'InfraccionReglaAcceso', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'InfraccionDestinoDedicado', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'InfraccionVentanaEntrega', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'InfraccionEntrega1Dia', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'InfraccionDiasApertura', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'InfraccionCompatibilidad', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'Infraccion', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'FirmaSalida', 'Plan_Real': 'Real', 'Tipo': 'STRING'},
        {'Campo': 'FirmaLiquidaci??n', 'Plan_Real': 'Real', 'Tipo': 'STRING'},
        {'Campo': 'LatitudSalidaPlan', 'Plan_Real': 'Plan', 'Tipo': 'FLOAT'},
        {'Campo': 'LongitudSalidaPlan', 'Plan_Real': 'Plan', 'Tipo': 'FLOAT'},
        {'Campo': 'LatitudRetornoPlan', 'Plan_Real': 'Plan', 'Tipo': 'FLOAT'},
        {'Campo': 'LongitudRetornoPlan', 'Plan_Real': 'Plan', 'Tipo': 'FLOAT'},
        {'Campo': 'LatitudSalidaReal', 'Plan_Real': 'Real', 'Tipo': 'FLOAT'},
        {'Campo': 'LongitudSalidaReal', 'Plan_Real': 'Real', 'Tipo': 'FLOAT'},
        {'Campo': 'LatitudRetornoReal', 'Plan_Real': 'Real', 'Tipo': 'FLOAT'},
        {'Campo': 'LongitudRetornoReal', 'Plan_Real': 'Real', 'Tipo': 'FLOAT'},
        {'Campo': 'FueraDeGeocercaSalida', 'Plan_Real': 'Real', 'Tipo': 'FLOAT'},
        {'Campo': 'FueraDeGeocercaRetorno', 'Plan_Real': 'Real', 'Tipo': 'FLOAT'},
        {'Campo': 'ExactitudCoordenadasSalida', 'Plan_Real': 'Real', 'Tipo': 'FLOAT'},
        {'Campo': 'ExactitudCoordenadasRetorno', 'Plan_Real': 'Real', 'Tipo': 'FLOAT'},
        {'Campo': 'DisponibilidadCoordenadasSalida', 'Plan_Real': 'Real', 'Tipo': 'INTEGER'},
        {'Campo': 'DisponibilidadCoordenadasRetorno', 'Plan_Real': 'Real', 'Tipo': 'INTEGER'},
        {'Campo': 'FechaHoraAutomaticaSalida', 'Plan_Real': 'Real', 'Tipo': 'INTEGER'},
        {'Campo': 'FechaHoraAutomaticaRetorno', 'Plan_Real': 'Real', 'Tipo': 'INTEGER'},
        {'Campo': 'InfoRedSalida', 'Plan_Real': 'Real', 'Tipo': 'STRING'},
        {'Campo': 'InfoRedRetorno', 'Plan_Real': 'Real', 'Tipo': 'STRING'},
        {'Campo': 'IsMockedSalida', 'Plan_Real': 'Real', 'Tipo': 'INTEGER'},
        {'Campo': 'IsMockedRetorno', 'Plan_Real': 'Real', 'Tipo': 'INTEGER'},
        {'Campo': 'TipoDistribucion', 'Plan_Real': 'Plan', 'Tipo': 'STRING'}
    ]

diccionario_campos_nodos =\
    [
        {'Campo': 'P_Nombre_Sesion', 'Plan_Real':'Plan', 'Tipo':'STRING'},
        {'Campo': 'P_Fecha_Sesion', 'Plan_Real':'Plan', 'Tipo':'TIMESTAMP'},
        {'Campo': 'P_Fecha_Venta', 'Plan_Real':'Plan', 'Tipo':'TIMESTAMP'},
        {'Campo': 'P_Fecha_Autorizacion', 'Plan_Real':'Plan', 'Tipo':'TIMESTAMP'},
        {'Campo': 'P_Fecha_Predespegue', 'Plan_Real':'Plan', 'Tipo':'TIMESTAMP'},
        {'Campo': 'FechaSalidaViajePlan', 'Plan_Real':'Plan', 'Tipo':'TIMESTAMP'},
        {'Campo': 'FechaRetornoViajePlan', 'Plan_Real':'Plan', 'Tipo':'TIMESTAMP'},
        {'Campo': 'FechaEntregaDestino', 'Plan_Real':'Plan', 'Tipo':'TIMESTAMP'},
        {'Campo': 'FechaSalidaDestino', 'Plan_Real':'Plan', 'Tipo':'TIMESTAMP'},
        {'Campo': 'P_Fecha_Entrega_TR2', 'Plan_Real':'Plan', 'Tipo':'TIMESTAMP'},
        {'Campo': 'P_Duracion_Servicio', 'Plan_Real':'Plan', 'Tipo':'INTEGER'},
        {'Campo': 'P_Duracion_Traslado_Destino_Anterior', 'Plan_Real':'Plan', 'Tipo':'INTEGER'},
        {'Campo': 'P_Duracion_Traslado_Destino_Siguiente', 'Plan_Real':'Plan', 'Tipo':'INTEGER'},
        {'Campo': 'idEmpresa', 'Plan_Real':'Plan', 'Tipo':'INTEGER'},
        {'Campo': 'P_Empresa', 'Plan_Real':'Plan', 'Tipo':'STRING'},
        {'Campo': 'VendedorDestino', 'Plan_Real':'Plan', 'Tipo':'STRING'},
        {'Campo': 'Planeador', 'Plan_Real':'Plan', 'Tipo':'STRING'},
        {'Campo': 'P_Autorizador', 'Plan_Real':'Plan', 'Tipo':'STRING'},
        {'Campo': 'Despegador', 'Plan_Real':'Plan', 'Tipo':'STRING'},
        {'Campo': 'Pedidos', 'Plan_Real':'Plan', 'Tipo':'INTEGER'},
        {'Campo': 'Valor', 'Plan_Real':'Plan', 'Tipo':'FLOAT'},
        {'Campo': 'Piezas', 'Plan_Real':'Plan', 'Tipo':'INTEGER'},
        {'Campo': 'Peso', 'Plan_Real':'Plan', 'Tipo':'FLOAT'},
        {'Campo': 'Volumen', 'Plan_Real':'Plan', 'Tipo':'FLOAT'},
        {'Campo': 'Cedi', 'Plan_Real':'Plan', 'Tipo':'INTEGER'},
        {'Campo': 'Cedis', 'Plan_Real':'Plan', 'Tipo':'STRING'},
        {'Campo': 'P_Region_TR1', 'Plan_Real':'Plan', 'Tipo':'STRING'},
        {'Campo': 'P_SubRegion_TR1', 'Plan_Real':'Plan', 'Tipo':'STRING'},
        {'Campo': 'P_Entidad_TR1', 'Plan_Real':'Plan', 'Tipo':'STRING'},
        {'Campo': 'P_Zona_TR1', 'Plan_Real':'Plan', 'Tipo':'STRING'},
        {'Campo': 'P_Ciudad_TR1', 'Plan_Real':'Plan', 'Tipo':'STRING'},
        {'Campo': 'Destino', 'Plan_Real':'Plan', 'Tipo':'STRING'},
        {'Campo': 'Dedicado', 'Plan_Real':'Plan', 'Tipo':'INTEGER'},
        {'Campo': 'P_Latitud_Longitud_TR1', 'Plan_Real':'Plan', 'Tipo':'STRING'},
        {'Campo': 'DestinoTR2', 'Plan_Real':'Plan', 'Tipo':'STRING'},
        {'Campo': 'Viaje', 'Plan_Real':'Plan', 'Tipo':'INTEGER'},
        {'Campo': 'Status', 'Plan_Real':'Real', 'Tipo':'INTEGER'},
        {'Campo': 'StatusAnterior', 'Plan_Real':'Real', 'Tipo':'INTEGER'},
        {'Campo': 'TiradaPlan', 'Plan_Real':'Plan', 'Tipo':'INTEGER'},
        {'Campo': 'TipoVehiculo', 'Plan_Real':'Plan', 'Tipo':'STRING'},
        {'Campo': 'P_Costo_Total_Nodo', 'Plan_Real':'Plan', 'Tipo':'FLOAT'},
        {'Campo': 'R_Duracion_Servicio', 'Plan_Real':'Real', 'Tipo':'INTEGER'},
        {'Campo': 'R_Duracion_Traslado_Destino_Anterior', 'Plan_Real':'Real', 'Tipo':'INTEGER'},
        {'Campo': 'R_Duraci??n_Traslado_Destino_Siguiente', 'Plan_Real':'Real', 'Tipo':'INTEGER'},
        {'Campo': 'R_Duracion_EsperaTrayecto', 'Plan_Real':'Real', 'Tipo':'INTEGER'},
        {'Campo': 'R_Duracion_EsperaServicio', 'Plan_Real':'Real', 'Tipo':'INTEGER'},
        {'Campo': 'R_Costo_Hora_Servicio', 'Plan_Real':'Real', 'Tipo':'FLOAT'},
        {'Campo': 'R_Costo_Hora_Espera', 'Plan_Real':'Real', 'Tipo':'FLOAT'},
        {'Campo': 'FechaSalidaViajeReal', 'Plan_Real':'Real', 'Tipo':'TIMESTAMP'},
        {'Campo': 'FechaRetornoViajeReal', 'Plan_Real':'Real', 'Tipo':'TIMESTAMP'},
        {'Campo': 'FechaEntregaPedidoReal', 'Plan_Real':'Real', 'Tipo':'TIMESTAMP'},
        {'Campo': 'FechaSalidaPedidoReal', 'Plan_Real':'Real', 'Tipo':'TIMESTAMP'},
        {'Campo': 'FechaCancelacionDestino', 'Plan_Real':'Real', 'Tipo':'TIMESTAMP'},
        {'Campo': 'ProveedorFlete', 'Plan_Real':'Real', 'Tipo':'STRING'},
        {'Campo': 'OperadorReal', 'Plan_Real':'Real', 'Tipo':'STRING'},
        {'Campo': 'TiradaReal', 'Plan_Real':'Real', 'Tipo':'INTEGER'},
        {'Campo': 'R_Tipo_Vehiculo', 'Plan_Real':'Real', 'Tipo':'STRING'},
        {'Campo': 'R_Viaje_salio_a_Tiempo', 'Plan_Real':'Real', 'Tipo':'INTEGER'},
        {'Campo': 'R_Viaje_Respeto_ruta', 'Plan_Real':'Real', 'Tipo':'INTEGER'},
        {'Campo': 'R_Costo_Total_Nodo', 'Plan_Real':'Real', 'Tipo':'FLOAT'},
        {'Campo': 'Pof', 'Plan_Real':'Real', 'Tipo':'INTEGER'},
        {'Campo': 'PofTiempo', 'Plan_Real':'Real', 'Tipo':'INTEGER'},
        {'Campo': 'PofCompleto', 'Plan_Real':'Real', 'Tipo':'INTEGER'},
        {'Campo': 'PofSD', 'Plan_Real':'Real', 'Tipo':'INTEGER'},
        {'Campo': 'PofDocs', 'Plan_Real':'Real', 'Tipo':'INTEGER'},
        {'Campo': 'E_Duracion_Servicio', 'Plan_Real':'Real', 'Tipo':'INTEGER'},
        {'Campo': 'E_Duracion_Traslado_Destino_Anterior', 'Plan_Real':'Real', 'Tipo':'INTEGER'},
        {'Campo': 'E_Duracion_Traslado_Destino_Anterior_Subestimo', 'Plan_Real':'Real', 'Tipo':'INTEGER'},
        {'Campo': 'E_Duracion_Traslado_Destino_Anterior_Sobreestimo', 'Plan_Real':'Real', 'Tipo':'INTEGER'},
        {'Campo': 'E_Duracion_Servicio_Subestimado', 'Plan_Real':'Real', 'Tipo':'INTEGER'},
        {'Campo': 'E_Duracion_Servicio_Sobreestimado', 'Plan_Real':'Real', 'Tipo':'INTEGER'},
        {'Campo': 'E_Costo_Total_Nodo', 'Plan_Real':'Real', 'Tipo':'FLOAT'},
        {'Campo': 'idParada', 'Plan_Real':'Plan', 'Tipo':'INTEGER'},
        {'Campo': 'idSesion', 'Plan_Real':'Plan', 'Tipo':'INTEGER'},
        {'Campo': 'idPlaneador', 'Plan_Real':'Plan', 'Tipo':'INTEGER'},
        {'Campo': 'idDespegador', 'Plan_Real':'Plan', 'Tipo':'INTEGER'},
        {'Campo': 'idFletera', 'Plan_Real':'Real', 'Tipo':'INTEGER'},
        {'Campo': 'idOperador', 'Plan_Real':'Real', 'Tipo':'INTEGER'},
        {'Campo': 'idVehiculoPlan', 'Plan_Real':'Plan', 'Tipo':'INTEGER'},
        {'Campo': 'idVehiculo', 'Plan_Real':'Real', 'Tipo':'INTEGER'},
        {'Campo': 'idDestinoTR1', 'Plan_Real':'Plan', 'Tipo':'INTEGER'},
        {'Campo': 'TipoDistribucion', 'Plan_Real':'Plan', 'Tipo':'STRING'},
        {'Campo': 'Placas', 'Plan_Real':'Real', 'Tipo':'STRING'},
        {'Campo': 'NumeroEconomico', 'Plan_Real':'Real', 'Tipo':'STRING'},
        {'Campo': 'PesoPermitido', 'Plan_Real':'Plan', 'Tipo':'FLOAT'},
        {'Campo': 'VolumenPermitido', 'Plan_Real':'Plan', 'Tipo':'FLOAT'},
        {'Campo': 'VolumenFrioPermitido', 'Plan_Real':'Plan', 'Tipo':'FLOAT'},
        {'Campo': 'Rendimiento', 'Plan_Real':'Plan', 'Tipo':'FLOAT'},
        {'Campo': 'RestriccionVolumen', 'Plan_Real':'Plan', 'Tipo':'FLOAT'},
        {'Campo': 'VentanaFechaInicial', 'Plan_Real':'Plan', 'Tipo':'DATE'},
        {'Campo': 'VentanaFechaFinal', 'Plan_Real':'Plan', 'Tipo':'DATE'},
        {'Campo': 'VentanaLunVieIni1', 'Plan_Real':'Plan', 'Tipo':'TIME'},
        {'Campo': 'VentanaLunVieFin1', 'Plan_Real':'Plan', 'Tipo':'TIME'},
        {'Campo': 'VentanaLunVieIni2', 'Plan_Real':'Plan', 'Tipo':'TIME'},
        {'Campo': 'VentanaLunVieFin2', 'Plan_Real':'Plan', 'Tipo':'TIME'},
        {'Campo': 'VentanaSabDomIni', 'Plan_Real':'Plan', 'Tipo':'TIME'},
        {'Campo': 'VentanaSabDomFin', 'Plan_Real':'Plan', 'Tipo':'TIME'},
        {'Campo': 'PedidosEntregadosReal', 'Plan_Real':'Real', 'Tipo':'INTEGER'},
        {'Campo': 'KgEntregadosReal', 'Plan_Real':'Real', 'Tipo':'FLOAT'},
        {'Campo': 'm3EntregadoReal', 'Plan_Real':'Real', 'Tipo':'FLOAT'},
        {'Campo': 'ValorEntregadoReal', 'Plan_Real':'Real', 'Tipo':'FLOAT'},
        {'Campo': 'PiezasEntregadoReal', 'Plan_Real':'Real', 'Tipo':'INTEGER'},
        {'Campo': 'LatitudEntregaPlan', 'Plan_Real':'Plan', 'Tipo':'FLOAT'},
        {'Campo': 'LongitudEntregaPlan', 'Plan_Real':'Plan', 'Tipo':'FLOAT'},
        {'Campo': 'LatitudEntregaReal', 'Plan_Real':'Real', 'Tipo':'FLOAT'},
        {'Campo': 'LongitudEntregaReal', 'Plan_Real':'Real', 'Tipo':'FLOAT'},
        {'Campo': 'LatitudSalidaReal', 'Plan_Real':'Real', 'Tipo':'FLOAT'},
        {'Campo': 'LongitudSalidaReal', 'Plan_Real':'Real', 'Tipo':'FLOAT'},
        {'Campo': 'FueraDeGeocercaEntrega', 'Plan_Real':'Real', 'Tipo':'FLOAT'},
        {'Campo': 'FueraDeGeocercaSalida', 'Plan_Real':'Real', 'Tipo':'FLOAT'},
        {'Campo': 'ExactitudCoordenadasEntrega', 'Plan_Real':'Real', 'Tipo':'FLOAT'},
        {'Campo': 'ExactitudCoordenadasSalida', 'Plan_Real':'Real', 'Tipo':'FLOAT'},
        {'Campo': 'DisponibilidadCoordenadasEntrega', 'Plan_Real':'Real', 'Tipo':'INTEGER'},
        {'Campo': 'DisponibilidadCoordenadasSalida', 'Plan_Real':'Real', 'Tipo':'INTEGER'},
        {'Campo': 'FechaHoraAutomaticaEntrega', 'Plan_Real':'Real', 'Tipo':'INTEGER'},
        {'Campo': 'FechaHoraAutomaticaSalida', 'Plan_Real':'Real', 'Tipo':'INTEGER'},
        {'Campo': 'IsMockedEntrega', 'Plan_Real':'Real', 'Tipo':'INTEGER'},
        {'Campo': 'IsMockedSalida', 'Plan_Real':'Real', 'Tipo':'INTEGER'},
        {'Campo': 'InfoRedEntrega', 'Plan_Real':'Real', 'Tipo':'STRING'},
        {'Campo': 'InfoRedSalida', 'Plan_Real':'Real', 'Tipo':'STRING'},
        {'Campo': 'FirmaEntrega', 'Plan_Real':'Real', 'Tipo':'STRING'},
        {'Campo': 'FotoEntrega', 'Plan_Real':'Real', 'Tipo':'STRING'},
        {'Campo': 'TiemServicioTirada', 'Plan_Real':'Plan', 'Tipo':'INTEGER'},
        {'Campo': 'TiemServicioDestino', 'Plan_Real':'Plan', 'Tipo':'INTEGER'},
        {'Campo': 'StatusAnterior', 'Plan_Real':'Real', 'Tipo':'FLOAT'},
        {'Campo': 'Status', 'Plan_Real':'Real', 'Tipo':'FLOAT'}
    ]

diccionario_campos_pedidos =\
    [
        {'Campo': 'Pedido', 'Plan_Real': 'Plan', 'Tipo': 'STRING'},
        {'Campo': 'Status', 'Plan_Real': 'Real', 'Tipo': 'STRING'},
        {'Campo': 'P_Nombre_Sesion', 'Plan_Real': 'Plan', 'Tipo': 'STRING'},
        {'Campo': 'P_Fecha_Sesion', 'Plan_Real': 'Plan', 'Tipo': 'TIMESTAMP'},
        {'Campo': 'P_Fecha_Venta', 'Plan_Real': 'Plan', 'Tipo': 'TIMESTAMP'},
        {'Campo': 'P_Fecha_Autorizacion', 'Plan_Real': 'Plan', 'Tipo': 'TIMESTAMP'},
        {'Campo': 'P_Fecha_Predespegue', 'Plan_Real': 'Plan', 'Tipo': 'TIMESTAMP'},
        {'Campo': 'FechaSalidaViajePlan', 'Plan_Real': 'Plan', 'Tipo': 'TIMESTAMP'},
        {'Campo': 'FechaRetornoViajePlan', 'Plan_Real': 'Plan', 'Tipo': 'TIMESTAMP'},
        {'Campo': 'FechaEntregaDestino', 'Plan_Real': 'Plan', 'Tipo': 'TIMESTAMP'},
        {'Campo': 'FechaSalidaDestino', 'Plan_Real': 'Plan', 'Tipo': 'TIMESTAMP'},
        {'Campo': 'P_Fecha_Entrega_TR2', 'Plan_Real': 'Plan', 'Tipo': 'TIMESTAMP'},
        {'Campo': 'idEmpresa', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'P_Empresa', 'Plan_Real': 'Plan', 'Tipo': 'STRING'},
        {'Campo': 'VendedorPedido', 'Plan_Real': 'Plan', 'Tipo': 'STRING'},
        {'Campo': 'Planeador', 'Plan_Real': 'Plan', 'Tipo': 'STRING'},
        {'Campo': 'P_Autorizador', 'Plan_Real': 'Plan', 'Tipo': 'STRING'},
        {'Campo': 'Despegador', 'Plan_Real': 'Plan', 'Tipo': 'STRING'},
        {'Campo': 'P_Tipo_Envio', 'Plan_Real': 'Plan', 'Tipo': 'STRING'},
        {'Campo': 'P_Tipo_Servicio', 'Plan_Real': 'Plan', 'Tipo': 'STRING'},
        {'Campo': 'TipoPedido', 'Plan_Real': 'Plan', 'Tipo': 'STRING'},
        {'Campo': 'P_Familia', 'Plan_Real': 'Plan', 'Tipo': 'STRING'},
        {'Campo': 'Productos', 'Plan_Real': 'Plan', 'Tipo': 'STRING'},
        {'Campo': 'Valor', 'Plan_Real': 'Plan', 'Tipo': 'FLOAT'},
        {'Campo': 'Piezas', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'Peso', 'Plan_Real': 'Plan', 'Tipo': 'FLOAT'},
        {'Campo': 'Volumen', 'Plan_Real': 'Plan', 'Tipo': 'FLOAT'},
        {'Campo': 'Cedi', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'Cedis', 'Plan_Real': 'Plan', 'Tipo': 'STRING'},
        {'Campo': 'P_CEDIS_Intermedio', 'Plan_Real': 'Plan', 'Tipo': 'STRING'},
        {'Campo': 'P_Region_TR1', 'Plan_Real': 'Plan', 'Tipo': 'STRING'},
        {'Campo': 'P_Entidad_TR1', 'Plan_Real': 'Plan', 'Tipo': 'STRING'},
        {'Campo': 'P_Ciudad_TR1', 'Plan_Real': 'Plan', 'Tipo': 'STRING'},
        {'Campo': 'Destino', 'Plan_Real': 'Plan', 'Tipo': 'STRING'},
        {'Campo': 'P_Latitud_Longitud_TR1', 'Plan_Real': 'Plan', 'Tipo': 'STRING'},
        {'Campo': 'DestinoTR2', 'Plan_Real': 'Plan', 'Tipo': 'STRING'},
        {'Campo': 'P_Latitud_Longitud_TR2', 'Plan_Real': 'Plan', 'Tipo': 'STRING'},
        {'Campo': 'Viaje', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'Status', 'Plan_Real': 'Real', 'Tipo': 'INTEGER'},
        {'Campo': 'StatusAnterior', 'Plan_Real': 'Real', 'Tipo': 'INTEGER'},
        {'Campo': 'TiradaPlan', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'TipoVehiculo', 'Plan_Real': 'Plan', 'Tipo': 'STRING'},
        {'Campo': 'P_Costo_Total_Pedido', 'Plan_Real': 'Plan', 'Tipo': 'FLOAT'},
        {'Campo': 'FechaSalidaViajeReal', 'Plan_Real': 'Real', 'Tipo': 'TIMESTAMP'},
        {'Campo': 'FechaRetornoViajeReal', 'Plan_Real': 'Real', 'Tipo': 'TIMESTAMP'},
        {'Campo': 'FechaEntregaPedidoReal', 'Plan_Real': 'Real', 'Tipo': 'TIMESTAMP'},
        {'Campo': 'FechaSalidaPedidoReal', 'Plan_Real': 'Real', 'Tipo': 'TIMESTAMP'},
        {'Campo': 'FechaCancelacionPedido', 'Plan_Real': 'Real', 'Tipo': ''},
        {'Campo': 'ProveedorFlete', 'Plan_Real': 'Real', 'Tipo': 'STRING'},
        {'Campo': 'Operador', 'Plan_Real': 'Real', 'Tipo': 'STRING'},
        {'Campo': 'TiradaReal', 'Plan_Real': 'Real', 'Tipo': 'INTEGER'},
        {'Campo': 'TipoVehiculo', 'Plan_Real': 'Real', 'Tipo': 'STRING'},
        {'Campo': 'R_Viaje_salio_a_Tiempo', 'Plan_Real': 'Real', 'Tipo': 'INTEGER'},
        {'Campo': 'R_Viaje_Respeto_ruta', 'Plan_Real': 'Real', 'Tipo': 'INTEGER'},
        {'Campo': 'R_Costo_Total_Pedido', 'Plan_Real': 'Real', 'Tipo': 'FLOAT'},
        {'Campo': 'Pof', 'Plan_Real': 'Real', 'Tipo': 'INTEGER'},
        {'Campo': 'PofTiempo', 'Plan_Real': 'Real', 'Tipo': 'INTEGER'},
        {'Campo': 'PofCompleto', 'Plan_Real': 'Real', 'Tipo': 'INTEGER'},
        {'Campo': 'PofSD', 'Plan_Real': 'Real', 'Tipo': 'INTEGER'},
        {'Campo': 'PofDocs', 'Plan_Real': 'Real', 'Tipo': 'INTEGER'},
        {'Campo': 'idPedido', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'idSesion', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'idPlaneador', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'idDespegador', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'idVehiculoPlan', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'idFletera', 'Plan_Real': 'Real', 'Tipo': 'INTEGER'},
        {'Campo': 'idOperador', 'Plan_Real': 'Real', 'Tipo': 'INTEGER'},
        {'Campo': 'idVehiculo', 'Plan_Real': 'Real', 'Tipo': 'INTEGER'},
        {'Campo': 'idDestinoTR1', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'idDestinoTR2', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'idParada', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'TipoDistribucion', 'Plan_Real': 'Plan', 'Tipo': 'STRING'},
        {'Campo': 'Placas', 'Plan_Real': 'Real', 'Tipo': 'STRING'},
        {'Campo': 'NumeroEconomico', 'Plan_Real': 'Real', 'Tipo': 'STRING'},
        {'Campo': 'PesoPermitido', 'Plan_Real': 'Plan', 'Tipo': 'FLOAT'},
        {'Campo': 'VolumenPermitido', 'Plan_Real': 'Plan', 'Tipo': 'FLOAT'},
        {'Campo': 'VolumenFrioPermitido', 'Plan_Real': 'Plan', 'Tipo': 'FLOAT'},
        {'Campo': 'Rendimiento', 'Plan_Real': 'Plan', 'Tipo': 'FLOAT'},
        {'Campo': 'RestriccionVolumen', 'Plan_Real': 'Plan', 'Tipo': 'FLOAT'},
        {'Campo': 'Dedicado', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'VentanaFechaInicial', 'Plan_Real': 'Plan', 'Tipo': 'DATE'},
        {'Campo': 'VentanaFechaFinal', 'Plan_Real': 'Plan', 'Tipo': 'DATE'},
        {'Campo': 'VentanaLunVieIni1', 'Plan_Real': 'Plan', 'Tipo': 'TIME'},
        {'Campo': 'VentanaLunVieFin1', 'Plan_Real': 'Plan', 'Tipo': 'TIME'},
        {'Campo': 'VentanaLunVieIni2', 'Plan_Real': 'Plan', 'Tipo': 'TIME'},
        {'Campo': 'VentanaLunVieFin2', 'Plan_Real': 'Plan', 'Tipo': 'TIME'},
        {'Campo': 'VentanaSabDomIni', 'Plan_Real': 'Plan', 'Tipo': 'TIME'},
        {'Campo': 'VentanaSabDomFin', 'Plan_Real': 'Plan', 'Tipo': 'TIME'},
        {'Campo': 'TiemDescarga', 'Plan_Real': 'Plan', 'Tipo': 'INTEGER'},
        {'Campo': 'LatitudEntregaPlan', 'Plan_Real': 'Plan', 'Tipo': 'FLOAT'},
        {'Campo': 'LongitudEntregaPlan', 'Plan_Real': 'Plan', 'Tipo': 'FLOAT'},
        {'Campo': 'LatitudEntregaReal', 'Plan_Real': 'Real', 'Tipo': 'FLOAT'},
        {'Campo': 'LongitudEntregaReal', 'Plan_Real': 'Real', 'Tipo': 'FLOAT'},
        {'Campo': 'LatitudSalidaReal', 'Plan_Real': 'Real', 'Tipo': 'FLOAT'},
        {'Campo': 'LongitudSalidaReal', 'Plan_Real': 'Real', 'Tipo': 'FLOAT'},
        {'Campo': 'FueraDeGeocercaEntrega', 'Plan_Real': 'Real', 'Tipo': 'FLOAT'},
        {'Campo': 'FueraDeGeocercaSalida', 'Plan_Real': 'Real', 'Tipo': 'FLOAT'},
        {'Campo': 'DisponibilidadCoordenadasEntrega', 'Plan_Real': 'Real', 'Tipo': 'INTEGER'},
        {'Campo': 'FechaHoraAutomaticaEntrega', 'Plan_Real': 'Real', 'Tipo': 'INTEGER'},
        {'Campo': 'FirmaEntrega', 'Plan_Real': 'Real', 'Tipo': 'STRING'},
        {'Campo': 'FotoEntrega', 'Plan_Real': 'Real', 'Tipo': 'STRING'},
        {'Campo': 'Factura', 'Plan_Real': 'Real', 'Tipo': 'STRING'},
        {'Campo': 'FotoFactura', 'Plan_Real': 'Real', 'Tipo': 'STRING'}

    ]

diccionario_campos_itinerario =\
    [
        {'Campo': 'Cedi', 'Plan_Real':'Plan', 'Tipo':'INTEGER'},
        {'Campo': 'Viaje', 'Plan_Real':'Plan', 'Tipo':'INTEGER'},
        {'Campo': 'TiradaPlan', 'Plan_Real':'Plan', 'Tipo':'INTEGER'},
        {'Campo': 'FechaInicial', 'Plan_Real':'Plan', 'Tipo':'TIMESTAMP'},
        {'Campo': 'FechaFinal', 'Plan_Real':'Plan', 'Tipo':'TIMESTAMP'},
        {'Campo': 'Destino', 'Plan_Real':'Plan', 'Tipo':'STRING'},
        {'Campo': 'Actividad', 'Plan_Real':'Plan', 'Tipo':'STRING'},
        {'Campo': 'Duracion', 'Plan_Real':'Plan', 'Tipo':'INTEGER'},
        {'Campo': 'IdItinerario', 'Plan_Real':'Plan', 'Tipo':'INTEGER'}
    ]

columnas_viajesplan_a_tiradas =\
    [
        'Viaje',
        'P_Nombre_Sesion',
        'P_Fecha_Sesion',
        'P_Fecha_Autorizacion',
        'P_Fecha_Predespegue',
        'FechaSalidaViajePlan',
        'FechaRetornoViajePlan',
        'idEmpresa',
        'P_Empresa',
        'Planeador',
        'P_Autorizador',
        'Despegador',
        'Cedi',
        'Cedis',
        'TipoVehiculo',
        'idSesion',
        'idPlaneador',
        'idDespegador',
        'idVehiculoPlan',
        'TipoDistribucion',
        'PesoPermitido',
        'VolumenPermitido',
        'VolumenFrioPermitido',
        'Rendimiento',
        'P_Costo_Total_Nodo',
        'P_Costo_Total_Pedido',
    ]

columnas_tiradasplan_a_pedidos =\
    [
        'idParada',
        'FechaEntregaDestino',
        'FechaSalidaDestino',
        'P_Region_TR1',
        'P_Entidad_TR1',
        'P_Ciudad_TR1',
        'Destino',
        'P_Latitud_Longitud_TR1',
        'TiradaPlan',
        'idDestinoTR1',
        'RestriccionVolumen',
        'Dedicado',
        'VentanaSabDomIni',
        'VentanaSabDomFin',
        'LatitudEntregaPlan',
        'LongitudEntregaPlan'
    ]

columnas_pedidosplan_a_tiradas_first =\
    [
        'idParada',
        'P_Fecha_Venta',
        'P_Fecha_Entrega_TR2',
        'VendedorPedido',
        'VentanaFechaInicial',
        'VentanaFechaFinal'
    ]

columnas_pedidosplan_a_tiradas_sum =\
    [
        'idParada',
        'Valor',
        'Piezas',
        'Peso',
        'Volumen'
    ]

columnas_pedidosplan_a_tiradas_count =\
    [
        'idParada',
        'Pedidos'
    ]

columnas_pedidosplan_a_tiradas_concat =\
    [
        'idParada',
        'P_Destino_TR2'
    ]

diccionario_cambio_actvidadduracion_fb_df =\
    {
        'Trayecto':         'R_Duracion_Traslado',
        'Servicio':         'R_Duracion_Servicio',
        'Comida':           'R_Duracion_Alimentos',
        'Hotel':            'R_Duracion_Pernocta',
        'Espera':           'R_Duracion_EsperaServicio',
        'Descanso':         'R_Duracion_Descanso',
        'Combustible':      'R_Duracion_Combustible',
        'Ferry':            'R_Duracion_Ferry',
        'LiquidacionViaje': 'R_Duracion_Liquidacion'

    }

diccionario_cambio_actvidadcount_fb_df =\
    {
        'Trayecto':         'R_Actividad_Traslado',
        'Servicio':         'R_Actividad_Servicio',
        'Comida':           'R_Actividad_Alimentos',
        'Hotel':            'R_Actividad_Pernocta',
        'Espera':           'R_Actividad_EsperaServicio',
        'Descanso':         'R_Actividad_Descanso',
        'Combustible':      'R_Actividad_Combustible',
        'Ferry':            'R_Actividad_Ferry',
        'LiquidacionViaje': 'R_Actividad_Liquidacion'

    }

diccionario_cambio_actividad_fb_dfnodos_tray =\
    {
        'duracion-actividad': 'R_Duracion_Traslado_Destino_Anterior',
        'duracion-actividadpos': 'R_Duraci??n_Traslado_Destino_Siguiente',
    }
diccionario_cambio_actividad_fb_dfnodos_espera =\
    {
        'duracion-actividad': 'R_Duracion_EsperaServicio',
    }
diccionario_cambio_actividad_fb_dfnodos_servicio =\
    {
        'duracion-actividad': 'R_Duracion_Servicio',
    }

columnas_agrupacion_dfactividades_nodos = ['tipo-actividad', 'tirada-actividad', 'duracion-actividad']

df_campos_viajes = pd.DataFrame(diccionario_campos_viajes)
columnas_viajesplan = df_campos_viajes.loc[df_campos_viajes['Plan_Real'] == 'Plan', 'Campo'].tolist()

df_campos_nodos = pd.DataFrame(diccionario_campos_nodos)
columnas_nodosplan = df_campos_nodos.loc[df_campos_nodos['Plan_Real'] == 'Plan', 'Campo'].tolist()

df_campos_pedidos = pd.DataFrame(diccionario_campos_pedidos)
columnas_pedidosplan = df_campos_pedidos.loc[df_campos_pedidos['Plan_Real'] == 'Plan', 'Campo'].tolist()

df_campos_itinerario = pd.DataFrame(diccionario_campos_itinerario)
columnas_itinerarioplan = df_campos_itinerario.loc[df_campos_itinerario['Plan_Real'] == 'Plan', 'Campo'].tolist()