-- ---------------------------------------------------------------------
-- RACH - Viernes 09 de Agosto de 2019
-- Para obtener los viajes despegados y en transito
-- ---------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_api_etl_obtener_viajes;

DELIMITER //

CREATE PROCEDURE sp_api_etl_obtener_viajes(
    _id_cedi INT
)
BEGIN
    DECLARE _msg TEXT;
    DECLARE _code CHAR(5) DEFAULT '00000';

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        GET DIAGNOSTICS CONDITION 1
            _code = RETURNED_SQLSTATE, _msg = MESSAGE_TEXT;
        SELECT 'FAIL' AS result, _msg AS data;
    END;

    -- Verifico que exista el cedi
    IF NOT EXISTS(
        SELECT idCedi
        FROM cedis
        WHERE idCedi = _id_cedi
    ) THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'No existe el cedi';
    END IF;

    -- Verifico que existan viajes
    IF NOT EXISTS(
        SELECT
            COUNT(idViaje)
        FROM
            viajes
        WHERE
            status in (9, 9.5)
            and escrito_BQ = 0
    ) THEN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'No hay viajes con el estatus solicitado';
    END IF;

    DROP TEMPORARY TABLE IF EXISTS _vw_viajes;
    CREATE TEMPORARY TABLE _vw_viajes
    select
        v.idViaje as Viaje,
        pr.Nombre as P_Nombre_Sesion,
        v.token as P_Token,
        date_format(pr.FechaCreacion, '%Y-%m-%d %H:%i') as P_Fecha_Sesion,
        date_format(v.fechaAutorizacion, '%Y-%m-%d %H:%i') as P_Fecha_Autorizacion,
        date_format(v.fechaPredespegue, '%Y-%m-%d %H:%i') as P_Fecha_Predespegue,
        date_format(v.fechaSalida, '%Y-%m-%d %H:%i') as FechaSalidaViajePlan,
        date_format(v.fechaRetorno, '%Y-%m-%d %H:%i') as FechaRetornoViajePlan,
        tiempoTotalTrayecto as P_Duracion_Traslado,
        tiempoTotalServicio as P_Duracion_Servicio,
        tiempoTotalPernocta as P_Duracion_Pernocta,
        tiempoTotalComidas as P_Duracion_Alimentos,
        tiempoTotalEspera as P_Duracion_EsperaTrayecto,
        tiempoTotalDescansos as P_Duracion_Descanso,
        0 as P_Duracion_Combustible,
        0 as P_Duracion_Ferry,
        0 as P_Duracion_Liquidacion,
        0 as P_Duracion_Ajuste,
        tiempoTotalViaje as P_Duracion_Total_Viaje,
        v.idEmpresa as idEmpresa,
        em.empresa as P_Empresa,
        lv_plan.nombre as Planeador,
        lv_aut.nombre as P_Autorizador,
        lv_pre.nombre as Despegador,
        v.Tiradas,
        v.Pedidos,
        v.Valor,
        v.Piezas,
        v.Peso,
        v.Volumen,
        v.idCedi as Cedi,
        ce.Cedis as Cedis,
        0 as P_Cedis_Intermedio,
        v.idCediAterrizaje as CediAterrizaje,
        ceAt.Cedis as P_Cedis_Aterriza,
        if(v.idcedi <> v.idCediAterrizaje,2,1) as P_Num_CEDIS,
        0 as P_Numero_Regiones,
        0 as P_Numero_Entidades,
        0 as P_Numero_Destinos_TR1,
        0 as P_Numero_Destinos_TR2,
        ve.TipoVehiculo,
        0 as P_Activ_Traslado,
        v.Tiradas as P_Activ_Servicio,
        v.numComidas as P_Activ_Alimentos,
        v.numPernoctas as P_Activ_Pernocta,
        0 as P_Activ_EsperaTrayecto,
        v.numDescansos as P_Activ_Descanso,
        0 as P_Activ_Combustible,
        0 as P_Activ_Ferry,
        1 as P_Activ_Liquidacion,
        0 as P_Activ_Ajuste,
        (v.Tiradas + v.numComidas + v.numPernoctas + v.numDescansos + 1) as P_Activ_Total,
        costoCombustible as CostoCombustiblePlan,
        0 as CostoCasetaPlan,
        costoComidas as CostoComidasPlan,
        costoHotel as CostoHotelPlan,
        costoFerry as CostoFerryPlan,
        costoThermo as CostoThermoPlan,
        0 as CostoOtrosPlan,
        costoTotal as CostoTotalPlan,
        v.km as Km,
        v.co2 as Co2,
        v.LitrosCombustible,
        v.LitrosThermo,
        ve.Rendimiento,
        ve.RendimientoThermo,
        ve.Peso as PesoPermitido,
        (ve.VolumenSeco + ve.VolumenFrio) as VolumenPermitido,
        ve.VolumenSeco as VolumenSecoPermitido,
        ve.VolumenFrio as VolumenFrioPermitido,
        ocupacionPesoTotal as P_Ocupacion_Peso,
        ocupacionVolumenTotal as OcupacionVolumen,
        ocupacionVolumenSeco as P_Ocupacion_Volumen_Seco,
        ocupacionVolumenFrio as P_Ocupacion_Volumen_Frio,
        v.idProyecto as idSesion,
        v.idPlaneador as idPlaneador,
        v.idPlaneador as idAutorizador,
        v.idUsuario as idDespegador,
        ve.idFletera as idFleteraPlan,
        v.idVehiculo as idVehiculoPlan,
        infraccionReglaPeso as InfraccionReglaPeso,
        infraccionReglaVolumen as InfraccionReglaVolumen,
        infraccionReglaValor as InfraccionReglaValor,
        infraccionReglaAcceso as InfraccionReglaAcceso,
        infraccionDestinoDedicado as InfraccionDestinoDedicado,
        infraccionVentanaEntrega as InfraccionVentanaEntrega,
        infraccionEntrega1Dia as InfraccionEntrega1Dia,
        infraccionDiasApertura as InfraccionDiasApertura,
        infraccionCompatibilidad as InfraccionCompatibilidad,
        infraccion as Infraccion,
        ce.latitud as LatitudSalidaPlan,
        ce.longitud as LongitudSalidaPlan,
        ceAt.latitud as LatitudRetornoPlan,
        ceAt.longitud as LongitudRetornoPlan,
        tipoDistribucion as TipoDistribucion

    from
        viajes v
        left join
        usuarios lv_plan
        ON lv_plan.idusuario = v.idPlaneador
        left join
        usuarios lv_aut
        ON lv_aut.idusuario = v.idAutorizador
        left join
        usuarios lv_pre
        ON lv_pre.idusuario = v.idPredespegador
        join
        proyectos pr USING (idProyecto)
        join
        cedis ce
        ON ce.idCedi = v.idCedi
        join
        cedis ceAt
        ON ceAt.idCedi = v.idCediAterrizaje
        join
        empresas em
        ON em.idEmpresa = v.idEmpresa
        join
        vehiculos ve USING(idVehiculo)
        WHERE v.status in (9, 9.5)
        and escrito_BQ = 0
        group by idViaje
        LIMIT 20;


    ALTER TABLE _vw_viajes ADD PRIMARY KEY(Viaje);

    # operacion exitosa
	SELECT 'OK' AS result, '' AS data;

    #genero json de viajes
    SELECT
		*
	FROM
		_vw_viajes;

	#Genero itinerario de viajes
    SELECT
        _v.Cedi as Cedi,
        _v.Cedis as Cedis,
        _v.P_Cedis_Aterriza as P_Cedis_Aterriza,
        _v.Viaje AS Viaje,
        i.tirada AS TiradaPlan,
        date_format(i.fechaActividadInicio, '%Y-%m-%d %H:%i') AS FechaInicial,
		date_format(i.fechaActividadFin, '%Y-%m-%d %H:%i') AS FechaFinal,
		i.actividad AS Actividad,
		i.duracion AS Duracion,
		i.idItinerario as IdItinerario

	FROM
		itinerario i
			JOIN
		_vw_viajes _v
        ON _v.Viaje = i.idViaje;

	#Genero información de los destinos o tiradas
    SELECT
        date_format(p.fechaEntrega, '%Y-%m-%d %H:%i') as FechaEntregaDestino,
		date_format(p.fechaSalida, '%Y-%m-%d %H:%i') as FechaSalidaDestino,
		TIME_TO_SEC(TIMEDIFF(p.fechasalida, p.fechaentrega)) as P_Duracion_Servicio,
        d.Region as P_Region_TR1,
        d.SubRegion as P_SubRegion_TR1,
        d.Estado as P_Entidad_TR1,
        d.Municipio as P_Ciudad_TR1,
        d.Zona as P_Zona_TR1,
        d.Destino as Destino,
        d.Dedicado as Dedicado,
        concat(d.latitud, ",", d.longitud) as P_Latitud_Longitud_TR1,
        p.idViaje as Viaje,
        p.tirada as TiradaPlan,
        p.idParada,
		p.idDestino as idDestinoTR1,
        d.restriccionVolumen as RestriccionVolumen,
        d.VentanaLunVieIni1,
        d.VentanaLunVieFin1,
        d.VentanaLunVieIni2,
        d.VentanaLunVieFin2,
        d.VentanaSabDomIni,
        d.VentanaSabDomFin,
        d.latitud as LatitudEntregaPlan,
        d.longitud as LongitudEntregaPlan,
        d.tiempoServicio as TiemServicioDestino,
        p.tiemServicio as TiemServicioTirada
	FROM
		paradas p
	    JOIN
		_vw_viajes _v
        ON _v.Viaje = p.idViaje
		JOIN destinos d
		ON p.iddestino = d.iddestino;

	#Obtengo la info de los pedidos
    SELECT
        pe.Pedido as Pedido,
        date_format(pe.fechaDeVenta, '%Y-%m-%d %H:%i') as P_Fecha_Venta,
        date_format(ventanaTR2, '%Y-%m-%d %H:%i') as P_Fecha_Entrega_TR2,
        pe.Vendedor as VendedorPedido,
        pe.familia as P_Familia,
		pe.idDestinoTR2 as idDestinoTR2,
		pe.tipoEnvio as P_Tipo_Envio,
		pe.tipoPedido as TipoPedido,
		pe.tipoServicio as P_Tipo_Servicio,
		pe.producto as Productos,
		pe.valor as Valor,
		pe.piezas as Piezas,
		pe.peso as Peso,
		pe.volumen as Volumen,
		pe.idCediIntermedio as P_CEDIS_Intermedio,
		d.Destino as DestinoTR2,
		concat(d.latitud, ",", d.longitud) as P_Latitud_Longitud_TR2,
		pe.idViaje as Viaje,
		pe.idPedido,
        pe.idParada,
		date_format(pe.ventanaFechaIni, '%Y-%m-%d') as VentanaFechaInicial,
		date_format(pe.ventanaFechaFin, '%Y-%m-%d') as VentanaFechaFinal,
		date_format(pe.primerHorarioIni, '%H:%i') as VentanaLunVieIni1,
		date_format(pe.primerHorarioFin, '%H:%i') as VentanaLunVieFin1,
		date_format(pe.segundoHorarioIni, '%H:%i') as VentanaLunVieIni2,
		date_format(pe.segundoHorarioFin, '%H:%i') as VentanaLunVieFin2,
		pe.tiemDescarga as TiemDescarga

	FROM
		pedidos pe
			JOIN
		_vw_viajes _v
		ON _v.Viaje = pe.idViaje
		JOIN
		destinos d
		ON pe.idDestinoTR2 = d.idDestino;

END //

-- call sp_api_etl_obtener_viajes(16)