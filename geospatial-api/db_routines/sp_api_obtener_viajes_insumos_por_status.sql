-- ---------------------------------------------------------------------
-- RACH - Miércoles 31 de Julio de 2019
-- Para obtener los insumos de los viajes por status
-- ---------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_api_obtener_viajes_insumos_por_status;

DELIMITER //

CREATE PROCEDURE sp_api_obtener_viajes_insumos_por_status(
    _id_cedi INT,
    _status FLOAT
)
BEGIN
    DECLARE _msg TEXT;
    DECLARE _code CHAR(5) DEFAULT '00000';

    DECLARE _tabla VARCHAR(20);

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        GET DIAGNOSTICS CONDITION 1
            _code = RETURNED_SQLSTATE, _msg = MESSAGE_TEXT;
        SELECT 'FAIL' AS result, _msg AS data;
    END;

    SET SESSION time_zone = 'America/Mexico_City';

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
            status = _status
    ) THEN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'No hay viajes con el estatus solicitado';
    END IF;

    SET _tabla = 'fechaAutorizacion';

    IF _status = 9 THEN
        DROP TEMPORARY TABLE IF EXISTS _vw_viajes;
        CREATE TEMPORARY TABLE _vw_viajes
        select 
            v.status as EstatusViaje,
            idViaje as IdViaje,
            km as Km,
            date_format(fechaSalida, '%Y-%m-%d %H:%i') as FechaSalida,
            date_format(fechaRetorno, '%Y-%m-%d %H:%i') as FechaRetorno,
            tipoDistribucion as TipoDistribucion,
            v.idPlaneador as IdPlaneador,
            CONCAT(IFNULL(usr.nombre, ''), ' ', IFNULL(usr.apellidoPaterno, ''), ' ', IFNULL(usr.apellidoMaterno, '')) as Planeador,
            CONCAT(IFNULL(lv_aut.nombre, ''), ' ', IFNULL(lv_aut.apellidoPaterno, ''), ' ', IFNULL(lv_aut.apellidoMaterno, '')) as Autorizador,
            CONCAT(IFNULL(lv_pre.nombre, ''), ' ', IFNULL(lv_pre.apellidoPaterno, ''), ' ', IFNULL(lv_pre.apellidoMaterno, '')) as Predespegador,
            date_format(lv_aut.fecha_actualizacion, '%Y-%m-%d %H:%i') as FechaAutorizacion,
            date_format(lv_pre.fecha_actualizacion, '%Y-%m-%d %H:%i') as FechaPredespegue,
            token as Token,
            co2 as Co2,
            idCediDespega as IdCediDespega,
            idCediAterrizaje as IdCediAterrizaje,
            pr.idSesion as IdSesion,
            pr.Nombre as Proyecto,
            idVehiculo
        from
            viajes v
                left join
            (select 
                status, idviaje, idUsuario, nombre, apellidoPaterno, apellidoMaterno, fecha_actualizacion
            from
                log_viajes
            join usuarios USING (idUsuario)
            where
                status = 7) lv_aut USING (idViaje)
                left join
            (select 
                status, idviaje, idUsuario, nombre, apellidoPaterno, apellidoMaterno, fecha_actualizacion
            from
                log_viajes
            join usuarios USING (idUsuario)
            where
                status = 9) lv_pre USING (idViaje)
                join
            proyectos pr USING (idProyecto)
                left join
            usuarios usr ON v.idPlaneador = usr.idUsuario
            WHERE v.status = _status
            AND v.FechaPredespegue BETWEEN DATE_SUB(NOW(), INTERVAL 2 week) AND NOW()
            AND v.idCedi = _id_cedi
            group by idViaje;
        ALTER TABLE _vw_viajes ADD PRIMARY KEY(idViaje);
    ELSE
        DROP TEMPORARY TABLE IF EXISTS _vw_viajes;
        CREATE TEMPORARY TABLE _vw_viajes
        select 
            v.status as EstatusViaje,
            idViaje as IdViaje,
            km as Km,
            date_format(fechaSalida, '%Y-%m-%d %H:%i') as FechaSalida,
            date_format(fechaRetorno, '%Y-%m-%d %H:%i') as FechaRetorno,
            tipoDistribucion as TipoDistribucion,
            v.idPlaneador as IdPlaneador,
            CONCAT(IFNULL(usr.nombre, ''), ' ', IFNULL(usr.apellidoPaterno, ''), ' ', IFNULL(usr.apellidoMaterno, '')) as Planeador,
            CONCAT(IFNULL(lv_aut.nombre, ''), ' ', IFNULL(lv_aut.apellidoPaterno, ''), ' ', IFNULL(lv_aut.apellidoMaterno, '')) as Autorizador,
            CONCAT(IFNULL(lv_pre.nombre, ''), ' ', IFNULL(lv_pre.apellidoPaterno, ''), ' ', IFNULL(lv_pre.apellidoMaterno, '')) as Predespegador,
            date_format(lv_aut.fecha_actualizacion, '%Y-%m-%d %H:%i') as FechaAutorizacion,
            date_format(lv_pre.fecha_actualizacion, '%Y-%m-%d %H:%i') as FechaPredespegue,
            token as Token,
            co2 as Co2,
            idCediDespega as IdCediDespega,
            idCediAterrizaje as IdCediAterrizaje,
            pr.idSesion as IdSesion,
            pr.Nombre as Proyecto,
            idVehiculo
        from
            viajes v
                left join
            (select 
                status, idviaje, idUsuario, nombre, apellidoPaterno, apellidoMaterno, fecha_actualizacion
            from
                log_viajes
            join usuarios USING (idUsuario)
            where
                status = 7) lv_aut USING (idViaje)
                left join
            (select 
                status, idviaje, idUsuario, nombre, apellidoPaterno, apellidoMaterno, fecha_actualizacion
            from
                log_viajes
            join usuarios USING (idUsuario)
            where
                status = 9) lv_pre USING (idViaje)
                join
            proyectos pr USING (idProyecto)
                left join
            usuarios usr ON v.idPlaneador = usr.idUsuario
            WHERE v.status = _status
            AND v.FechaAutorizacion BETWEEN DATE_SUB(NOW(), INTERVAL 2 week) AND NOW()
            AND v.idCedi = _id_cedi
            group by idViaje;
        ALTER TABLE _vw_viajes ADD PRIMARY KEY(idViaje);
    END IF;

    # operacion exitosa
	SELECT 'OK' AS result, '' AS data;
    
    #genero json de viajes
    SELECT 
		EstatusViaje,
		IdViaje,
		Km,
		FechaSalida,
		FechaRetorno,
		TipoDistribucion,
        IdPlaneador,
        Planeador,
		Autorizador,
		Predespegador,
		FechaAutorizacion,
		FechaPredespegue,
		Token,
		Co2,
		IdCediDespega,
		IdCediAterrizaje,
		Proyecto,
        IdSesion,
		idVehiculo
	FROM
		_vw_viajes;
        
	#Genero itinerario de viajes
    SELECT 
		idItinerario,
		actividad AS Evento,
		date_format(fechaActividadInicio, '%Y-%m-%d %H:%i') AS FechaInicial,
		date_format(fechaActividadFin, '%Y-%m-%d %H:%i') AS FechaFinal,
		duracion AS Duracion,
		tirada AS Tirada,
		idViaje AS IdViaje
	FROM
		itinerario
			JOIN
		_vw_viajes USING (IdViaje);
        
	#Genero información de los destinos
    SELECT 
		p.tiemServicio as DuracionServicio,
		p.tirada as NumTiradaPlan,
		p.idDestino as IdDestino,
        d.destino as Destino,
		date_format(p.fechaEntrega, '%Y-%m-%d %H:%i') as FechaEntregaPedido,
		date_format(p.fechaSalida, '%Y-%m-%d %H:%i') as FechaSalidaPedido,
        p.idParada,
        _v.IdViaje
	FROM
		paradas p
			JOIN
		_vw_viajes _v USING (IdViaje)
            JOIN
        destinos d USING (idDestino);
        
	#Obtengo la info de los pedidos
    SELECT 
		pedido as IdPedido,
		date_format(fechaDeVenta, '%Y-%m-%d %H:%i') as FechaDeVenta,
		vendedor as Vendedor,
		idDestinoTR2 as IdDestinoTr2,
        d.destino as DestinoTR2,
		tipoEnvio as TipoEnvio,
		tipoPedido as TipoPedido,
		tipoServicio as TipoServicio,
		producto as Productos,
		valor as ValorMercancia,
		piezas as Piezas,
		peso as Kg,
		volumen as m3,
		date_format(ventanaFechaIni, '%Y-%m-%d') as VentanaFechaIni,
		date_format(ventanaFechaFin, '%Y-%m-%d') as VentanaFechaFin,
		date_format(primerHorarioIni, '%H:%i') as PrimerHorarioIni,
		date_format(primerHorarioFin, '%H:%i') as PrimerHorarioFin,
		date_format(segundoHorarioIni, '%H:%i') as SegundoHorarioIni,
		date_format(segundoHorarioFin, '%H:%i') as SegundoHorarioFin,
		tiemDescarga as TiempoDescarga,
		date_format(ventanaTR2, '%Y-%m-%d %H:%i') as FechaEntregaTR2,
        idParada,
        _v.IdViaje,
        plan as Plan,
        idGrouping as idGrouping,
        idCediIntermedio as IdCEDIIntermedio,
        familia as Familia
	FROM
		pedidos p
			JOIN
		_vw_viajes _v USING (IdViaje)
            left join
		destinos d ON p.idDestinoTR2 = d.idDestino;
    
    #Obtengo la informacion del vehiculo
    SELECT
        v.IdViaje,
        idVehiculo,
		TipoVehiculo as TipoVehiculo,
		VolumenSeco as VolumenSeco,
        VolumenFrio as VolumenFrio,
        Peso as Peso,
        VolumenMixto as VolumenMixto,
        RendimientoThermo as RendimientoThermo,
        Rendimiento as Rendimiento,
        Talla as Talla,
        HuellaCarbono as HuellaCarbono,
        TipoCombustible as TipoCombustible,
        f.fletera
	FROM
		vehiculos
			JOIN
		_vw_viajes v USING (idVehiculo)
            JOIN
        fleteras f USING (idFletera);
END //

call sp_api_obtener_viajes_insumos_por_status(16, 9)