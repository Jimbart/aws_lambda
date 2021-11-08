-- --------------------------------------------------------------------------------
-- EDUARDO lunes, 15 julio 2019 (GMT-5)
-- Note: para autorizar los viajes
-- --------------------------------------------------------------------------------

DROP PROCEDURE if exists sp_api_arms_autorizar_viajes2;

delimiter //

CREATE PROCEDURE `sp_api_arms_autorizar_viajes2`(
    _idCedi INT,
    _idUsuario INT,
    _idPlaneador INT,
    _idSesion varchar(50),
    _proyecto varchar(60),
	_viajes json,
    _idProyecto INT)
BEGIN
    -- datos de vehiculo
    DECLARE _fletera VARCHAR(100);
    DECLARE _idFletera INT;
    DECLARE _tipoVehiculo VARCHAR(45);
    DECLARE _rendimiento float;
    DECLARE _talla VARCHAR(1);
    DECLARE _huellaCarbono float;
    DECLARE _tipoCombustible VARCHAR(45);
    DECLARE _volumenSeco float;
    DECLARE _volumenFrio float;
    DECLARE _pesoVehiculo float;
    DECLARE _rendimientoThermo float;

    -- datos del viaje
	DECLARE _idViaje int(11);
    DECLARE _co2 float;
    DECLARE _costoFerry double;
    DECLARE _costoHotel double;
    DECLARE _costoThermo double;
    DECLARE _costoComidas double;
    DECLARE _costoCombustible double;
    DECLARE _status int(11);
    DECLARE _ocupacionVolumenSeco float;
    DECLARE _ocupacionVolumenFrio float;
    DECLARE _ocupacionVolumenTotal float;
    DECLARE _ocupacionPesoSeco float;
    DECLARE _ocupacionPesoFrio float;
    DECLARE _ocupacionPesoTotal float;
    DECLARE _token varchar(6);
    DECLARE _idEmpresa int(11);
    DECLARE _idVehiculo int(11);
    DECLARE _tiradas int(11);
    DECLARE _pedidos int(11);
    DECLARE _pesoViaje float;
    DECLARE _volumenViaje float;
    DECLARE _valorViaje float;
    DECLARE _piezasViaje float;
    DECLARE _litrosCombustible float;
    DECLARE _litrosThemo float;
    DECLARE _costoTotal double;
    DECLARE _infraccionReglaPeso tinyint(1);
    DECLARE _infraccionReglaVolumen tinyint(1);
    DECLARE _infraccionReglaValor tinyint(1);
    DECLARE _infraccionReglaAcceso tinyint(1);
    DECLARE _infraccionDestinoDedicado tinyint(1);
    DECLARE _infraccionVentanaEntrega tinyint(1);
    DECLARE _infraccionEntrega1Dia tinyint(1);
    DECLARE _infraccionDiasApertura tinyint(1);
    DECLARE _infraccionCompatibilidad tinyint(1);
    DECLARE _infraccion tinyint(1);
    DECLARE _numPernoctas int(11);
    DECLARE _numComidas int(11);
    DECLARE _numDescansos int(11);
    DECLARE _numTrayectos int(11);
    DECLARE _numEsperas int(11);
    DECLARE _numRegiones int(11);
    DECLARE _numEntidades int(11);
    DECLARE _numDestinosTR1 int(11);
    DECLARE _numDestinosTR2 int(11);
    DECLARE _tiempoTotalViaje int(11);
    DECLARE _tiempoTotalTrayecto int(11);
    DECLARE _tiempoTotalDescansos int(11);
    DECLARE _tiempoTotalEspera int(11);
    DECLARE _tiempoTotalComidas int(11);
    DECLARE _tiempoTotalPernocta int(11);
    DECLARE _tiempoTotalServicio int(11);
    DECLARE _fechaSalidaViaje datetime;
    DECLARE _fechaRetornoViaje datetime;
    DECLARE _tipoDistribucion varchar(3);
    DECLARE _km float;
    DECLARE _idCediDespega int(11);
    DECLARE _idCediAterrizaje int(11);
    DECLARE _idViajeSesion varchar(15);

    -- datos del pedido
    DECLARE _idPedidos int(11);
    DECLARE _pedido varchar(70);
    DECLARE _tipoPedido varchar(45);
    DECLARE _piezas int(11);
    DECLARE _volumen float;
    DECLARE _peso float;
    DECLARE _valor float;
    DECLARE _producto varchar(45);
    DECLARE _tiemDescarga int(11);
    DECLARE _tipoEnvio tinyint(1);
    DECLARE _plan varchar(200);
    DECLARE _familia varchar(100);
    DECLARE _idDestinoTR2 int(11) unsigned;
    DECLARE _viajeTR2 int(11) unsigned;
    DECLARE _idParadaTR2 int(11);
    DECLARE _vendedor varchar(45);
    DECLARE _tipoServicio varchar(45);
    DECLARE _fechaDeVenta datetime;
    DECLARE _ventanaTR2 datetime;
    DECLARE _idCediIntermedio int(11);
    DECLARE _ventanaFechaIni date;
    DECLARE _ventanaFechaFin date;
    DECLARE _primerHorarioIni time;
    DECLARE _primerHorarioFin time;
    DECLARE _segundoHorarioIni time;
    DECLARE _segundoHorarioFin time;
    DECLARE _idGrouping int;

    -- datos de la parada
    DECLARE _idParada int(11);
    DECLARE _idDestino int(11);
    DECLARE _fechaEntrega datetime;
    DECLARE _fechaSalida datetime;
    DECLARE _tirada int(11);
    DECLARE _tiemServicio int(11);

    -- datos de las actividades
    DECLARE _actividad varchar(45);
    DECLARE _fechaActividadInicio datetime;
    DECLARE _fechaActividadFin datetime;
    DECLARE _duracion int(11);

    -- datos del sp
    DECLARE _iViajes INT;
    DECLARE _iViaje INT;
    DECLARE _iItinerarios INT;
    DECLARE _iItinerario INT;
    DECLARE _iParadas INT;
    DECLARE _iParada INT;
    DECLARE _iPedidos INT;
    DECLARE _iPedido INT;

    DECLARE _hoy DATETIME;
    DECLARE _idsViajes json;
	
    DECLARE _message_text TEXT;

	DECLARE _msg TEXT;
	DECLARE _code CHAR(5) DEFAULT '00000';

	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN

		GET DIAGNOSTICS CONDITION 1
			_code = RETURNED_SQLSTATE,_msg = MESSAGE_TEXT;

		ROLLBACK;
        SELECT 'FAIL' AS result, _msg AS data;

	END;

	SET SESSION time_zone = 'America/Mexico_City';

    SET _hoy = NOW();

    START TRANSACTION;
        if not exists (		-- valido el proyecto
                SELECT 
                    cedis
                FROM
                    cedis
                WHERE
                    idCedi = _idCedi
                        ) then
                signal sqlstate '45000' set message_text = 'El cedi no existe';
        END IF;
        IF _idProyecto = 0 THEN
            -- Crear un nuevo proyecto
            INSERT INTO proyectos (nombre, fechaCreacion, fechaActualizacion, idCedi, idSesion)
                VALUES(_proyecto, now(), now(), _idCedi, _idSesion);
            SET _idProyecto = (select last_insert_id());
        elseif not exists (		-- valido el proyecto
                SELECT 
                    nombre
                FROM
                    proyectos
                WHERE
                    idProyecto = _idProyecto
                    and idCedi = _idCedi
                        ) then
                signal sqlstate '45000' set message_text = 'El proyecto no existe';
        END IF;


        DROP temporary table if EXISTS _json_viajes;
        CREATE temporary table _json_viajes(v json);

        INSERT INTO _json_viajes (v)
        VALUES(_viajes);

        select v from _json_viajes;

        select JSON_LENGTH(_viajes);
        select JSON_LENGTH(select v from _json_viajes);
        -- SET _iViajes = JSON_LENGTH(_viajes);
        -- SET _iViaje = 0;

        -- DROP temporary TABLE IF EXISTS _viajes_reales;
        -- CREATE temporary TABLE _viajes_reales(idViajeSesion int, idViajeDB int);

        -- WHILE _iViaje < _iViajes DO
        --     SET _fletera = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].Vehiculo.Fletera')));
        --     SET _tipoVehiculo = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].Vehiculo.TipoVehiculo')));
        --     SET _rendimiento = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].Vehiculo.Rendimiento')));
        --     SET _talla = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].Vehiculo.Talla')));
        --     SET _huellaCarbono = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].Vehiculo.HuellaCarbono')));
        --     SET _tipoCombustible = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].Vehiculo.TipoCombustible')));
        --     SET _volumenSeco = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].Vehiculo.VolumenSeco')));
        --     SET _volumenFrio = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].Vehiculo.VolumenFrio')));
        --     SET _pesoVehiculo = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].Vehiculo.Peso')));
        --     SET _rendimientoThermo = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].Vehiculo.RendimientoThermo')));

        --     if not exists (		-- valido la fletera
        --         SELECT 
        --             idFletera
        --         FROM
        --             fleteras
        --         WHERE
        --             fletera = _fletera AND idCedi = _idCedi
        --         ) 
        --     THEN
        --         INSERT INTO fleteras (fletera, idCedi) VALUES (_fletera, _idCedi);
        --     END IF;

        --     SET _idFletera = (SELECT idFletera FROM fleteras WHERE fletera = _fletera AND idCedi = _idCedi);
            
        --     -- SI EXISTE EL VEHICULO
        --     IF EXISTS (
        --         SELECT idVehiculo FROM vehiculos WHERE idFletera = _idFletera AND TipoVehiculo LIKE _tipoVehiculo
        --     ) 
        --     -- OBTENGO EL ID Y ACTUALIZO LOS DATOS
        --     THEN
        --         SET _idVehiculo = (SELECT idVehiculo FROM vehiculos WHERE idFletera = _idFletera AND TipoVehiculo LIKE _tipoVehiculo);
        --         UPDATE vehiculos 
        --         SET Rendimiento = _rendimiento, Talla = _talla, HuellaCarbono = _huellaCarbono, 
        --             TipoCombustible = _tipoCombustible, VolumenSeco = _volumenSeco, 
        --             VolumenFrio = _volumenFrio, Peso = _pesoVehiculo, RendimientoThermo = _rendimientoThermo
        --         WHERE idVehiculo = _idVehiculo;
        --     -- SI NO, CREO UN NUEVO TIPO DE VEHICULO
        --     ELSE
        --         INSERT INTO vehiculos (idCedi, TipoVehiculo, Rendimiento, Talla, HuellaCarbono, TipoCombustible, VolumenSeco, VolumenFrio, Peso, RendimientoThermo, idFletera)
        --         VALUES (_idCedi, _tipoVehiculo, _rendimiento, _talla, _huellaCarbono, _tipoCombustible, _volumenSeco, _volumenFrio, _pesoVehiculo, _rendimientoThermo, _idFletera);
        --         SET _idVehiculo = (SELECT LAST_INSERT_ID());
        --     END IF;

        --     SET _co2 = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].Co2')));
        --     SET _costoFerry = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].CostoFerries')));
        --     SET _costoHotel = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].CostoPernocta')));
        --     SET _costoThermo = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].CostoThermo')));
        --     SET _costoComidas = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].CostoAlimento')));
        --     SET _costoCombustible = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].CostoCombustible')));
        --     SET _status = 7;
        --     SET _ocupacionVolumenSeco = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].OcupacionVolumenSeco')));
        --     SET _ocupacionVolumenFrio = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].OcupacionVolumenFrio')));
        --     SET _ocupacionVolumenTotal = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].OcupacionVolumenTotal')));
        --     SET _ocupacionPesoSeco = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].OcupacionPesoSeco')));
        --     SET _ocupacionPesoFrio = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].OcupacionPesoFrio')));
        --     SET _ocupacionPesoTotal = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].OcupacionPesoTotal')));
        --     SET _token = NULL;
        --     -- SET _idVehiculo = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].IdVehiculo')));
        --     SET _tiradas = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].Tiradas')));
        --     SET _pedidos = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].RecuentoPedidos')));
        --     SET _pesoViaje = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].Peso')));
        --     SET _volumenViaje = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].Volumen')));
        --     SET _valorViaje = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].Valor')));
        --     SET _piezasViaje = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].Piezas')));
        --     SET _litrosCombustible = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].RecuentoCombustible')));
        --     SET _litrosThemo = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].RecuentoThermo')));
        --     SET _costoTotal = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].CostoTotal')));
        --     SET _infraccionReglaPeso = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].InfraccionReglaPeso')));
        --     SET _infraccionReglaVolumen = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].InfraccionReglaVolumen')));
        --     SET _infraccionReglaValor = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].InfraccionReglaValor')));
        --     SET _infraccionReglaAcceso = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].InfraccionReglaAcceso')));
        --     SET _infraccionDestinoDedicado = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].InfraccionDestinoDedicado')));
        --     SET _infraccionVentanaEntrega = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].InfraccionVentanaEntrega')));
        --     SET _infraccionEntrega1Dia = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].InfraccionEntrega1Dia')));
        --     SET _infraccionDiasApertura = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].InfraccionDiasApertura')));
        --     SET _infraccionCompatibilidad = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].InfraccionCompatibilidad')));
        --     SET _infraccion = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].Infraccion')));
        --     SET _numPernoctas = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].NumPernoctas')));
        --     SET _numComidas = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].NumComidas')));
        --     SET _numDescansos = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].NumDescansos')));
        --     SET _numTrayectos = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].NumTrayectos')));
        --     SET _numEsperas = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].NumEsperas')));
        --     SET _numRegiones = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].NumRegiones')));
        --     SET _numEntidades = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].NumEntidades')));
        --     SET _numDestinosTR1 = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].NumDestinosTR1')));
        --     SET _numDestinosTR2 = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].NumDestinosTR2')));
        --     SET _tiempoTotalViaje = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].TiempoTotalViaje')));
        --     SET _tiempoTotalTrayecto = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].TiempoTotalTrayecto')));
        --     SET _tiempoTotalDescansos = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].TiempoTotalDescansos')));
        --     SET _tiempoTotalEspera = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].TiempoTotalEspera')));
        --     SET _tiempoTotalComidas = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].TiempoTotalComidas')));
        --     SET _tiempoTotalPernocta = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].TiempoTotalPernocta')));
        --     SET _tiempoTotalServicio = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].TiempoTotalServicio')));
        --     SET _fechaSalidaViaje = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].FechaSalida')));
        --     SET _fechaRetornoViaje = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].FechaRetorno')));
        --     SET _tipoDistribucion = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].TipoDistribucion')));
        --     SET _km = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].Km')));
        --     SET _idCediDespega = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].IdCEDIDespega')));
        --     SET _idCediAterrizaje = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].IdCEDIAterriza')));
        --     SET _idViajeSesion = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].IdViaje')));

        --     SET _idEmpresa = (SELECT idEmpresa from cedis where idCedi = _idCedi);
            
        --     if not exists (		-- valido el proyecto
        --             SELECT 
        --                 idVehiculo
        --             FROM
        --                 vehiculos
        --             WHERE
        --                 idVehiculo = _idVehiculo
        --                     ) then
        --             signal sqlstate '45000' set message_text = 'El vehiculo no existe';
        --     END IF;

        --     INSERT INTO viajes (co2,
        --                         costoFerry,
        --                         costoHotel,
        --                         costoThermo,
        --                         costoComidas,
        --                         costoCombustible,
        --                         status,
        --                         ocupacionVolumenSeco,
        --                         ocupacionVolumenFrio,
        --                         ocupacionVolumenTotal,
        --                         ocupacionPesoSeco,
        --                         ocupacionPesoFrio,
        --                         ocupacionPesoTotal,
        --                         token,
        --                         idCedi,
        --                         idEmpresa,
        --                         idVehiculo,
        --                         idProyecto,
        --                         tiradas,
        --                         pedidos,
        --                         peso,
        --                         volumen,
        --                         valor,
        --                         piezas,
        --                         litrosCombustible,
        --                         litrosThermo,
        --                         costoTotal,
        --                         infraccionReglaPeso,
        --                         infraccionReglaVolumen,
        --                         infraccionReglaValor,
        --                         infraccionReglaAcceso,
        --                         infraccionDestinoDedicado,
        --                         infraccionVentanaEntrega,
        --                         infraccionEntrega1Dia,
        --                         infraccionDiasApertura,
        --                         infraccionCompatibilidad,
        --                         infraccion,
        --                         numPernoctas,
        --                         numComidas,
        --                         numDescansos,
        --                         numTrayectos,
        --                         numEsperas,
        --                         numRegiones,
        --                         numEntidades,
        --                         numDestinosTR1,
        --                         numDestinosTR2,
        --                         tiempoTotalViaje,
        --                         tiempoTotalTrayecto,
        --                         tiempoTotalDescansos,
        --                         tiempoTotalEspera,
        --                         tiempoTotalComidas,
        --                         tiempoTotalPernocta,
        --                         tiempoTotalServicio,
        --                         fechaSalida,
        --                         fechaRetorno,
        --                         tipoDistribucion,
        --                         km,
        --                         idCediDespega,
        --                         idCediAterrizaje,
        --                         idUsuario,
        --                         idPlaneador,
        --                         idAutorizador,
        --                         fechaAutorizacion)
        --             VALUES(_co2,
        --                     _costoFerry,
        --                     _costoHotel,
        --                     _costoThermo,
        --                     _costoComidas,
        --                     _costoCombustible,
        --                     _status,
        --                     _ocupacionVolumenSeco,
        --                     _ocupacionVolumenFrio,
        --                     _ocupacionVolumenTotal,
        --                     _ocupacionPesoSeco,
        --                     _ocupacionPesoFrio,
        --                     _ocupacionPesoTotal,
        --                     _token,
        --                     _idCedi,
        --                     _idEmpresa,
        --                     _idVehiculo,
        --                     _idProyecto,
        --                     _tiradas,
        --                     _pedidos,
        --                     _pesoViaje,
        --                     _volumenViaje,
        --                     _valorViaje,
        --                     _piezasViaje,
        --                     _litrosCombustible,
        --                     _litrosThemo,
        --                     _costoTotal,
        --                     _infraccionReglaPeso,
        --                     _infraccionReglaVolumen,
        --                     _infraccionReglaValor,
        --                     _infraccionReglaAcceso,
        --                     _infraccionDestinoDedicado,
        --                     _infraccionVentanaEntrega,
        --                     _infraccionEntrega1Dia,
        --                     _infraccionDiasApertura,
        --                     _infraccionCompatibilidad,
        --                     _infraccion,
        --                     _numPernoctas,
        --                     _numComidas,
        --                     _numDescansos,
        --                     _numTrayectos,
        --                     _numEsperas,
        --                     _numRegiones,
        --                     _numEntidades,
        --                     _numDestinosTR1,
        --                     _numDestinosTR2,
        --                     _tiempoTotalViaje,
        --                     _tiempoTotalTrayecto,
        --                     _tiempoTotalDescansos,
        --                     _tiempoTotalEspera,
        --                     _tiempoTotalComidas,
        --                     _tiempoTotalPernocta,
        --                     _tiempoTotalServicio,
        --                     _fechaSalidaViaje,
        --                     _fechaRetornoViaje,
        --                     _tipoDistribucion,
        --                     _km,
        --                     _idCediDespega,
        --                     _idCediAterrizaje,
        --                     _idUsuario,
        --                     _idPlaneador,
        --                     _idUsuario,
        --                     _hoy);
        --     SET _idViaje = (SELECT last_insert_id());

        --     -- RELACION ID DE ID VIAJE SESION CON ID VIAJE DB
        --     INSERT INTO _viajes_reales (idViajeSesion, idViajeDB) VALUES (_idViajeSesion, _idViaje);

        --     -- registramos el itinerario
        --     SET _iItinerarios = JSON_LENGTH(_viajes, CONCAT('$[',_iViaje,'].Itinerario'));
        --     SET _iItinerario = 0;
        --     WHILE _iItinerario < _iItinerarios DO
        --         SET _actividad = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].Itinerario[',_iItinerario,'].Evento')));
        --         SET _fechaActividadInicio = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].Itinerario[',_iItinerario,'].FechaInicial')));
        --         SET _fechaActividadFin = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].Itinerario[',_iItinerario,'].FechaFinal')));
        --         SET _duracion = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].Itinerario[',_iItinerario,'].Duracion')));
        --         SET _tirada = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].Itinerario[',_iItinerario,'].Tirada')));

        --         INSERT INTO itinerario (actividad,
        --                                 fechaActividadInicio,
        --                                 fechaActividadFin,
        --                                 duracion,
        --                                 tirada,
        --                                 idViaje)
        --                 VALUES(_actividad,
        --                         _fechaActividadInicio,
        --                         _fechaActividadFin,
        --                         _duracion,
        --                         _tirada,
        --                         _idViaje);
        --         SET _iItinerario = _iItinerario + 1;
        --     END WHILE;
        --     -- recorremos el nodo de destinos
        --     SET _iParadas = JSON_LENGTH(_viajes, CONCAT('$[',_iViaje,'].Destinos'));
        --     SET _iParada = 0;
        --     WHILE _iParada < _iParadas DO
        --         SET _idDestino = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].Destinos[',_iParada,'].IdDestino')));
        --         SET _tirada = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].Destinos[',_iParada,'].NumTiradaPlan')));
        --         SET _fechaSalida = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].Destinos[',_iParada,'].FechaSalidaPedido')));
        --         SET _fechaEntrega = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].Destinos[',_iParada,'].FechaEntregaPedido')));
        --         SET _tiemServicio = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].Destinos[',_iParada,'].DuracionServicio')));
        --         INSERT INTO paradas (
        --                     idDestino, 
        --                     fechaEntrega, 
        --                     fechaSalida,
        --                     tirada,
        --                     tiemServicio,
        --                     idViaje)
        --             VALUES(
        --                 _idDestino,
        --                 _fechaEntrega,
        --                 _fechaSalida,
        --                 _tirada,
        --                 _tiemServicio,
        --                 _idViaje
        --             );
        --         SET _idParada = (select last_insert_id());
        --         -- insert into pedidos
        --         SET _iPedidos = JSON_LENGTH(_viajes, CONCAT('$[',_iViaje,'].Destinos[',_iParada,'].Pedidos'));
        --         SET _iPedido = 0;
                
        --         WHILE _iPedido < _iPedidos DO
        --             SET _pedido = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].Destinos[',_iParada,'].Pedidos[',_iPedido,'].IdPedido')));
        --             SET _tipoPedido = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].Destinos[',_iParada,'].Pedidos[',_iPedido,'].TipoPedido')));
        --             SET _piezas = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].Destinos[',_iParada,'].Pedidos[',_iPedido,'].Piezas')));
        --             SET _volumen = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].Destinos[',_iParada,'].Pedidos[',_iPedido,'].M3')));
        --             SET _peso = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].Destinos[',_iParada,'].Pedidos[',_iPedido,'].Kg')));
        --             SET _ventanaTR2 = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].Destinos[',_iParada,'].Pedidos[',_iPedido,'].FechaEntregaTR2')));
        --             SET _valor = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].Destinos[',_iParada,'].Pedidos[',_iPedido,'].ValorMercancia')));
        --             SET _producto = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].Destinos[',_iParada,'].Pedidos[',_iPedido,'].Productos')));
        --             SET _tiemDescarga = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].Destinos[',_iParada,'].Pedidos[',_iPedido,'].TiempoDescarga')));
        --             SET _tipoEnvio = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].Destinos[',_iParada,'].Pedidos[',_iPedido,'].TipoEnvio')));
        --             SET _plan = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].Destinos[',_iParada,'].Pedidos[',_iPedido,'].Plan')));
        --             SET _familia = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].Destinos[',_iParada,'].Pedidos[',_iPedido,'].Familia')));
        --             SET _idDestinoTR2 = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].Destinos[',_iParada,'].Pedidos[',_iPedido,'].IdDestinoTr2')));
        --             SET _vendedor = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].Destinos[',_iParada,'].Pedidos[',_iPedido,'].Vendedor')));
        --             SET _tipoServicio = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].Destinos[',_iParada,'].Pedidos[',_iPedido,'].TipoServicio')));
        --             SET _fechaDeVenta = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].Destinos[',_iParada,'].Pedidos[',_iPedido,'].FechaDeVenta')));
        --             SET _idCediIntermedio = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].Destinos[',_iParada,'].Pedidos[',_iPedido,'].IdCEDIIntermedio')));

        --             SET _ventanaFechaIni = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].Destinos[',_iParada,'].Pedidos[',_iPedido,'].VentanaFechaIni')));
        --             SET _ventanaFechaFin = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].Destinos[',_iParada,'].Pedidos[',_iPedido,'].VentanaFechaFin')));
        --             SET _primerHorarioIni = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].Destinos[',_iParada,'].Pedidos[',_iPedido,'].PrimerHorarioIni')));
        --             SET _primerHorarioFin = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].Destinos[',_iParada,'].Pedidos[',_iPedido,'].PrimerHorarioFin')));
        --             SET _segundoHorarioIni = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].Destinos[',_iParada,'].Pedidos[',_iPedido,'].SegundoHorarioIni')));
        --             SET _segundoHorarioFin = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].Destinos[',_iParada,'].Pedidos[',_iPedido,'].SegundoHorarioFin')));

        --             SET _idGrouping = JSON_UNQUOTE(JSON_EXTRACT(_viajes, CONCAT('$[',_iViaje,'].Destinos[',_iParada,'].Pedidos[',_iPedido,'].idGrouping')));

        --             INSERT INTO pedidos (pedido,
        --                                 tipoPedido,
        --                                 piezas,
        --                                 volumen,
        --                                 peso,
        --                                 valor,
        --                                 producto,
        --                                 tiemDescarga,
        --                                 tipoEnvio,
        --                                 plan,
        --                                 familia,
        --                                 idViaje,
        --                                 idCedi,
        --                                 idDestinoTR2,
        --                                 idProyecto,
        --                                 idParada,
        --                                 viajeTR2,
        --                                 idParadaTR2,
        --                                 vendedor,
        --                                 tipoServicio,
        --                                 fechaDeVenta,
        --                                 ventanaTR2,
        --                                 idCediIntermedio,
        --                                 ventanaFechaIni,
        --                                 ventanaFechaFin,
        --                                 primerHorarioIni,
        --                                 primerHorarioFin,
        --                                 segundoHorarioIni,
        --                                 segundoHorarioFin,
        --                                 idGrouping)
        --                     VALUES(_pedido,
        --                             _tipoPedido,
        --                             _piezas,
        --                             _volumen,
        --                             _peso,
        --                             _valor,
        --                             _producto,
        --                             _tiemDescarga,
        --                             _tipoEnvio,
        --                             _plan,
        --                             _familia,
        --                             _idViaje,
        --                             _idCedi,
        --                             _idDestinoTR2,
        --                             _idProyecto,
        --                             _idParada,
        --                             _viajeTR2,
        --                             _idParadaTR2,
        --                             _vendedor,
        --                             _tipoServicio,
        --                             _fechaDeVenta,
        --                             _ventanaTR2,
        --                             _idCediIntermedio,
        --                             _ventanaFechaIni,
        --                             _ventanaFechaFin,
        --                             _primerHorarioIni,
        --                             _primerHorarioFin,
        --                             _segundoHorarioIni,
        --                             _segundoHorarioFin,
        --                             _idGrouping);

        --             SET _iPedido = _iPedido + 1;
        --         END WHILE;
        --         SET _iParada = _iParada + 1;
        --     END WHILE;
        --     SET _iViaje = _iViaje + 1;
        -- END WHILE;

        -- SET _idsViajes = (SELECT JSON_ARRAYAGG(
        --     JSON_OBJECT(
        --             "idSesion", 
        --             idViajeSesion, 
        --             "idDB", 
        --             idViajeDB
        --         ))
        --         FROM
        --         _viajes_reales
        -- );

	COMMIT;

    SELECT 'OK' AS result, DATE_FORMAT(_hoy, '%Y-%m-%d %H:%i:%s') AS "Fecha Autorizacion", _idsViajes AS idsViajes;
END //
