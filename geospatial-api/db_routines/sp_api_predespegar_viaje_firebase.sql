-- --------------------------------------------------------------------------------
-- RACH martes, 21 de mayo de 2019 (GMT-6)
-- para obtener el json que se inyecta hacia firebase
-- --------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS `sp_api_predespegar_viaje_firebase`;

DELIMITER //

CREATE PROCEDURE `sp_api_predespegar_viaje_firebase`(
	_idCedi int
    ,_idViaje int
)
BEGIN

    DECLARE _i, _j, _secuencia int default(1);
    DECLARE _json_v, _json_d, _json_p, _json_f, _json_i JSON;

	DECLARE _msg TEXT;  
    DECLARE _resultado TEXT;    
	DECLARE _code CHAR(5) DEFAULT '00000';
	DECLARE _idUsuario int;
    
	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN
		
		GET DIAGNOSTICS CONDITION 1
			_code = RETURNED_SQLSTATE,_msg = MESSAGE_TEXT;
            
        SELECT 'FAIL' AS result, _msg AS data;
        
	END;       
    
	SET SESSION time_zone = 'America/Mexico_City';
    SET SESSION group_concat_max_len = 1000000;

    IF NOT EXISTS (	-- verifico exista el viaje
		SELECT idViaje
        FROM 
			viajes v 
        WHERE
			v.idCedi = _idCedi 
			AND v.idViaje = _idViaje
	) THEN
		SIGNAL SQLSTATE '45000'
			SET MESSAGE_TEXT = 'el viaje no existe';
	END IF;

    # verifico si debo inyectar los pedidos como facturas
    IF EXISTS(
		SELECT 
			idFuncion
		FROM 
			viajes v
			JOIN cedi_funciones cf USING (idCedi)
		WHERE	
			v.idViaje = _idViaje
			and cf.nombre_funcion = 'usar pedidos como facturas'
			and cf.activa = 1    
	) then
		insert ignore into pedido_facturas (
			factura
			,idCedi
			,idPedido        
        )
		SELECT
			p.pedido
            ,_idCedi
			,p.idPedido
		FROM
			pedidos p
		WHERE
			p.idViaje = _idViaje;
            
    END IF;

    DROP TEMPORARY TABLE IF EXISTS _facturas;
    CREATE TEMPORARY TABLE _facturas (
		idFactura2 VARCHAR(45) PRIMARY KEY
        ,tirada int
		,inventario VARCHAR(45)
    );
    INSERT INTO _facturas (
        idFactura2
		,tirada
        ,inventario
    )
    SELECT
		pf.factura
        ,pa.tirada
        ,IFNULL(pf.valor1, IFNULL(p.tipoPedido, 'desconocido'))
	FROM
		pedidos p
        JOIN paradas pa USING (idParada)
		JOIN pedido_facturas pf USING(idPedido)
	WHERE
		p.idViaje = _idViaje
	GROUP BY
		pf.factura;			# para evitar duplicados de factura
        
	set _json_v = (		-- obtengo el JSON del viaje
		SELECT
			JSON_OBJECT(
				'idCedi', v.idCedi
				,'fecha-salida', v.fechaSalida
				,'fecha-retorno', v.fechaRetorno
				,'km', v.km
				,'tiempo-recorrido', HOUR(TIMEDIFF(v.fechaRetorno, v.fechaSalida))
                ,'cedi-origen-latitud', c.latitud
                ,'cedi-origen-longitud', c.longitud
                ,'cedi-destino-latitud', c.latitud
                ,'cedi-destino-longitud', c.longitud
                ,'id-viaje', v.idViaje
                ,'contactos', JSON_MERGE(
					JSON_OBJECT(1, JSON_OBJECT('cargo', 'n/d', 'nombre', IFNULL(c.contacto1, ''), 'telefono', 'n/d'))
					,JSON_OBJECT(2, JSON_OBJECT('cargo', 'n/d', 'nombre', IFNULL(c.contacto2, ''), 'telefono', 'n/d'))
						)
				,'facturas', JSON_MERGE(
					JSON_OBJECT('total', (SELECT count(idFactura2) FROM _facturas))
                    ,JSON_OBJECT('entregadas', 0)
						)        
			)
		FROM
			viajes v
            JOIN cedis c USING (idCedi)
		WHERE
			v.idViaje = _idViaje
	);
--     
	DROP TEMPORARY TABLE IF EXISTS _paradas;		-- obtengo los JSON de las paradas
	CREATE TEMPORARY TABLE _paradas (
		id int AUTO_INCREMENT PRIMARY KEY
        ,secuencia int
        ,idParada int
		,json_parada JSON
    );
	INSERT INTO _paradas(secuencia, idParada, json_parada)
	SELECT
		pa.tirada
		,pa.tirada
		,JSON_OBJECT(
			'id-destino', d1.idDestino
			,'destino-entrega', IFNULL(d1.alias, d1.destino)
			,'estado', IFNULL(d1.estado, 'n/d')
			,'fecha-llegada', pa.fechaEntrega
			,'fecha-entrega', pa.fechaEntrega
			,'fecha-salida', pa.fechaSalida
			,'latitud', d1.latitud
			,'longitud', d1.longitud
			,'pedidos', IFNULL(count(p.idPedido), 0)
			,'calle-numero', IFNULL(d1.calleNumero, 'n/d')
			,'ciudad', IFNULL(d1.municipio, 'n/d')
			,'colonia', IFNULL(d1.colonia, 'n/d')
			,'cp', IFNULL(d1.codigoPostal, 'n/d')
			,'tipo', 'n/d'
			,'status', 'sin visitar'
			,'contactos', JSON_MERGE(
				JSON_OBJECT(1, JSON_OBJECT('cargo', 'n/d', 'nombre', IFNULL(d1.contacto1, ''), 'telefono', IFNULL(d1.telefono1, '')))
				,JSON_OBJECT(2, JSON_OBJECT('cargo', 'n/d', 'nombre', IFNULL(d1.contacto2, ''), 'telefono', IFNULL(d1.telefono2, '')))
				,JSON_OBJECT(3, JSON_OBJECT('cargo', 'n/d', 'nombre', IFNULL(c.contacto1, ''), 'telefono', 'n/d'))
				,JSON_OBJECT(4, JSON_OBJECT('cargo', 'n/d', 'nombre', IFNULL(c.contacto2, ''), 'telefono', 'n/d'))
					)            
			,'ventana', IFNULL(concat(DATE_FORMAT(d1.ventanaLunVieIni1, '%H:%i'), ' - ',DATE_FORMAT(d1.ventanaLunVieFin1, '%H:%i')), 'abierta') -- verificar que ventanas se van a mostrar
			,'desfaseHorario', 0
			,'facturas', f.json_facturas
			,'entregaRecoleccion', if(IFNULL(tipoEnvio, 0) > 0, CAST(TRUE as JSON), CAST(FALSE as JSON))
		)        
	FROM
		viajes v
		JOIN pedidos p USING (idViaje)
        JOIN paradas pa USING (idParada)
		JOIN cedis c on v.idCedi = c.idCedi
		LEFT JOIN destinos d1 on pa.idDestino = d1.idDestino
		LEFT JOIN (
			SELECT
				tirada
				,cast(
					concat(
						'{'
						,group_concat(concat('"',idFactura2,'":', JSON_OBJECT('entregada', false, 'inventario', inventario)))
						,'}'
					) as json) as json_facturas
			FROM _facturas
			GROUP BY
				tirada) f USING (tirada)        
		LEFT JOIN (
			SELECT
				pa.tirada
			FROM
				pedidos pe
                JOIN paradas pa USING (idParada)
			WHERE
				pe.idViaje = _idViaje
				and IFNULL(pe.tipoEnvio, 1) = 0		-- 1 = entrega, 0 = recoleccion
			GROUP BY
				pa.tirada) er USING (tirada)                    
	WHERE
		v.idViaje = _idViaje
	GROUP BY
		pa.tirada;

	DROP TEMPORARY TABLE IF EXISTS _activities;
	CREATE TEMPORARY TABLE _activities (
		idActividad int auto_increment PRIMARY KEY
		,actividad VARCHAR(50)
        ,destino VARCHAR(45)
		,inicio datetime
		,fin datetime
	);
    
	# TODO: optimizar vista de actividades    
-- 	INSERT INTO _activities (
-- 		actividad
--         ,destino
-- 		,inicio
-- 		,fin
-- 	)
-- 	SELECT 
-- 		actividad
--         ,destino
-- 		,inicio
-- 		,fin
-- 	FROM vw_actividades
--     WHERE
-- 		idViaje = _idViaje
-- 	ORDER BY
-- 		inicio
-- 		,fin;

	DROP TEMPORARY TABLE IF EXISTS _activities2;
	CREATE TEMPORARY TABLE _activities2 like _activities;
	INSERT INTO _activities2 SELECT * FROM _activities;

	DROP TEMPORARY TABLE IF EXISTS _activities3;
	CREATE TEMPORARY TABLE _activities3 like _activities;
	INSERT INTO _activities3 SELECT * FROM _activities;
    
    set _json_i = (
		SELECT
			cast(
				concat('[',
					group_concat(
						concat(
							'{'
								,'"accion":"', actividad,'"'
								,',"fechaHoraInicio":"',DATE_FORMAT(inicio, '%Y-%m-%d %T.%f'),'"'
								,case
									when fin = inicio then ''
									else concat(',"fechaHoraFin":"',DATE_FORMAT(fin, '%Y-%m-%d %T.%f'),'"')
								end
								,case
									when IFNULL(destino, '') = '' then ''
									else concat(',"destino":"', destino, '"')
								end
							,'}'))
					,']') 
			as json)
		FROM (
				SELECT 				-- actividades generales
					actividad
					,destino
					,inicio
					,fin
				FROM _activities		
				union
				SELECT 				-- traslados
					'Traslado'
					,''
					,a.fin as inicio
					,b.inicio as fin
				FROM 
					_activities2 a 
					JOIN _activities3 b
					on 
						a.idActividad + 1 = b.idActividad
				union
				SELECT 				-- checkpoint llegada a destino
					'Aterriza'
					,IFNULL(d1.alias, d1.destino) as destinoTr1
					,pa.fechaEntrega as fecEntregaTr1
					,pa.fechaSalida as fecSalidaTr1
				FROM
					paradas pa
					JOIN pedidos p USING (idParada)        
					JOIN destinos d1 USING (idDestino)
				WHERE
					pa.idViaje = _idViaje
				GROUP BY
					pa.idViaje
					,pa.tirada
				union
				SELECT 				-- checkpoint salida de destino
					'Despega'
					,IFNULL(d1.alias, d1.destino) as destinoTr1
					,pa.fechaEntrega as fecEntregaTr1
					,pa.fechaSalida as fecSalidaTr1
				FROM
					paradas pa
					JOIN pedidos p USING (idParada)        
					JOIN destinos d1 USING (idDestino)
				WHERE
					pa.idViaje = _idViaje
				GROUP BY
					pa.idViaje
					,pa.tirada
				ORDER BY
					inicio
					,fin
			) dat
	);
   
	set _json_v = JSON_MERGE(_json_v, JSON_OBJECT('itinerario', _json_i));	    -- agrego itinerario
        
    -- consolido viaje con paradas
    set _j = (SELECT max(id)FROM _paradas);
    while _i <= _j do

		-- obtengo la parada actual y el id del viaje para comparar
        SELECT json_parada, secuencia into _json_p, _secuencia FROM _paradas WHERE id = _i;

        -- consolido las paradas
        if _i = 1 then
            set _json_d = JSON_OBJECT(_secuencia, _json_p);	-- inicializo
		else
            set _json_d = JSON_MERGE(_json_d, JSON_OBJECT(_secuencia, _json_p));
		END IF;

		set _i = _i + 1;		-- siguiente

    end while;

    -- agrego ultimo viaje
	set _json_v = JSON_MERGE(_json_v, JSON_OBJECT('destinos', _json_d));		-- agrego paradas

    -- resultado
	SELECT
		'OK' AS result
		,_json_v AS data;

END