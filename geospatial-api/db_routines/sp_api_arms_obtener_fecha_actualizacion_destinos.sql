-- --------------------------------------------------------------------------------
-- EDUARDO miercoles, 24 de Abril de 2019 (GMT-6)
-- Note: para obtener la fecha de actualizacion de un destino
-- Modificacion miercoler, 15 de Mayo de 2019
-- para obtener la fecha de actualizacion del cedi
-- --------------------------------------------------------------------------------

drop procedure if exists sp_api_arms_obtener_fecha_actualizacion_destinos;

DELIMITER //

CREATE PROCEDURE `sp_api_arms_obtener_fecha_actualizacion_destinos`(
	_idCedi int,
    _json_destinos json)
BEGIN

	declare _i int;
    declare _idDestino text;
    
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
    SET SESSION group_concat_max_len = 8000000;

    drop temporary table if exists _lats_lons;
    create temporary table _lats_lons (
		idDestino int(11),
		fechaActualizacion datetime
    );

	
    if JSON_LENGTH(ifnull(_json_destinos, '{}')) > 0 then
		set _i = 0;
        
		while _i < JSON_LENGTH(_json_destinos) do
			
            -- OBTENGO LAS LATITUDES Y LONGITUDES DE LA LISTA DE IDs DE DESTINOS QUE SE PASAN
			set _idDestino = JSON_UNQUOTE(JSON_EXTRACT(_json_destinos, CONCAT('$[',_i,']')));
            insert ignore into _lats_lons (idDestino, fechaActualizacion)
            select idDestino, fechaActualizacion FROM destinos WHERE idDestino = _idDestino;
            
			set _i = _i + 1;		-- siguiente

		end while;

		insert ignore into _lats_lons (idDestino, fechaActualizacion)
		select idCedi, fechaActualizacion FROM cedis WHERE idCedi = _idCedi;

    end IF;
    
    -- select * from _lats_lons;

    if _json_destinos is NOT null then		-- verifico vs nulos
        -- ARMO EL JSON DE RESPUESTA
        select
			'OK' AS result
			,cast(concat('['
					,ifnull(group_concat(json_destinos_lat_lon), '')
				, ']') as json) as data
		from (
			SELECT 
				JSON_OBJECT('idDestino',
						idDestino,
						'fechaActualizacion',
						fechaActualizacion) AS json_destinos_lat_lon
			FROM
				_lats_lons
		) dat;
    else
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'No existen destinos';
    end if;



END//

 -- call sp_api_arms_obtener_fecha_actualizacion_destinos(16, '[3847,3850,3851,3852,3853,3854,3855,3857,3858,3859]');