-- --------------------------------------------------------------------------------
-- RAQUEL viernes, 26 de enero de 2018 (GMT-5)
-- Note: para obtener configuracion del cedi (incidentes activo)
-- --------------------------------------------------------------------------------


DROP PROCEDURE if exists `sp_api_incidentes_activo`;

delimiter //

CREATE PROCEDURE `sp_api_incidentes_activo`(
    _idViaje int)
BEGIN

    declare _json_incidente JSON;

    DECLARE _msg TEXT;
    DECLARE _resultado TEXT;
	DECLARE _code CHAR(5) DEFAULT '00000';

	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN

		GET DIAGNOSTICS CONDITION 1
			_code = RETURNED_SQLSTATE,_msg = MESSAGE_TEXT;

		ROLLBACK;
        SELECT 'FAIL' AS result, _msg AS data;

	END;

	SET SESSION time_zone = 'America/Mexico_City';

    START TRANSACTION;

        set _json_incidente =
                (select
					json_object(
						'NotificarIncidentes', cf.activa
						,'idCedi', cf.idCedi
					) as row1
				from
					viajes v
					join cedi_funciones cf using(idCedi)
				where
					v.idViaje = _idViaje
					and cf.nombre_funcion = 'notificaciones real time');

        if (
            JSON_LENGTH(_json_incidente) = 0
            ) then

            SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'El cedi no esta registrado en la tabla cedi_configuracion';
		end if;




    SELECT 'OK' AS result, _json_incidente AS json_incidente;
END //


call sp_api_incidentes_activo(83)