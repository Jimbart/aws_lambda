-- --------------------------------------------------------------------------------
-- EDUARDO lunes, 9 de septiembre de 2019
-- Note, Para desautorizar un viaje
-- --------------------------------------------------------------------------------
drop procedure if exists sp_api_arms_desautorizar_viaje;

DELIMITER //

CREATE PROCEDURE `sp_api_arms_desautorizar_viaje`(
       _id_viaje int,
       _id_usuario int)
BEGIN

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
    SET SESSION group_concat_max_len = 4000000;

    START TRANSACTION;
        IF NOT EXISTS (
            SELECT 
                idViaje
            FROM
                viajes
            WHERE
                idViaje = _id_viaje
            AND
                status = 7 -- VERIFICO QUE EL VIAJE ESTE AUTORIZADO
                    ) then
            signal sqlstate '45000' set message_text = 'El viaje no existe';
        END IF;

        DELETE FROM viajes
        WHERE
            idViaje = _id_viaje;
        
        INSERT INTO log_viajes (idViaje, fecha_actualizacion, idUsuario, status)
        VALUES (_id_viaje, NOW(), _id_usuario, 0);
    COMMIT;

    -- resultado
    SELECT 'OK' AS result, "Viaje desautorizado correctamente";


END//

-- call sp_api_arms_desautorizar_viaje (41, 10);