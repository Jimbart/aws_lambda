-- --------------------------------------------------------------------------------
-- Eduardo, 13 de septiembre de 2019 (GMT-6)
-- Note: para obtener los viajes que se encuentran en tránsito
-- --------------------------------------------------------------------------------

drop procedure if exists sp_api_tracking_obtener_viajes_transito;

DELIMITER // 

CREATE PROCEDURE `sp_api_tracking_obtener_viajes_transito`(
	_cedis VARCHAR(100)
    )

BEGIN	
    DECLARE _msg TEXT;  
	DECLARE _code CHAR(5) DEFAULT '00000';
    DECLARE _viajes JSON;

	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN
		
		GET DIAGNOSTICS CONDITION 1
			_code = RETURNED_SQLSTATE,_msg = MESSAGE_TEXT;
        
		SELECT 'FAIL' AS result, _msg AS data;
            
	END;

	SET SESSION group_concat_max_len = 100000000;
    
    SET SESSION time_zone = 'America/Mexico_City';

    SET _viajes = (
        SELECT 
            CONCAT(
                '[',
                GROUP_CONCAT(
                    json_object(
                        'idCedi', idCedi, 'idViajes', CONCAT('[', viajes, ']')
                    )
                ),
                ']'
            )
        FROM(
            SELECT 
                idCedi,
                GROUP_CONCAT(CONCAT(idViaje)) viajes
            FROM 
                viajes
            WHERE
                status = 9
            AND
                FIND_IN_SET(idCedi, _cedis)
            GROUP BY
                idCedi
        ) _viajes
    );

    SELECT 'OK' as result, _viajes as data;
END //

-- call sp_api_tracking_obtener_viajes_transito ('4,16,81,91');