-- --------------------------------------------------------------------------------
-- RACH, viernes 17 de mayo de 2019 (GMT-6)
-- Note: para predespegar un viaje
-- --------------------------------------------------------------------------------

drop procedure if exists sp_api_arms_predespegar_viaje;

DELIMITER // 

CREATE PROCEDURE `sp_api_arms_predespegar_viaje`(
	_id_viaje int, 
	_token varchar(7),
    _id_usuario int)
BEGIN	
    
    DECLARE _msg TEXT;
    DECLARE _resultado TEXT;
	DECLARE _code CHAR(5) DEFAULT '00000';


	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN

		GET DIAGNOSTICS CONDITION 1
			_code = RETURNED_SQLSTATE,_msg = MESSAGE_TEXT;

        SELECT 'FAIL' AS result, _msg AS data;

	END;    

	SET SESSION group_concat_max_len = 1000000;
    
    SET SESSION time_zone = 'America/Mexico_City';
    
    if not exists (		-- valido viaje
		SELECT 
            idViaje
        FROM
            viajes
        WHERE
            idViaje = _id_viaje 
            AND status < 9
				) then
		signal sqlstate '45000' set message_text = 'El viaje no existe';

	end if;

    if EXISTS(
        SELECT idViaje
        FROM 
            viajes
        WHERE
            token = _token
            and status > 9.5
    )THEN
        UPDATE viajes
        set token = ''
        WHERE
            token = _token
            and status > 9.5;
    END IF;
        

    UPDATE 
        viajes 
    set 
        status = 9,
        token = _token,
        idUsuario = _id_usuario,
        idPredespegador = _id_usuario,
        fechaPredespegue = NOW()
    WHERE 
        idViaje = _id_viaje;
    

    SELECT 'OK' as result, '' as data; 
        
END //