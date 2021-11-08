-- --------------------------------------------------------------------------------
-- RACH, jueves 14 de marzo de 2019 (GMT-6)
-- Note: para hacer login para ARMS3.6
-- --------------------------------------------------------------------------------

drop procedure if exists sp_api_arms_login;

DELIMITER // 

CREATE PROCEDURE `sp_api_arms_login`(
	_cedis varchar(100), 
	_correo varchar(100))
BEGIN	
    
    DECLARE _msg TEXT;
    DECLARE _resultado TEXT;
	DECLARE _code CHAR(5) DEFAULT '00000';

    DECLARE json_usuario JSON;

	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN

		GET DIAGNOSTICS CONDITION 1
			_code = RETURNED_SQLSTATE,_msg = MESSAGE_TEXT;

        SELECT 'FAIL' AS result, _msg AS data;

	END;    

	SET SESSION group_concat_max_len = 1000000;
    
    SET SESSION time_zone = 'America/Mexico_City';
    
    if not exists (		-- valido usuario
		SELECT 
            u.idUsuario
        FROM
            usuarios u
                JOIN
            cedis_has_usuarios USING (idUsuario)
                JOIN
            cedis c USING (idCedi)
        WHERE
            cedis = _cedis
                AND u.correo = _correo
                AND activo = 1
				) then
		signal sqlstate '45000' set message_text = 'Usuario invalido';

	end if;

    set json_usuario = (SELECT 
                                JSON_OBJECT(
                                    'idUsuario', dat.idUsuario,
                                    'idEmpresa', dat.idEmpresa) AS json_plan
                            FROM
                                (SELECT 
                                    idUsuario, idEmpresa
                                FROM
                                    usuarios
                                        JOIN
                                    cedis_has_usuarios USING (idUsuario)
                                        JOIN
                                    cedis USING (idCedi)
                                WHERE
                                    correo = _correo
                                GROUP BY idUsuario) dat
                            );

    SELECT 'OK' as result, json_usuario as data; 
        
END //



call sp_api_arms_login('SILODISA', 'rantonio@arete.ws');