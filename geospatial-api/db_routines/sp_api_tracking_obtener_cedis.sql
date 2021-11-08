-- --------------------------------------------------------------------------------
-- Eduardo, 10 de septiembre de 2019 (GMT-6)
-- Note: para obtener los cedis de una empresa para el tracking
-- --------------------------------------------------------------------------------

drop procedure if exists sp_api_tracking_obtener_cedis;

DELIMITER // 

CREATE PROCEDURE `sp_api_tracking_obtener_cedis`(
	_correo VARCHAR(100)
    )

BEGIN	
    DECLARE _msg TEXT;  
	DECLARE _code CHAR(5) DEFAULT '00000';

    DECLARE _cedis VARCHAR(45);
    DECLARE _nombre_empresa VARCHAR(100);
    DECLARE _idUsuario INT;
    DECLARE _nombre_usuario VARCHAR(100);

	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN
		
		GET DIAGNOSTICS CONDITION 1
			_code = RETURNED_SQLSTATE,_msg = MESSAGE_TEXT;
        
		SELECT 'FAIL' AS result, _msg AS data;
            
	END;

	SET SESSION group_concat_max_len = 100000000;
    
    SET SESSION time_zone = 'America/Mexico_City';

    IF NOT EXISTS (
		SELECT 
            *
        FROM
            usuarios
        WHERE
            correo = _correo
        )
    THEN
		signal sqlstate '45000' set message_text = 'Usuario no registrado';
    ELSE
        SET _idUsuario = (SELECT idUsuario FROM usuarios WHERE correo = _correo);
    END IF;
    
    SET _cedis = (SELECT CONCAT('[', group_concat( DISTINCT(cu.idCedi) ), ']' )
    FROM usuarios u 
    JOIN cedis_has_usuarios cu USING (idUsuario)
    WHERE u.correo = _correo);

    SET _nombre_empresa = (SELECT DISTINCT(empresa) FROM empresas
                            JOIN cedis USING (idEmpresa)
                            JOIN cedis_has_usuarios USING (idCedi)
                            WHERE idUsuario = _idUsuario
                            LIMIT 1);

    SET _nombre_usuario = (SELECT CONCAT(IFNULL(nombre, ''), ' ', IFNULL(apellidoPaterno, ''), ' ', IFNULL(apellidoMaterno, ''))
                            FROM usuarios
                            WHERE idUsuario = _idUsuario);

    SELECT 'OK' as result, _cedis as cedis, _nombre_empresa as empresa, _nombre_usuario as usuario;
END //