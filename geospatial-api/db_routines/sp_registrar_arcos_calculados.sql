-- --------------------------------------------------------------------------------
-- RAQUEL viernes, 14 de junio de 2019 (GMT-6)
-- Note: para escribir el numero de arcos calculados
-- EDUARDO GILES, 25 de junio de 2019 (GMT-6)
-- Note: extraido de las apis del 3.6 y modificado para el 4
-- --------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS `sp_registrar_arcos_calculados`;

DELIMITER // 

CREATE PROCEDURE `sp_registrar_arcos_calculados`(
    _idUsuario int
    , _idEmpresa INT
    , _arcos INT
)
BEGIN


	DECLARE _msg TEXT;  
	DECLARE _code CHAR(5) DEFAULT '00000';

	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN
		
		GET DIAGNOSTICS CONDITION 1
			_code = RETURNED_SQLSTATE,_msg = MESSAGE_TEXT;
        
		SELECT 'FAIL' AS result, _msg AS data;
            
	END;       

	SET SESSION group_concat_max_len = 100000000;
    
    SET SESSION time_zone = 'America/Mexico_City';


	INSERT INTO log_arcos (idEmpresa, idUsuario, ArcosCalculados, Fecha) values (_idEmpresa, _idUsuario, _arcos, now());

	
	SELECT 'OK' AS result, 'Arcos calculados registrados correctamente' as data;

END

-- call sp_registrar_arcos_calculados(10, 1, 100)