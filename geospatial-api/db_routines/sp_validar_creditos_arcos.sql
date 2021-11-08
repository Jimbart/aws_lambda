-- --------------------------------------------------------------------------------
-- RAQUEL viernes, 14 de junio de 2019 (GMT-6)
-- Note: para validar los creditos por empresa y si un usuario tiene acceso al calculo de arcos
-- EDUARDO GILES, 25 de junio de 2019 (GMT-6)
-- Note: extraido de las apis del 3.6 y modificado para el 4
-- --------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS `sp_validar_creditos_arcos`;

DELIMITER // 

CREATE PROCEDURE `sp_validar_creditos_arcos`(
    _idUsuario int
    , _idEmpresa INT
    , _arcos INT
)
BEGIN
    DECLARE _arcos_calculados INT;
    DECLARE _primer_dia_mes DATE;
    DECLARE _ultimo_dia_mes DATE;

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


	if not exists (		-- valido usuario con permisos para calcular arcos
		SELECT 
            u.idUsuario
        FROM
            usuarios u
                JOIN
            tipo_usuarios tuf USING (tipo_usuario)
        WHERE
            u.idUsuario = _idUsuario
			and tuf.funcion = 'calcular_arcos'
				) then
		signal sqlstate '45000' set message_text = 'El usuario no tiene permisos para calcular distancias';

	end if;

    -- obtengo los arcos usados
    SET _primer_dia_mes = (SELECT ADDDATE(LAST_DAY(SUBDATE(NOW(), INTERVAL 1 MONTH)), 1));
    SET _ultimo_dia_mes = (SELECT LAST_DAY(NOW()));

    set _arcos_calculados = (SELECT 
                                SUM(ArcosCalculados)
                            FROM
                                log_arcos
                            WHERE
                                idEmpresa = _idEmpresa
                                    AND DATE(Fecha) BETWEEN _primer_dia_mes AND _ultimo_dia_mes
                            );
    

    if not exists (		-- valido el limite de creditos y si el api esta activa
		SELECT 
            Limite
        FROM
            apis_empresas
        WHERE
            Activo = 1
                AND FechaExpiracion > NOW()
                AND IdEmpresa = _idEmpresa
                AND Limite > ifnull(_arcos_calculados, 0)
                AND Nombre = 'Distance Matrix'
                AND (Limite - ifnull(_arcos_calculados, 0)) > _arcos
				) then
		signal sqlstate '45000' set message_text = 'No cuentas con la cantidad de creditos suficientes para calcular las distancias, comuniquese con soporte técnico';

	end if;

	
	SELECT 'OK' AS result, 
    CAST(AES_DECRYPT(API, '5T3J\TQjVDq:(nsD') AS CHAR (70))
                    FROM
                        apis_empresas 
                    WHERE
                        IdEmpresa = _idEmpresa;

END

-- call sp_validar_creditos_arcos(10, 1, 100)