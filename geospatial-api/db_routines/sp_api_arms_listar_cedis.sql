-- --------------------------------------------------------------------------------
-- RAQUEL lunes, 25 de marzo de 2019 (GMT-5)
-- Note, Para listar los cedis de un usuario
-- --------------------------------------------------------------------------------

drop procedure if exists sp_api_arms_listar_cedis;

DELIMITER //

CREATE PROCEDURE `sp_api_arms_listar_cedis`(
	   _correo VARCHAR(100))
BEGIN

    declare _json_cedis JSON;
    declare _json_empresa json;
	declare _json_cedis_empresa json;

    DECLARE _msg TEXT;
    DECLARE _resultado TEXT;
	DECLARE _code CHAR(5) DEFAULT '00000';

	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN

		GET DIAGNOSTICS CONDITION 1
			_code = RETURNED_SQLSTATE,_msg = MESSAGE_TEXT;

        SELECT 'FAIL' AS result, _msg AS data;

	END;

	SET SESSION time_zone = 'America/Mexico_City';
    SET SESSION group_concat_max_len = 4000000;
    
	if not exists (		-- verifico exista el correo
		select correo
        from
			usuarios
		where
			correo = _correo
	) then
		SIGNAL SQLSTATE '45000'
			SET MESSAGE_TEXT = 'El correo no está registrado';
	end if;

    

    SET _json_cedis = (select cast(concat('['
								,ifnull(group_concat(json_plan), '')
							, ']') as json) as data
					from (
						SELECT 
							JSON_OBJECT(
									'idCedi', 
									dat.idCedi,
									'nombre', 
									dat.cedis,
									'latitud', 
									dat.latitud,
									'longitud', 
									dat.longitud,
									'pertenece',
									1) AS json_plan
						FROM
							(SELECT 
								idCedi, 
								cedis,
								latitud,
								longitud
							FROM
								usuarios
							JOIN cedis_has_usuarios USING (idUsuario)
							JOIN cedis USING (idCedi)
							WHERE
								correo = _correo) dat
						) dat);

	SET _json_empresa = (SELECT 
                                JSON_OBJECT(
                                    'empresa', dat.empresa,
                                    'idEmpresa', dat.idEmpresa,
                                    'idUsuario', dat.idUsuario,
									'idCediPreferido', dat.idCediPreferido) AS json_plan
                            FROM(SELECT 
									empresa, idEmpresa, idUsuario, idCediPreferido
								FROM
									usuarios
								JOIN cedis_has_usuarios USING (idUsuario)
								JOIN cedis USING (idCedi)
								JOIN empresas USING (idEmpresa)
								WHERE
									correo = _correo
								limit 1)dat);

	DROP temporary TABLE IF EXISTS _empresas_tmp;
	CREATE temporary TABLE _empresas_tmp(idEmpresa int);

	INSERT INTO _empresas_tmp (idEmpresa) 
	SELECT 
		DISTINCT(idEmpresa)
	FROM
		usuarios
	JOIN cedis_has_usuarios USING (idUsuario)
	JOIN cedis USING (idCedi)
	JOIN empresas USING (idEmpresa)
	WHERE
		correo = _correo;
	
	SET _json_cedis_empresa = (select cast(concat('['
								,ifnull(group_concat(json_plan), '')
							, ']') as json) as data
					from (
						SELECT 
							JSON_OBJECT(
									'idCedi', 
									dat.idCedi,
									'nombre', 
									dat.cedis,
									'latitud', 
									dat.latitud,
									'longitud', 
									dat.longitud,
									'pertenece',
									0) AS json_plan
						FROM
							(SELECT 
								idCedi, 
								cedis,
								latitud,
								longitud
							FROM cedis 
							JOIN _empresas_tmp 
							USING (idEmpresa)) dat
						) dat);

    -- resultado
    if _json_cedis is null then		-- verifico vs nulos
		SIGNAL SQLSTATE '45000'
			SET MESSAGE_TEXT = 'El usuario no tiene cedis';

	else
		SELECT 'OK' AS result, _json_cedis AS data, _json_empresa as empresa, _json_cedis_empresa as cedis;

	end if;

END //

-- call sp_api_arms_listar_cedis('emgiles@arete.ws');