-- --------------------------------------------------------------------------------
-- Eduardo jueves, 12 de septiembre de 2019 (GMT-5)
-- Note, Para actualizar los SS preferidos del usuario
-- --------------------------------------------------------------------------------

drop procedure if exists sp_api_arms_actualizar_ss_preferido_usuario;

DELIMITER //

CREATE PROCEDURE `sp_api_arms_actualizar_ss_preferido_usuario`(
       _id_cedi int,
       _id_usuario int,
       _spreads json)
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

	SET SESSION time_zone = 'America/Mexico_City';
    SET SESSION group_concat_max_len = 4000000;

    if not exists (		-- verifico exista el correo
		select *
        from
			cedis_has_usuarios
		where
            idCedi = _id_cedi
        AND
			idUsuario = _id_usuario
	) then
		SIGNAL SQLSTATE '45000'
			SET MESSAGE_TEXT = 'El usuario no está registrado';
	end if;

    DROP temporary table if EXISTS _json_ss_usuario;
    CREATE temporary table _json_ss_usuario(idUsuario int, p json);

    INSERT INTO _json_ss_usuario (idUsuario, p)
    VALUES(_id_usuario, _spreads);

    UPDATE cedis_has_usuarios cu join _json_ss_usuario j USING (idUsuario)
    SET cu.ssDestinos = p->>'$.ssDestinos',
        cu.ssTarifario = p->>'$.ssTarifario',
        cu.ssVehiculos = p->>'$.ssVehiculos',
        cu.ssPreferencias = p->>'$.ssPreferencias',
        cu.ssIncompatibilidades = p->>'$.ssIncompatibilidades'
    WHERE
        cu.idCedi = _id_cedi;


    -- resultado
    SELECT 'OK' AS result, 'El archivo marcado como favorito fue guardado exitosamente' AS data;


END //

-- call sp_api_arms_actualizar_ss_preferido_usuario (16, 10, '{"ssDestinos": "destino", "ssTarifario": "tarifario", "ssVehiculos": "vehiculo", "ssPreferencias": "preferencia", "ssIncompatibilidades": "incompatibilidad"}');