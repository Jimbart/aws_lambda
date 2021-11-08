-- --------------------------------------------------------------------------------
-- RAQUEL martes, 26 de marzo de 2019 (GMT-5)
-- Note, Para obtener las preferencias del usuario
-- --------------------------------------------------------------------------------

drop procedure if exists sp_api_arms_guardar_preferencias_usuario;

DELIMITER //

CREATE PROCEDURE `sp_api_arms_guardar_preferencias_usuario`(
       _id_usuario int,
       _preferencias json)
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
		select correo
        from
			usuarios
		where
			idUsuario = _id_usuario
	) then
		SIGNAL SQLSTATE '45000'
			SET MESSAGE_TEXT = 'El correo no está registrado';
	end if;

    DROP temporary table if EXISTS _json_preferencias_usuario;
    CREATE temporary table _json_preferencias_usuario(idUsuario int, p json);

    INSERT INTO  (idUsuario, preferencias)
    VALUES(_id_usuario, _preferencias);

    UPDATE usuarios u join _json_preferencias_usuario j USING (idUsuario)
    SET u.ssDestinos = p->>'$.ssDestinos',
        u.ssTarifario = p->>'$.ssTarifario',
        u.ssVehiculos = p->>'$.ssVehiculos',
        u.ssPreferencias = p->>'$.ssPreferencias',
        u.ssIncompatibilidades = p->>'$.ssIncompatibilidades',
        u.idCediPreferido = p->>'$.idCediPreferido',
        u.preferencias_grids = p->>'$.preferencias_grids';


    -- resultado
    SELECT 'OK' AS result, 'Preferencias guardadas correctamente' AS data;


END //
