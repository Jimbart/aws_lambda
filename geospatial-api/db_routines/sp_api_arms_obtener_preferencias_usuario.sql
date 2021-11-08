-- --------------------------------------------------------------------------------
-- RAQUEL martes, 26 de marzo de 2019 (GMT-5)
-- Note, Para obtener las preferencias del usuario
-- EDUARDO jueves, 16 de mayo de 2019 (GMT-5)
-- Modificacion para obtener los ids de SS de la tabla cedis_has_usuarios
-- --------------------------------------------------------------------------------

drop procedure if exists sp_api_arms_obtener_preferencias_usuario;

DELIMITER //

CREATE PROCEDURE `sp_api_arms_obtener_preferencias_usuario`(
       _id_cedis int,
       _correo VARCHAR(100))
BEGIN

    declare json_preferencias JSON;

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

    set json_preferencias = (SELECT 
                                JSON_OBJECT(
                                    'idProyectos', dat.idProyectos,
                                    'idPedidos', dat.idPedidos,
                                    'idVehiculos', dat.idVehiculos,
                                    'idTarifario', dat.idTarifario,
                                    'idIncompatibilidades', dat.idIncompatibilidades,
                                    'idDestinos', dat.idDestinos,
                                    'folder_cedi', dat.folder_cedi,
                                    'idPreferencias', dat.idPreferencias,
                                    'ssVehiculos', dat.ssVehiculos,
                                    'ssTarifario', dat.ssTarifario,
                                    'ssPreferencias', dat.ssPreferencias,
                                    'ssIncompatibilidades', dat.ssIncompatibilidades,
                                    'ssDestinos', dat.ssDestinos,
                                    'preferencias_grids', dat.preferencias_grids,
                                    'latitudCedi', dat.latitudCedi,
                                    'longitudCedi', dat.longitudCedi) AS json_plan
                            FROM
                                (SELECT 
                                    c.idProyectos,
                                    c.idPedidos,
                                    c.idVehiculos,
                                    c.idTarifario,
                                    c.idIncompatibilidades,
                                    c.idDestinos,
                                    c.idPreferencias,
                                    c.idFolder AS folder_cedi,
                                    e.idFolder AS folder_empresa,
                                    cu.ssVehiculos,
                                    cu.ssTarifario,
                                    cu.ssPreferencias,
                                    cu.ssIncompatibilidades,
                                    cu.ssDestinos,
                                    cu.preferencias_grids,
                                    c.latitud AS latitudCedi,
                                    c.longitud AS longitudCedi
                                FROM
                                    cedis c 
                                    JOIN empresas e USING(idEmpresa)
                                    JOIN cedis_has_usuarios cu USING(idCedi)
                                    JOIN usuarios u USING (idUsuario)
                                where c.idCedi = _id_cedis
                                and u.correo = _correo) dat
                            );

    UPDATE usuarios set idCediPreferido = _id_cedis where correo = _correo;

    -- resultado
    SELECT 'OK' AS result, json_preferencias AS data;


END //

call sp_api_arms_obtener_preferencias_usuario(16, 'rantonio@arete.ws');