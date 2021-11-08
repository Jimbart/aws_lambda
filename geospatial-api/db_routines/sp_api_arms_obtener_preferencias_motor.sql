-- --------------------------------------------------------------------------------
-- RAQUEL martes, 26 de marzo de 2019 (GMT-5)
-- Note, Para obtener las preferencias del motor de un usuario por perfil y cedi
-- --------------------------------------------------------------------------------

drop procedure if exists sp_api_arms_obtener_preferencias_motor;

DELIMITER //

CREATE PROCEDURE `sp_api_arms_obtener_preferencias_motor`(
       _id_cedis int,
       _perfil VARCHAR(15))
BEGIN

    declare json_preferencias JSON;
    declare _list_perfiles json;

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

    set json_preferencias = (SELECT 
                                JSON_OBJECT(
                                    'idPreferenciasMotor', dat.idPreferenciasMotor, 
                                    'idCedi', dat.idCedi, 
                                    'Heuristica', dat.heuristica, 
                                    'Metaheuristica', dat.metaheuristica, 
                                    'ValorMax', dat.valorMaximo, 
                                    'FacRespetarDiasDestinos', dat.facRespetarDiasDestinos, 
                                    'FacEntregasUnDia', dat.facEntregasUnDia, 
                                    'FacVentanaEntrega', dat.facVentanaEntrega, 
                                    'FacVolumen', dat.facVolumen, 
                                    'FacPeso', dat.facPeso, 
                                    'FacValor', dat.reglaValor, 
                                    'FacRestriccionVolumen', dat.facRestriccionVolumen, 
                                    'FacCompatibilidad', dat.facCompatibilidad, 
                                    'FacDescansos', dat.facDescansos, 
                                    'FacComidas', dat.facComidas, 
                                    'FacHoteles', dat.facHoteles, 
                                    'DuracionDescanso', dat.duracionDescanso, 
                                    'DuracionComida', dat.duracionComida, 
                                    'DuracionHotel', dat.duracionHotel, 
                                    'IntervaloDescanso', dat.intervaloDescanso, 
                                    'HoraComida', dat.horaComida, 
                                    'CostoCombustible', dat.costoCombustible, 
                                    'CostoThermo', dat.costoThermo, 
                                    'CostoHotel', dat.costoHotel, 
                                    'CostoComida', dat.costoComida, 
                                    'perfil', dat.perfil) AS json_plan
                            FROM
                                (SELECT 
                                    p.idPreferenciasMotor,
                                    p.idCedi,
                                    p.heuristica,
                                    p.metaheuristica,
                                    p.valorMaximo,
                                    p.facRespetarDiasDestinos,
                                    p.facEntregasUnDia,
                                    p.facVentanaEntrega,
                                    p.facVolumen,
                                    p.facPeso,
                                    p.reglaValor,
                                    p.facRestriccionVolumen,
                                    p.facCompatibilidad,
                                    p.facDescansos,
                                    p.facComidas,
                                    p.facHoteles,
                                    p.duracionDescanso,
                                    p.duracionComida,
                                    p.duracionHotel,
                                    p.intervaloDescanso,
                                    DATE_FORMAT(p.horaComida, '%H:%i:%s') as horaComida,
                                    costoCombustible,
                                    costoThermo,
                                    costoHotel,
                                    costoComida,
                                    p.perfil
                                FROM
                                    preferencias_motor p 
                                where p.idCedi = _id_cedis and perfil = _perfil) dat
                            );

    

    SET _list_perfiles = (SELECT cast(CONCAT('[', GROUP_CONCAT('"', perfil, '"'), ']') AS json)
                        
                    FROM
                        (
                        SELECT 
                            perfil
                        FROM
                            preferencias_motor
                        WHERE
                            idCedi = _id_cedis) AS json_perfiles);


    if json_preferencias is null
    then
		SIGNAL SQLSTATE '45000'
			SET MESSAGE_TEXT = 'Error en obtener las preferencias, comunicate con el equipo ARMS';
	end if;

    -- resultado
    SELECT 'OK' AS result, ifnull(json_preferencias, '{}') AS data, ifnull(_list_perfiles, '[]') as perfiles;


END//

