-- --------------------------------------------------------------------------------
-- EDUARDO lunes, 27 de mayo de 2019
-- Note, Para obtener las preferencias default, incluyendo las del motor
-- --------------------------------------------------------------------------------

drop procedure if exists sp_api_arms_obtener_preferencias_default;

DELIMITER //

CREATE PROCEDURE `sp_api_arms_obtener_preferencias_default`(
       _id_cedis int,
       _perfil VARCHAR(15))
BEGIN

    declare json_preferencias_motor JSON;
    declare json_preferencias_pedidos JSON;
    declare json_preferencias_destinos JSON;
    declare json_preferencias_vehiculos JSON;
    declare _list_perfiles json;
    declare _tipoArco varchar(10);
    declare _tarifario INT;

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

    set _tipoArco = (SELECT 
    (
        CASE 
            WHEN tipoArco = 0 THEN 'google'
            WHEN tipoArco = 1 THEN 'osrm'
        END
    ) AS tipo 
    from cedis where idCedi = _id_cedis);

    set _tarifario = (SELECT tarifario from cedis where idCedi = _id_cedis);

    set json_preferencias_motor = (SELECT 
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
                                    'FacCediRetorno', dat.facCediRetorno, 
                                    'DuracionDescanso', dat.duracionDescanso, 
                                    'DuracionComida', dat.duracionComida, 
                                    'DuracionHotel', dat.duracionHotel, 
                                    'IntervaloDescanso', dat.intervaloDescanso, 
                                    'HoraComida', dat.horaComida, 
                                    'CostoCombustible', dat.costoCombustible, 
                                    'CostoThermo', dat.costoThermo, 
                                    'CostoHotel', dat.costoHotel, 
                                    'CostoComida', dat.costoComida, 
                                    'perfil', dat.perfil,
                                    'TipoArco', _tipoArco,
                                    'Tarifario', _tarifario,
                                    'HorasThermo', dat.horasThermo
                                    ) AS json_plan
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
                                    p.facCediRetorno,
                                    p.duracionDescanso,
                                    p.duracionComida,
                                    p.duracionHotel,
                                    p.intervaloDescanso,
                                    DATE_FORMAT(p.horaComida, '%H:%i') as horaComida,
                                    costoCombustible,
                                    costoThermo,
                                    costoHotel,
                                    costoComida,
                                    p.perfil,
                                    horasThermo
                                FROM
                                    preferencias_motor p 
                                where p.idCedi = _id_cedis and perfil = _perfil) dat
                            );

    SET json_preferencias_pedidos = (SELECT 
                                        JSON_OBJECT(
                                                'M3',
                                                dat.m3,
                                                'Kg',
                                                dat.kg,
                                                'ValorMercancia',
                                                dat.valorMercancia,
                                                'TiempoDescarga',
                                                dat.tiempoDescarga,
                                                'Productos',
                                                dat.productos,
                                                'TipoServicio',
                                                dat.tipoServicio,
                                                'Vendedor',
                                                dat.vendedor,
                                                'TipoPedido',
                                                dat.tipoPedido,
                                                'TipoEnvio',
                                                dat.tipoEnvio,
                                                'Piezas',
                                                dat.piezas,
                                                'Familia',
                                                dat.familia) AS json_plan
                                    FROM
                                        (SELECT 
                                                p.m3,
                                                p.kg,
                                                p.valorMercancia,
                                                p.tiempoDescarga,
                                                p.productos,
                                                p.tipoServicio,
                                                p.vendedor,
                                                p.tipoPedido,
                                                p.tipoEnvio,
                                                p.piezas,
                                                p.familia
                                        FROM
                                            preferencias_pedidos p
                                       where p.idCedi = _id_cedis and perfil = _perfil) dat);

    SET json_preferencias_destinos = (SELECT 
                                        JSON_OBJECT(
                                                'Lunes',
                                                dat.lunes,
                                                'Martes',
                                                dat.martes,
                                                'Miercoles',
                                                dat.miercoles,
                                                'Jueves',
                                                dat.jueves,
                                                'Viernes',
                                                dat.viernes,
                                                'Sabado',
                                                dat.sabado,
                                                'Domingo',
                                                dat.domingo,
                                                'VentanaLunVieIni1',
                                                dat.ventanaLunVieIni1,
                                                'VentanaLunVieFin1',
                                                dat.ventanaLunVieFin1,
                                                'VentanaLunVieIni2',
                                                dat.ventanaLunVieIni2,
                                                'VentanaLunVieFin2',
                                                dat.ventanaLunVieFin2,
                                                'VentanaSabDomIni',
                                                dat.ventanaSabDomIni,
                                                'VentanaSabDomFin',
                                                dat.ventanaSabDomFin,
                                                'HotelesSubida',
                                                dat.hotelesSubida,
                                                'HotelesRetorno',
                                                dat.hotelesRetorno,
                                                'FactorSubidaS',
                                                dat.factorSubidaS,
                                                'FactorSubidaM',
                                                dat.factorSubidaM,
                                                'FactorSubidaL',
                                                dat.factorSubidaL,
                                                'FactorIntermedioS',
                                                dat.factorIntermedioS,
                                                'FactorIntermedioM',
                                                dat.factorIntermedioM,
                                                'FactorIntermedioL',
                                                dat.factorIntermedioL,
                                                'FactorRetornoS',
                                                dat.factorRetornoS,
                                                'FactorRetornoM',
                                                dat.factorRetornoM,
                                                'FactorRetornoL',
                                                dat.factorRetornoL,
                                                'TiempoServicio',
                                                dat.tiempoServicio,
                                                'CediRetorno',
                                                dat.cediRetorno,
                                                'RestriccionVolumen',
                                                dat.restriccionVolumen) AS json_plan
                                    FROM
                                        (SELECT 
                                                p.lunes,
                                                p.martes,
                                                p.miercoles,
                                                p.jueves,
                                                p.viernes,
                                                p.sabado,
                                                p.domingo,
                                                DATE_FORMAT(p.ventanaLunVieIni1, '%H:%i') as ventanaLunVieIni1,
                                                DATE_FORMAT(p.ventanaLunVieFin1, '%H:%i') as ventanaLunVieFin1,
                                                DATE_FORMAT(p.ventanaLunVieIni2, '%H:%i') as ventanaLunVieIni2,
                                                DATE_FORMAT(p.ventanaLunVieFin2, '%H:%i') as ventanaLunVieFin2,
                                                DATE_FORMAT(p.ventanaSabDomIni, '%H:%i') as ventanaSabDomIni,
                                                DATE_FORMAT(p.ventanaSabDomFin, '%H:%i') as ventanaSabDomFin,
                                                p.hotelesSubida,
                                                p.hotelesRetorno,
                                                p.factorSubidaS,
                                                p.factorSubidaM,
                                                p.factorSubidaL,
                                                p.factorIntermedioS,
                                                p.factorIntermedioM,
                                                p.factorIntermedioL,
                                                p.factorRetornoS,
                                                p.factorRetornoM,
                                                p.factorRetornoL,
                                                p.tiempoServicio,
                                                p.cediRetorno,
                                                p.restriccionVolumen
                                        FROM
                                            preferencias_destinos p
                                       where p.idCedi = _id_cedis and perfil = _perfil) dat);

    SET json_preferencias_vehiculos = (SELECT 
                                        JSON_OBJECT(
                                                'Cantidad',
                                                dat.cantidad,
                                                'VolumenMixto',
                                                dat.volumenMixto,
                                                'Fletera',
                                                dat.fletera,
                                                'Rendimiento',
                                                dat.rendimiento,
                                                'RendimientoTermo',
                                                dat.rendimientoTermo) AS json_plan
                                    FROM
                                        (SELECT 
                                                p.cantidad,
                                                p.volumenMixto,
                                                p.fletera,
                                                p.rendimiento,
                                                p.rendimientoTermo
                                        FROM
                                            preferencias_vehiculos p
                                       where p.idCedi = _id_cedis and perfil = _perfil) dat);

    SET _list_perfiles = (SELECT cast(CONCAT('[', GROUP_CONCAT('"', perfil, '"'), ']') AS json)
                        
                    FROM
                        (
                        SELECT 
                            perfil
                        FROM
                            preferencias_motor
                        WHERE
                            idCedi = _id_cedis) AS json_perfiles);


    if json_preferencias_motor is null
    then
		SIGNAL SQLSTATE '45000'
			SET MESSAGE_TEXT = 'Error en obtener las preferencias, comunicate con el equipo ARMS';
    elseif json_preferencias_motor is null
    then
		SIGNAL SQLSTATE '45000'
			SET MESSAGE_TEXT = 'Error en obtener las preferencias, comunicate con el equipo ARMS';
	end if;

    -- resultado
    SELECT 'OK' AS result, 
    ifnull(json_preferencias_motor, '{}') AS motor,
    ifnull(json_preferencias_pedidos, '{}') AS pedidos,
    ifnull(json_preferencias_destinos, '{}') AS destinos,
    ifnull(json_preferencias_vehiculos, '{}') AS vehiculos,
    ifnull(_list_perfiles, '[]') as perfiles;


END//

-- call sp_api_arms_obtener_preferencias_default(16, 'default');