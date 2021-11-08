-- --------------------------------------------------------------------------------
-- EDUARDO GILES lunes, 1 de septiembre de 2019 (GMT-5)
-- Note, Para guardar las preferencias del cedi
-- --------------------------------------------------------------------------------

drop procedure if exists sp_api_arms_guardar_preferencias_cedi;

DELIMITER //

CREATE PROCEDURE `sp_api_arms_guardar_preferencias_cedi`(
       _id_cedi int,
       _perfil varchar(45),
       _preferencias json)
BEGIN

    DECLARE _tipoArcoS VARCHAR(10);
    DECLARE _tipoArco INT;
    DECLARE _tarifario INT;
    DECLARE _msg TEXT;
    DECLARE _resultado TEXT;
	DECLARE _code CHAR(5) DEFAULT '00000';

	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN

		GET DIAGNOSTICS CONDITION 1
			_code = RETURNED_SQLSTATE,_msg = MESSAGE_TEXT;

        ROLLBACK;
        SELECT 'FAIL' AS result, _msg AS data;

	END;

	SET SESSION time_zone = 'America/Mexico_City';
    SET SESSION group_concat_max_len = 4000000;

    START TRANSACTION;

        if not exists (		-- verifico exista el cedi
            select *
            from
                cedis
            where
                idCedi = _id_cedi
        ) then
            SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'El cedi no existe';
        end if;

        DROP temporary table if EXISTS _json_preferencias_cedi;
        CREATE temporary table _json_preferencias_cedi(idCedi int, p json);

        INSERT INTO _json_preferencias_cedi (idCedi, p)
        VALUES(_id_cedi, _preferencias);

        SET _tarifario = JSON_UNQUOTE(JSON_EXTRACT(_preferencias, CONCAT('$.Tarifario')));
        SET _tipoArcoS = JSON_UNQUOTE(JSON_EXTRACT(_preferencias, CONCAT('$.TipoArco')));
        
        IF _tipoArcoS = 'google' THEN
            SET _tipoArco = 0;
        ELSE
            SET _tipoArco = 1;
        END IF;

        UPDATE cedis
        SET tipoArco = _tipoArco, tarifario = _tarifario
        WHERE idCedi = _id_cedi;

        -- ACTUALIZAR TARIFARIO PARA EL CEDI
        -- ACTUALIZAR TIPO DE ARCO PARA EL CEDI
        UPDATE preferencias_destinos pd join _json_preferencias_cedi j USING (idCedi)
        SET pd.cediRetorno = p->>'$.CediRetorno',
            pd.factorIntermedioL = p->>'$.FactorIntermedioL',
            pd.factorIntermedioM = p->>'$.FactorIntermedioM',
            pd.factorIntermedioS = p->>'$.FactorIntermedioS',
            pd.factorRetornoL = p->>'$.FactorRetornoL',
            pd.factorRetornoM = p->>'$.FactorRetornoM',
            pd.factorRetornoS = p->>'$.FactorRetornoS',
            pd.factorSubidaL = p->>'$.FactorSubidaL',
            pd.factorSubidaM = p->>'$.FactorSubidaM',
            pd.factorSubidaS = p->>'$.FactorSubidaS',
            pd.hotelesRetorno = p->>'$.HotelesRetorno',
            pd.hotelesSubida = p->>'$.HotelesSubida',
            pd.lunes = p->>'$.Lunes',
            pd.martes = p->>'$.Martes',
            pd.miercoles = p->>'$.Miercoles',
            pd.jueves = p->>'$.Jueves',
            pd.viernes = p->>'$.Viernes',
            pd.sabado = p->>'$.Sabado',
            pd.domingo = p->>'$.Domingo',
            pd.restriccionVolumen = p->>'$.RestriccionVolumen',
            pd.tiempoServicio = p->>'$.TiempoServicio',
            pd.ventanaLunVieIni1 = p->>'$.VentanaLunVieIni1',
            pd.ventanaLunVieFin1 = p->>'$.VentanaLunVieFin1',
            pd.ventanaLunVieIni2 = p->>'$.VentanaLunVieIni2',
            pd.ventanaLunVieFin2 = p->>'$.VentanaLunVieFin2',
            pd.ventanaSabDomIni = p->>'$.VentanaSabDomIni',
            pd.ventanaSabDomFin = p->>'$.VentanaSabDomFin'
        WHERE perfil = _perfil;
        
        UPDATE preferencias_pedidos pp join _json_preferencias_cedi j USING (idCedi)
        SET pp.familia = p->>'$.Familia',
            pp.kg = p->>'$.Kg',
            pp.m3 = p->>'$.M3',
            pp.piezas = p->>'$.Piezas',
            pp.productos = p->>'$.Productos',
            pp.tiempoDescarga = p->>'$.TiempoDescarga',
            pp.tipoEnvio = p->>'$.TipoEnvio',
            pp.tipoPedido = p->>'$.TipoPedido',
            pp.tipoServicio = p->>'$.TipoServicio',
            pp.valorMercancia = p->>'$.ValorMercancia',
            pp.vendedor = p->>'$.Vendedor'
        WHERE perfil = _perfil;
        
        UPDATE preferencias_motor pm join _json_preferencias_cedi j USING (idCedi)
        SET pm.costoCombustible = p->>'$.CostoCombustible',
            pm.costoComida = p->>'$.CostoComida',
            pm.costoHotel = p->>'$.CostoHotel',
            pm.costoThermo = p->>'$.CostoThermo',
            pm.duracionComida = p->>'$.DuracionComida',
            pm.duracionDescanso = p->>'$.DuracionDescanso',
            pm.duracionHotel = p->>'$.DuracionHotel',
            pm.facCediRetorno = p->>'$.FacCediRetorno',
            pm.facComidas = p->>'$.FacComidas',
            pm.facCompatibilidad = p->>'$.FacCompatibilidad',
            pm.facDescansos = p->>'$.FacDescansos',
            pm.facHoteles = p->>'$.FacHoteles',
            pm.facPeso = p->>'$.FacPeso',
            pm.facRespetarDiasDestinos = p->>'$.FacRespetarDiasDestinos',
            pm.facRestriccionVolumen = p->>'$.FacRestriccionVolumen',
            pm.reglaValor = p->>'$.FacValor',
            pm.facVentanaEntrega = p->>'$.FacVentanaEntrega',
            pm.facVolumen = p->>'$.FacVolumen',
            pm.facEntregasUnDia = p->>'$.FacEntregasUnDia',
            pm.heuristica = p->>'$.Heuristica',
            pm.horaComida = p->>'$.HoraComida',
            pm.horasThermo = p->>'$.HorasThermo',
            pm.intervaloDescanso = p->>'$.IntervaloDescanso',
            pm.metaheuristica = p->>'$.Metaheuristica',
            pm.valorMaximo = p->>'$.ValorMax'
        WHERE perfil = _perfil;
        
        UPDATE preferencias_vehiculos pv join _json_preferencias_cedi j USING (idCedi)
        SET pv.cantidad = p->>'$.Cantidad',
            pv.fletera = p->>'$.Fletera',
            pv.rendimiento = p->>'$.Rendimiento',
            pv.rendimientoTermo = p->>'$.RendimientoTermo'
        WHERE perfil = _perfil;

    COMMIT;

    -- resultado
    SELECT 'OK' AS result, 'Preferencias guardadas correctamente' AS data;


END //
