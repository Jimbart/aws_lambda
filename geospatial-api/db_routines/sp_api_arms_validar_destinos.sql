-- --------------------------------------------------------------------------------
-- Norman Sánchez, 12 de Marzo de 2019 (GMT-6)
-- Note: para validar destinos de una lista dada
-- --------------------------------------------------------------------------------

drop procedure if exists sp_api_arms_validar_destinos;

DELIMITER // 

CREATE PROCEDURE `sp_api_arms_validar_destinos`(
	_json_destinos json, 
	_idCedi int)
BEGIN	

	declare _i int;
	DECLARE _destinos text;
    
    DECLARE _msg TEXT;
    DECLARE _resultado TEXT;
	DECLARE _code CHAR(5) DEFAULT '00000';

	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN

		GET DIAGNOSTICS CONDITION 1
			_code = RETURNED_SQLSTATE,_msg = MESSAGE_TEXT;

        SELECT 'FAIL' AS result, _msg AS data;

	END;    

	SET SESSION group_concat_max_len = 18446744073709551615;
    
    SET SESSION time_zone = 'America/Mexico_City';
    
    drop temporary table if exists _destinos_delete;
    create temporary table _destinos_delete (Destino varchar(140) primary key);

    drop temporary table if exists _list_destinos;
    create temporary table _list_destinos (Destino varchar(140) primary key);
    

    if json_length(ifnull(_json_destinos, '{}')) > 0 then

		set _i = 0;
		while _i < json_length(_json_destinos) do
			insert ignore into _destinos_delete (Destino) values (JSON_UNQUOTE(json_extract(_json_destinos, concat('$[',_i,']'))));
			set _i = _i + 1;		-- siguiente

		end while;
        
	end if;

	INSERT INTO _list_destinos SELECT Destino FROM _destinos_delete;

	SET SQL_SAFE_UPDATES = 0;
	DELETE _dest FROM _destinos_delete _dest
        JOIN
    destinos dest USING (Destino)
		WHERE
    idCedi = _idCedi;
    
    

  set _destinos = (select group_concat(Destino) from _destinos_delete);


	SET _json_destinos = (select cast(concat('['
					,ifnull(group_concat(json_plan), '')
				, ']') as json) as data
		from (
			SELECT 
				JSON_OBJECT('idDestino',
						dat.idDestino,
						'idCedi',
						dat.idCedi,
						'destino',
						dat.destino,
						'latitud',
						dat.latitud,
						'longitud',
						dat.longitud,
						'restriccionVolumen',
						dat.restriccionVolumen,
						'dedicado',
						dat.dedicado,
						'region',
						dat.region,
						'subregion',
						dat.subregion,
						'estado',
						dat.estado,
						'municipio',
						dat.municipio,
						'colonia',
						dat.colonia,
						'calleNumero',
						dat.calleNumero,
						'codigoPostal',
						dat.codigoPostal,
						'ventanaLunVieIni1',
						dat.ventanaLunVieIni1,
						'ventanaLunVieFin1',
						dat.ventanaLunVieFin1,
						'ventanaLunVieIni2',
						dat.ventanaLunVieIni2,
						'ventanaLunVieFin2',
						dat.ventanaLunVieFin2,
						'desfaseHorario',
						0,
						'L',
						dat.L,
						'M',
						dat.M,
						'Mi',
						dat.Mi,
						'J',
						dat.J,
						'V',
						dat.V,
						'S',
						dat.S,
						'D',
						dat.D,
						'tiempoServicio',
						dat.tiempoServicio,
						'contacto1',
						dat.contacto1,
						'telefono1',
						dat.telefono1,
						'correo1',
						dat.correo1,
						'contacto2',
						dat.contacto2,
						'telefono2',
						dat.telefono2,
						'correo2',
						dat.correo2,
						'hotel',
						dat.hotel,
						'factorSubidaS',
						dat.factorSubidaS,
						'factorSubidaM',
						dat.factorSubidaM,
						'factorSubidaL',
						dat.factorSubidaL,
						'factorIntermedioS',
						dat.factorIntermedioS,
						'factorIntermedioM',
						dat.factorIntermedioM,
						'factorIntermedioL',
						dat.factorIntermedioL,
						'factorRetornoS',
						dat.factorRetornoS,
						'factorRetornoM',
						dat.factorRetornoM,
						'factorRetornoL',
						dat.factorRetornoL,
						'recalcularDestino',
						dat.recalcularDestino,
						'validado',
						dat.validado,
						'zona',
						dat.Zona,
						'local',
						dat.local,
						'alias',
						dat.alias,
						'hotelSubida',
						dat.hotelSubida,
						'hotelRetorno',
						dat.hotelRetorno,
						'ventanaSabDomIni',
						dat.ventanaSabDomIni,
						'ventanaSabDomFin',
						dat.ventanaSabDomFin) AS json_plan
			FROM
				(SELECT 
					d.idDestino,
						d.idCedi,
						d.destino,
						d.latitud,
						d.longitud,
						d.zona,
						ROUND(d.restriccionVolumen, 2) as restriccionVolumen,
						d.dedicado,
						d.region,
						d.subregion,
						d.estado,
						d.municipio,
						d.colonia,
						d.calleNumero,
						d.codigoPostal,
						DATE_FORMAT(d.ventanaLunVieIni1, '%H:%i:%s') as ventanaLunVieIni1,
						DATE_FORMAT(d.ventanaLunVieFin1, '%H:%i:%s') as ventanaLunVieFin1,
						DATE_FORMAT(d.ventanaLunVieIni2, '%H:%i:%s') as ventanaLunVieIni2,
						DATE_FORMAT(d.ventanaLunVieFin2, '%H:%i:%s') as ventanaLunVieFin2,
						d.defaseHorario,
						d.L,
						d.M,
						d.Mi,
						d.J,
						d.V,
						d.S,
						d.D,
						d.tiempoServicio,
						d.contacto1,
						d.telefono1,
						d.correo1,
						d.contacto2,
						d.telefono2,
						d.correo2,
						d.hotel,
						d.factorSubidaS,
						d.factorSubidaM,
						d.factorSubidaL,
						d.factorIntermedioS,
						d.factorIntermedioM,
						d.factorIntermedioL,
						d.factorRetornoS,
						d.factorRetornoM,
						d.factorRetornoL,
						d.recalcularDestino,
						d.validado,
						d.local,
						d.alias,
						d.hotelSubida,
						d.hotelRetorno,
						DATE_FORMAT(d.ventanaSabDomIni, '%H:%i:%s') as ventanaSabDomIni,
						DATE_FORMAT(d.ventanaSabDomFin, '%H:%i:%s') as ventanaSabDomFin
				FROM
					_list_destinos _d
				JOIN destinos d USING (Destino)
				WHERE
					idCedi = _idCedi) dat
		) dat);


SELECT 'OK' as result, _json_destinos as json_destinos, ifnull(_destinos, '') as destinos_faltantes;
	
        
END //



call sp_api_arms_validar_destinos('["10152-ROBERTET DE MEXICO, S.A. DE C.V.", "11373-PRODUCTOS QUIMICOS GAMMA, S.A. DE C", "123"]', 13);