-- --------------------------------------------------------------------------------
-- RAQUEL lunes, 08 de abril de 2017 (GMT-5)
-- Note: para agregar vehiculos a un cedis
-- UPD 
-- --------------------------------------------------------------------------------

DROP PROCEDURE if exists sp_api_arms_registrar_destinos;

delimiter //

CREATE PROCEDURE `sp_api_arms_registrar_destinos`(
	_idCedi INT,
	_destinos json)
BEGIN
	DECLARE _iDestinos INT;
	DECLARE _iDestino INT;
    -- CAMPOS DEL DESTINO
    DECLARE _destino varchar(140);
    DECLARE _latitud decimal(10,6);
    DECLARE _longitud decimal(10,6);
    DECLARE _zona varchar(50);
    DECLARE _restriccionVolumen float;
    DECLARE _dedicado tinyint(4);
    DECLARE _region varchar(140);
    DECLARE _subregion varchar(140);
    DECLARE _estado varchar(140);
    DECLARE _municipio varchar(140);
    DECLARE _colonia varchar(140);
    DECLARE _calleNumero varchar(140);
    DECLARE _codigoPostal varchar(5);
    DECLARE _ventanaLunVieIni1 time;
    DECLARE _ventanaLunVieFin1 time;
    DECLARE _ventanaLunVieIni2 time;
    DECLARE _ventanaLunVieFin2 time;
    DECLARE _defaseHorario int(11);
    DECLARE _L tinyint(4);
    DECLARE _M tinyint(4);
    DECLARE _Mi tinyint(4);
    DECLARE _J tinyint(4);
    DECLARE _V tinyint(4);
    DECLARE _S tinyint(4);
    DECLARE _D tinyint(4);
    DECLARE _tiempoServicio int(11);
    DECLARE _contacto1 varchar(140);
    DECLARE _telefono1 varchar(10);
    DECLARE _correo1 varchar(140);
    DECLARE _contacto2 varchar(140);
    DECLARE _telefono2 varchar(10);
    DECLARE _correo2 varchar(140);
    DECLARE _hotel varchar(140);
    DECLARE _factorSubidaS double;
    DECLARE _factorSubidaM double;
    DECLARE _factorSubidaL double;
    DECLARE _factorIntermedioS double;
    DECLARE _factorIntermedioM double;
    DECLARE _factorIntermedioL double;
    DECLARE _factorRetornoS double;
    DECLARE _factorRetornoM double;
    DECLARE _factorRetornoL double;
    DECLARE _recalcularDestino tinyint(4);
    DECLARE _validado tinyint(4);
    DECLARE _local tinyint(4);
    DECLARE _alias varchar(200);
    DECLARE _hotelSubida int(11);
    DECLARE _hotelRetorno int(11);
    DECLARE _ventanaSabDomIni time;
    DECLARE _ventanaSabDomFin time;

	
    DECLARE _message_text TEXT;
	DECLARE _msg TEXT;
	DECLARE _code CHAR(5) DEFAULT '00000';

	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN

		GET DIAGNOSTICS CONDITION 1
			_code = RETURNED_SQLSTATE,_msg = MESSAGE_TEXT;

		ROLLBACK;
        SELECT 'FAIL' AS result, _msg AS data;

	END;

	SET SESSION time_zone = 'America/Mexico_City';

    START TRANSACTION;
  
	SET _iDestinos = JSON_LENGTH(_destinos);
	SET _iDestino = 0;


	WHILE _iDestino < _iDestinos DO
		SET _destino = JSON_UNQUOTE(JSON_EXTRACT(_destinos, CONCAT('$[',_iDestino,'].destino')));
        SET _latitud = JSON_UNQUOTE(JSON_EXTRACT(_destinos, CONCAT('$[',_iDestino,'].latitud')));
        SET _longitud = JSON_UNQUOTE(JSON_EXTRACT(_destinos, CONCAT('$[',_iDestino,'].longitud')));
        SET _zona = JSON_UNQUOTE(JSON_EXTRACT(_destinos, CONCAT('$[',_iDestino,'].zona')));
        SET _restriccionVolumen = JSON_UNQUOTE(JSON_EXTRACT(_destinos, CONCAT('$[',_iDestino,'].restriccionVolumen')));
        SET _dedicado = JSON_UNQUOTE(JSON_EXTRACT(_destinos, CONCAT('$[',_iDestino,'].dedicado')));
        SET _region = JSON_UNQUOTE(JSON_EXTRACT(_destinos, CONCAT('$[',_iDestino,'].region')));
        SET _subregion = JSON_UNQUOTE(JSON_EXTRACT(_destinos, CONCAT('$[',_iDestino,'].subregion')));
        SET _estado = JSON_UNQUOTE(JSON_EXTRACT(_destinos, CONCAT('$[',_iDestino,'].estado')));
        SET _municipio = JSON_UNQUOTE(JSON_EXTRACT(_destinos, CONCAT('$[',_iDestino,'].municipio')));
        SET _colonia = JSON_UNQUOTE(JSON_EXTRACT(_destinos, CONCAT('$[',_iDestino,'].colonia')));
        SET _calleNumero = JSON_UNQUOTE(JSON_EXTRACT(_destinos, CONCAT('$[',_iDestino,'].calleNumero')));
        SET _codigoPostal = JSON_UNQUOTE(JSON_EXTRACT(_destinos, CONCAT('$[',_iDestino,'].codigoPostal')));
        SET _ventanaLunVieIni1 = JSON_UNQUOTE(JSON_EXTRACT(_destinos, CONCAT('$[',_iDestino,'].ventanaLunVieIni1')));
        SET _ventanaLunVieFin1 = JSON_UNQUOTE(JSON_EXTRACT(_destinos, CONCAT('$[',_iDestino,'].ventanaLunVieFin1')));
        SET _ventanaLunVieIni2 = JSON_UNQUOTE(JSON_EXTRACT(_destinos, CONCAT('$[',_iDestino,'].ventanaLunVieIni2')));
        SET _ventanaLunVieFin2 = JSON_UNQUOTE(JSON_EXTRACT(_destinos, CONCAT('$[',_iDestino,'].ventanaLunVieFin2')));
        SET _defaseHorario = JSON_UNQUOTE(JSON_EXTRACT(_destinos, CONCAT('$[',_iDestino,'].defaseHorario')));
        SET _L = JSON_UNQUOTE(JSON_EXTRACT(_destinos, CONCAT('$[',_iDestino,'].L')));
        SET _M = JSON_UNQUOTE(JSON_EXTRACT(_destinos, CONCAT('$[',_iDestino,'].M')));
        SET _Mi = JSON_UNQUOTE(JSON_EXTRACT(_destinos, CONCAT('$[',_iDestino,'].Mi')));
        SET _J = JSON_UNQUOTE(JSON_EXTRACT(_destinos, CONCAT('$[',_iDestino,'].J')));
        SET _V = JSON_UNQUOTE(JSON_EXTRACT(_destinos, CONCAT('$[',_iDestino,'].V')));
        SET _S = JSON_UNQUOTE(JSON_EXTRACT(_destinos, CONCAT('$[',_iDestino,'].S')));
        SET _D = JSON_UNQUOTE(JSON_EXTRACT(_destinos, CONCAT('$[',_iDestino,'].D')));
        SET _tiempoServicio = JSON_UNQUOTE(JSON_EXTRACT(_destinos, CONCAT('$[',_iDestino,'].tiempoServicio')));
        SET _contacto1 = JSON_UNQUOTE(JSON_EXTRACT(_destinos, CONCAT('$[',_iDestino,'].contacto1')));
        SET _telefono1 = JSON_UNQUOTE(JSON_EXTRACT(_destinos, CONCAT('$[',_iDestino,'].telefono1')));
        SET _correo1 = JSON_UNQUOTE(JSON_EXTRACT(_destinos, CONCAT('$[',_iDestino,'].correo1')));
        SET _contacto2 = JSON_UNQUOTE(JSON_EXTRACT(_destinos, CONCAT('$[',_iDestino,'].contacto2')));
        SET _telefono2 = JSON_UNQUOTE(JSON_EXTRACT(_destinos, CONCAT('$[',_iDestino,'].telefono2')));
        SET _correo2 = JSON_UNQUOTE(JSON_EXTRACT(_destinos, CONCAT('$[',_iDestino,'].correo2')));
        SET _hotel = JSON_UNQUOTE(JSON_EXTRACT(_destinos, CONCAT('$[',_iDestino,'].hotel')));
        SET _factorSubidaS = JSON_UNQUOTE(JSON_EXTRACT(_destinos, CONCAT('$[',_iDestino,'].factorSubidaS')));
        SET _factorSubidaM = JSON_UNQUOTE(JSON_EXTRACT(_destinos, CONCAT('$[',_iDestino,'].factorSubidaM')));
        SET _factorSubidaL = JSON_UNQUOTE(JSON_EXTRACT(_destinos, CONCAT('$[',_iDestino,'].factorSubidaL')));
        SET _factorIntermedioS = JSON_UNQUOTE(JSON_EXTRACT(_destinos, CONCAT('$[',_iDestino,'].factorIntermedioS')));
        SET _factorIntermedioM = JSON_UNQUOTE(JSON_EXTRACT(_destinos, CONCAT('$[',_iDestino,'].factorIntermedioM')));
        SET _factorIntermedioL = JSON_UNQUOTE(JSON_EXTRACT(_destinos, CONCAT('$[',_iDestino,'].factorIntermedioL')));
        SET _factorRetornoS = JSON_UNQUOTE(JSON_EXTRACT(_destinos, CONCAT('$[',_iDestino,'].factorRetornoS')));
        SET _factorRetornoM = JSON_UNQUOTE(JSON_EXTRACT(_destinos, CONCAT('$[',_iDestino,'].factorRetornoM')));
        SET _factorRetornoL = JSON_UNQUOTE(JSON_EXTRACT(_destinos, CONCAT('$[',_iDestino,'].factorRetornoL')));
        SET _recalcularDestino = JSON_UNQUOTE(JSON_EXTRACT(_destinos, CONCAT('$[',_iDestino,'].recalcularDestino')));
        SET _validado = JSON_UNQUOTE(JSON_EXTRACT(_destinos, CONCAT('$[',_iDestino,'].validado')));
        SET _local = JSON_UNQUOTE(JSON_EXTRACT(_destinos, CONCAT('$[',_iDestino,'].local')));
        SET _alias = JSON_UNQUOTE(JSON_EXTRACT(_destinos, CONCAT('$[',_iDestino,'].alias')));
        SET _hotelSubida = JSON_UNQUOTE(JSON_EXTRACT(_destinos, CONCAT('$[',_iDestino,'].hotelSubida')));
        SET _hotelRetorno = JSON_UNQUOTE(JSON_EXTRACT(_destinos, CONCAT('$[',_iDestino,'].hotelRetorno')));
        SET _ventanaSabDomIni = JSON_UNQUOTE(JSON_EXTRACT(_destinos, CONCAT('$[',_iDestino,'].ventanaSabDomIni')));
        SET _ventanaSabDomFin = JSON_UNQUOTE(JSON_EXTRACT(_destinos, CONCAT('$[',_iDestino,'].ventanaSabDomFin')));



		INSERT INTO destinos(idCedi,
                                destino,
                                latitud,
                                longitud,
                                zona,
                                restriccionVolumen,
                                dedicado,
                                region,
                                subregion,
                                estado,
                                municipio,
                                colonia,
                                calleNumero,
                                codigoPostal,
                                ventanaLunVieIni1,
                                ventanaLunVieFin1,
                                ventanaLunVieIni2,
                                ventanaLunVieFin2,
                                defaseHorario,
                                L,
                                M,
                                Mi,
                                J,
                                V,
                                S,
                                D,
                                tiempoServicio,
                                contacto1,
                                telefono1,
                                correo1,
                                contacto2,
                                telefono2,
                                correo2,
                                hotel,
                                factorSubidaS,
                                factorSubidaM,
                                factorSubidaL,
                                factorIntermedioS,
                                factorIntermedioM,
                                factorIntermedioL,
                                factorRetornoS,
                                factorRetornoM,
                                factorRetornoL,
                                recalcularDestino,
                                validado,
                                local,
                                alias,
                                hotelSubida,
                                hotelRetorno,
                                ventanaSabDomIni,
                                ventanaSabDomFin)
		VALUES(
				_idCedi,
                _destino,
                _latitud,
                _longitud,
                _zona,
                _restriccionVolumen,
                _dedicado,
                _region,
                _subregion,
                _estado,
                _municipio,
                _colonia,
                _calleNumero,
                _codigoPostal,
                _ventanaLunVieIni1,
                _ventanaLunVieFin1,
                _ventanaLunVieIni2,
                _ventanaLunVieFin2,
                _defaseHorario,
                _L,
                _M,
                _Mi,
                _J,
                _V,
                _S,
                _D,
                _tiempoServicio,
                _contacto1,
                _telefono1,
                _correo1,
                _contacto2,
                _telefono2,
                _correo2,
                _hotel,
                _factorSubidaS,
                _factorSubidaM,
                _factorSubidaL,
                _factorIntermedioS,
                _factorIntermedioM,
                _factorIntermedioL,
                _factorRetornoS,
                _factorRetornoM,
                _factorRetornoL,
                _recalcularDestino,
                _validado,
                _local,
                _alias,
                _hotelSubida,
                _hotelRetorno,
                _ventanaSabDomIni,
                _ventanaSabDomFin)
        ON DUPLICATE KEY UPDATE
            latitud = _latitud,
            longitud = _longitud,
            zona = _zona,
            restriccionVolumen = _restriccionVolumen,
            dedicado = _dedicado,
            region = _region,
            subregion = _subregion,
            estado = _estado,
            municipio = _municipio,
            colonia = _colonia,
            calleNumero = _calleNumero,
            codigoPostal = _codigoPostal,
            ventanaLunVieIni1 = _ventanaLunVieIni1,
            ventanaLunVieFin1 = _ventanaLunVieFin1,
            ventanaLunVieIni2 = _ventanaLunVieIni2,
            ventanaLunVieFin2 = _ventanaLunVieFin2,
            defaseHorario = _defaseHorario,
            L = _L,
            M = _M,
            Mi = _Mi,
            J = _J,
            V = _V,
            S = _S,
            D = _D,
            tiempoServicio = _tiempoServicio,
            contacto1 = _contacto1,
            telefono1 = _telefono1,
            correo1 = _correo1,
            contacto2 = _contacto2,
            telefono2 = _telefono2,
            correo2 = _correo2,
            hotel = _hotel,
            factorSubidaS = _factorSubidaS,
            factorSubidaM = _factorSubidaM,
            factorSubidaL = _factorSubidaL,
            factorIntermedioS = _factorIntermedioS,
            factorIntermedioM = _factorIntermedioM,
            factorIntermedioL = _factorIntermedioL,
            factorRetornoS = _factorRetornoS,
            factorRetornoM = _factorRetornoM,
            factorRetornoL = _factorRetornoL,
            recalcularDestino = _recalcularDestino,
            validado = _validado,
            local = _local,
            alias = _alias,
            hotelSubida = _hotelSubida,
            hotelRetorno = _hotelRetorno,
            ventanaSabDomIni = _ventanaSabDomIni,
            ventanaSabDomFin = _ventanaSabDomFin;

		

	    SET _iDestino = _iDestino + 1;
	END WHILE;


	COMMIT;

    SELECT 'OK' AS result, 'Registro correcto' AS Data;
END //
