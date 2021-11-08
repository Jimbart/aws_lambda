-- --------------------------------------------------------------------------------
-- RAQUEL lunes, 08 de abril de 2017 (GMT-5)
-- Note: para agregar vehiculos a un cedis
-- UPD 
-- --------------------------------------------------------------------------------

DROP PROCEDURE if exists sp_api_arms_registrar_vehiculos;

delimiter //

CREATE PROCEDURE `sp_api_arms_registrar_vehiculos`(
	_idCedi INT,
	_vehiculos json)
BEGIN
	DECLARE _iVehiculos INT;
	DECLARE _iVehiculo INT;
    DECLARE _Cantidad int(11);
    DECLARE _TipoVehiculo varchar(45);
    DECLARE _Rendimiento float;
    DECLARE _Talla varchar(1);
    DECLARE _HuellaCarbono float;
    DECLARE _TipoCombustible varchar(45);
    DECLARE _VolumenSeco float;
    DECLARE _PesoSeco float;
    DECLARE _VolumenFrio float;
    DECLARE _PesoFrio float;
    DECLARE _VolumenMixto float;
    DECLARE _RendimientoThermo float;
    DECLARE _Fletera varchar(100);
    DECLARE _idFletera INT;

	
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
  
	SET _iVehiculos = JSON_LENGTH(_vehiculos);
	SET _iVehiculo = 0;


	WHILE _iVehiculo < _iVehiculos DO
		SET _Cantidad = JSON_UNQUOTE(JSON_EXTRACT(_vehiculos, CONCAT('$[',_iVehiculo,'].Cantidad')));
        SET _TipoVehiculo = JSON_UNQUOTE(JSON_EXTRACT(_vehiculos, CONCAT('$[',_iVehiculo,'].TipoVehiculo')));
        SET _Rendimiento = JSON_UNQUOTE(JSON_EXTRACT(_vehiculos, CONCAT('$[',_iVehiculo,'].Rendimiento')));
        SET _Talla = JSON_UNQUOTE(JSON_EXTRACT(_vehiculos, CONCAT('$[',_iVehiculo,'].Talla')));
        SET _HuellaCarbono = JSON_UNQUOTE(JSON_EXTRACT(_vehiculos, CONCAT('$[',_iVehiculo,'].HuellaCarbono')));
        SET _TipoCombustible = JSON_UNQUOTE(JSON_EXTRACT(_vehiculos, CONCAT('$[',_iVehiculo,'].TipoCombustible')));
        SET _VolumenSeco = JSON_UNQUOTE(JSON_EXTRACT(_vehiculos, CONCAT('$[',_iVehiculo,'].VolumenSeco')));
        SET _PesoSeco = JSON_UNQUOTE(JSON_EXTRACT(_vehiculos, CONCAT('$[',_iVehiculo,'].PesoSeco')));
        SET _VolumenFrio = JSON_UNQUOTE(JSON_EXTRACT(_vehiculos, CONCAT('$[',_iVehiculo,'].VolumenFrio')));
        SET _PesoFrio = JSON_UNQUOTE(JSON_EXTRACT(_vehiculos, CONCAT('$[',_iVehiculo,'].PesoFrio')));
        SET _VolumenMixto = JSON_UNQUOTE(JSON_EXTRACT(_vehiculos, CONCAT('$[',_iVehiculo,'].VolumenMixto')));
        SET _RendimientoThermo = JSON_UNQUOTE(JSON_EXTRACT(_vehiculos, CONCAT('$[',_iVehiculo,'].RendimientoThermo')));
        SET _Fletera = JSON_UNQUOTE(JSON_EXTRACT(_vehiculos, CONCAT('$[',_iVehiculo,'].Fletera')));

		INSERT IGNORE INTO fleteras (idCedi, fletera)
		VALUES(_idCedi, upper(_Fletera));

		SET _idFletera = (SELECT idFletera from fleteras WHERE fletera = _Fletera AND idCedi = _idCedi);

		INSERT INTO vehiculos(idCedi,
                                Cantidad,
                                TipoVehiculo,
                                Rendimiento,
                                Talla,
                                HuellaCarbono,
                                TipoCombustible,
                                VolumenSeco,
                                PesoSeco,
                                VolumenFrio,
                                PesoFrio,
                                VolumenMixto,
                                RendimientoThermo,
                                idFletera)
		VALUES(
				_idCedi,
                _Cantidad,
                _TipoVehiculo,
                _Rendimiento,
                _Talla,
                _HuellaCarbono,
                _TipoCombustible,
                _VolumenSeco,
                _PesoSeco,
                _VolumenFrio,
                _PesoFrio,
                _VolumenMixto,
                _RendimientoThermo,
                _idFletera)
        ON DUPLICATE KEY UPDATE
            Cantidad = _Cantidad,
            Rendimiento = _Rendimiento,
            Talla = _Talla,
            HuellaCarbono = _HuellaCarbono,
            TipoCombustible = _TipoCombustible,
            VolumenSeco = _VolumenSeco,
            PesoSeco = _PesoSeco,
            VolumenFrio = _VolumenFrio,
            PesoFrio = _PesoFrio,
            VolumenMixto = _VolumenMixto,
            RendimientoThermo = _RendimientoThermo;

		

	    SET _iVehiculo = _iVehiculo + 1;
	END WHILE;


	COMMIT;

    SELECT 'OK' AS result, 'Registro correcto' AS Data;
END //
