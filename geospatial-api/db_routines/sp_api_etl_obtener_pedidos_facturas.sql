-- ---------------------------------------------------------------------
-- RACH - Viernes 09 de Agosto de 2019
-- Para obtener los viajes despegados y en transito
-- ---------------------------------------------------------------------
DROP PROCEDURE IF EXISTS sp_etl_obtener_pedidos_facturas;

DELIMITER //

CREATE PROCEDURE `sp_etl_obtener_pedidos_facturas`(
	_idCedi INT,
	_json_viajes JSON
)
BEGIN
    DECLARE _transact boolean DEFAULT(false);
	DECLARE _viajes INT;
	DECLARE _viaje INT;
	DECLARE _msg TEXT;
	DECLARE _code CHAR(5) DEFAULT '00000';

	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN

		GET DIAGNOSTICS CONDITION 1
			_code = RETURNED_SQLSTATE,_msg = MESSAGE_TEXT;

		IF _transact THEN
			ROLLBACK;
		END IF;

        SELECT 'FAIL' AS result, _msg AS data;

	END;

	SET SESSION time_zone = 'America/Mexico_City';

	-- tabla temporal para viajes
	DROP TEMPORARY TABLE IF EXISTS _viajes;
    CREATE TEMPORARY TABLE _viajes (
		viaje int
    );

	SET _viajes = JSON_LENGTH(_json_viajes);
	SET _viaje = 0;

	WHILE _viaje < _viajes DO
		INSERT INTO _viajes(
			viaje
        )
        VALUES(
			JSON_UNQUOTE(JSON_EXTRACT(_json_viajes, CONCAT('$[',_viaje,'].viaje')))
        );
	    SET _viaje = _viaje + 1;

	END WHILE;

	# que todos los viajes existan
    IF  EXISTS(
            SELECT v.idviaje
            FROM
				_viajes _v
                LEFT JOIN viajes v
                ON _v.viaje = v.idviaje
			WHERE
				v.idviaje IS NULL
                AND v.idCedi =_idCedi
			LIMIT 1
        ) THEN
            SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'No existen algunos viajes';
        END IF;

    # operacion exitosa
	SELECT 'OK' AS result, '' AS data;

	#Obtengo la info de los pedidos
    SELECT
        pe.idViaje as Viaje,
        pa.Tirada as TiradaPlan,
        pe.Pedido as Pedido,
        pe.Volumen as Volumen,
        pe.Peso as Peso,
        pe.Piezas as Piezas,
        pe.Valor as Valor,
        fa.factura as Factura

	FROM
		pedidos pe
		JOIN
		_viajes _v
		ON pe.idviaje = _v.viaje
		JOIN
		paradas pa
		ON pe.idparada = pa.idparada
		LEFT JOIN
		pedido_facturas fa
		ON pe.idpedido = fa.idpedido

        order by viaje, tiradaplan;

END //



# call sp_etl_obtener_pedidos_facturas(16, '[{"viaje": 538} ,{"viaje": 537}]');