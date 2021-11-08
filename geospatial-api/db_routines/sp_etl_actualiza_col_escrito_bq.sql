-- --------------------------------------------------------------------------------
-- JATO 1 de noviembre de 2018 para registrar status de viajes
-- --------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_etl_actualiza_col_escrito_bq;

DELIMITER //

CREATE PROCEDURE `sp_etl_actualiza_col_escrito_bq`(
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
        ,escrito_BQ int
    );

	SET _viajes = JSON_LENGTH(_json_viajes);
	SET _viaje = 0;

	WHILE _viaje < _viajes DO
		INSERT INTO _viajes(
			viaje
			,escrito_BQ
        )
        VALUES(
			JSON_UNQUOTE(JSON_EXTRACT(_json_viajes, CONCAT('$[',_viaje,'].Viaje')))
            ,JSON_EXTRACT(_json_viajes, CONCAT('$[',_viaje,'].escritoBQ'))
        );
	    SET _viaje = _viaje + 1;

	END WHILE;

	# que todos los viajes existan
    IF  EXISTS(
            SELECT v.Viaje
            FROM
				_viajes _v
                LEFT JOIN viajes v USING(viaje)
			WHERE
				v.viaje IS NULL
                AND v.idCedi =_idCedi
			LIMIT 1
        ) THEN
            SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'No existen algunos viajes';
        END IF;

	SET _transact = true;
    START TRANSACTION;

	# actualizo status de viajes
	UPDATE
		_viajes _v
		JOIN viajes v USING(viaje)
	SET
		v.escrito_BQ = _v.escrito_BQ
	WHERE
		v.escrito_BQ <> _v.escrito_BQ;

	COMMIT;

    SELECT
		'OK' AS result
        , '' AS Data;

END //
