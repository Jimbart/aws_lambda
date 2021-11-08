-- ---------------------------------------------------------------------
-- RACH - Viernes 09 de Agosto de 2019
-- Para obtener los viajes despegados y en transito
-- ---------------------------------------------------------------------

DROP PROCEDURE IF EXISTS sp_api_etl_viajes_expirar;

DELIMITER //

CREATE PROCEDURE sp_api_etl_viajes_expirar(
    _id_cedi INT
)
BEGIN
    DECLARE _msg TEXT;
    DECLARE _code CHAR(5) DEFAULT '00000';

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        GET DIAGNOSTICS CONDITION 1
            _code = RETURNED_SQLSTATE, _msg = MESSAGE_TEXT;
        SELECT 'FAIL' AS result, _msg AS data;
    END;

    -- Verifico que exista el cedi
    IF NOT EXISTS(
        SELECT idCedi
        FROM cedis
        WHERE idCedi = _id_cedi
    ) THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'No existe el cedi';
    END IF;

    -- Verifico que existan viajes
    IF NOT EXISTS(
        SELECT
            COUNT(idViaje)
        FROM
            viajes
        WHERE
            status in (9, 9.5)
    ) THEN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'No hay viajes con el estatus solicitado';
    END IF;

    # operacion exitosa
	SELECT 'OK' AS result, '' AS data;

    select

        v.idViaje as Viaje,
        v.Status,
        v.fecharetorno as FechaRetornoPlan,
        CASE
	        WHEN  TIMESTAMPDIFF(Second, v.fechaRetorno, DATE_ADD(NOW(), INTERVAL - 5 HOUR))/(24*60*60) > 7 THEN 1
            ELSE 0 END AS Expirado

    FROM
        viajes v
        WHERE v.status in (9, 9.5)

        order by v.fecharetorno;

END //

-- call sp_api_etl_viajes_expirar(16)