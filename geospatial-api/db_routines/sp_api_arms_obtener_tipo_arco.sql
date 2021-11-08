-- --------------------------------------------------------------------------------
-- EDUARDO, miercoles 31 de julio de 2019 (GMT-6)
-- Note: para obtener el tipo de arco por cedi
-- --------------------------------------------------------------------------------

drop procedure if exists sp_api_arms_obtener_tipo_arco;

DELIMITER // 

CREATE PROCEDURE `sp_api_arms_obtener_tipo_arco`(
	_id_cedi INT)
BEGIN	

    DECLARE _msg TEXT;
    DECLARE _resultado TEXT;
	DECLARE _code CHAR(5) DEFAULT '00000';

    DECLARE _tipo_arco INT;

	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN

		GET DIAGNOSTICS CONDITION 1
			_code = RETURNED_SQLSTATE,_msg = MESSAGE_TEXT;

        SELECT 'FAIL' AS result, _msg AS data;

	END;

	SET SESSION time_zone = 'America/Mexico_City';
    SET SESSION group_concat_max_len = 4000000;

    if not exists (		-- verifico exista el correo
		select idCedi
        from
			cedis
		where
			idCedi = _id_cedi
	) then
		SIGNAL SQLSTATE '45000'
			SET MESSAGE_TEXT = 'El cedi no existe';
	end if;

    set _tipo_arco = (SELECT tipoArco FROM cedis WHERE idCedi = _id_cedi);

    -- resultado
    SELECT 'OK' AS result, _tipo_arco AS data;
        
END //



-- call sp_api_arms_obtener_tipo_arco(16);