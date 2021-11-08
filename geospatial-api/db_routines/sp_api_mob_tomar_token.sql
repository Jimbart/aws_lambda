-- --------------------------------------------------------------------------------
-- RAQUEL, 18 de junio de 2018 (GMT-5)
-- Note: para asignar un viaje en mobility

-- --------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS `sp_api_mob_tomar_token`;

DELIMITER // 

CREATE PROCEDURE `sp_api_mob_tomar_token`(
	_idCedi int
	,_idViaje int
    ,_operador VARCHAR(200)
    ,_fletera VARCHAR(100)
    ,_correo_operador VARCHAR(100)
    ,_placas VARCHAR(7)
    ,_numero_economico int
)
BEGIN
	DECLARE _json_res json;
	DECLARE _msg TEXT;  
	DECLARE _code CHAR(5) DEFAULT '00000';

	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN
		
		GET DIAGNOSTICS CONDITION 1
			_code = RETURNED_SQLSTATE,_msg = MESSAGE_TEXT;
        
		select
			'FAIL' as resultado
			,_msg as info;
            
	END;       

	SET SESSION time_zone = 'America/Mexico_City';
    
	if  not exists (		-- valido cedi
		select idCedi
        from cedis
        where
			idCedi = _idCedi
				) then
		signal sqlstate '45000' set message_text = 'No existe el cedi';
        
	end if;

    if  not exists (		-- valido token
        select idViaje
        from viajes
        where
            idViaje = _idViaje
            and idCedi = _idCedi
            and status = 9
                ) then
        signal sqlstate '45000' set message_text = 'No existe el viaje';
        
    end if;
    
    UPDATE viajes
    SET 
        operador = _operador,
        fletera = _fletera,
        correo_operador = _correo_operador,
        placas = _placas,
        numero_economico = _numero_economico,
        Status = 9.5
    WHERE
        idViaje = _idViaje AND idCedi = _idCedi;
    
    select 'OK' as resultado, 'Viaje asignado correctamente';

	

END //
