-- --------------------------------------------------------------------------------
-- RAQUEL, 16 de mayo de 2019 (GMT-5)
-- Note: para validar token para mobility
-- 
-- --------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS `sp_api_mob_validar_token`;

DELIMITER // 

CREATE PROCEDURE `sp_api_mob_validar_token`(
	_idCedi int
	,_token varchar(6)
)
BEGIN
	DECLARE _operador VARCHAR(45);
	DECLARE _message_text text;
	DECLARE _json_res int;
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
	SET _operador = (SELECT 
						operador
					FROM
						viajes
					WHERE
						token = _token);
	if exists (		-- valido que el viaje no haya sido tomado
		select idViaje
		from viajes
		where
			token = _token
			and idCedi = _idCedi
			and status = 9.5
				) then
		SET _operador = (SELECT 
						operador
					FROM
						viajes
					WHERE
						token = _token);
		SET _message_text = (Select concat('El token ya fue tomado por el operador ', _operador));
		signal sqlstate '45000' 
		set message_text = _message_text;
		
	end if;

	

	if  not exists (		-- valido token
		select idViaje
		from viajes
		where
			token = _token
			and idCedi = _idCedi
			and status = 9
				) then
		signal sqlstate '45000' set message_text = 'No existe el token';
		
	end if;

	
    
    SET _json_res = (SELECT 
                        idViaje
                    FROM
                        viajes
                    WHERE
                        token = _token AND idCedi = _idCedi);

    select
        'OK' as resultado
        , _json_res as data;

END //

call sp_api_mob_validar_token(16, '462190');