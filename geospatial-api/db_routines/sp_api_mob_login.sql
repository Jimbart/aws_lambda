-- --------------------------------------------------------------------------------
-- RAQUEL, 15 de mayo de 2019 (GMT-5)
-- Note: para hacer login para el mobility
-- --------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS `sp_api_mob_login`;

DELIMITER // 

CREATE PROCEDURE `sp_api_mob_login`(
	_cedi varchar(140)
	,_password varchar(140)
)
BEGIN
	DECLARE _msg TEXT;  
	DECLARE _code CHAR(5) DEFAULT '00000';
    DECLARE _json_res json;

	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN
		
		GET DIAGNOSTICS CONDITION 1
			_code = RETURNED_SQLSTATE,_msg = MESSAGE_TEXT;
        
		select
			'FAIL' as resultado
			,_msg as info;
            
	END;       

	SET SESSION time_zone = 'America/Mexico_City';
    
	if  not exists (		-- valido correo
		select idCedi
        from cedis
        where
			cedis = _cedi
				) then
		signal sqlstate '45000' set message_text = 'No existe el cedi';
        
	end if;

	if  not exists (		-- valido password
		select cediS
        from cedis
            join empresas using(idEmpresa)
        where
			cedis = _cedi
			and Password = _password 
				) then
		signal sqlstate '45000' set message_text = 'Password incorrecto';
        
	end if;
    
    set _json_res = (select 
						json_object('idEmpresa',
								idEmpresa,
								'empresa',
								empresa,
								'idCedi',
								idCedi,
								'cedis',
								cedis,
								'fletera',
								if(e.fleteras = 1, 'si', 'no'))
					from
						empresas e
							join
						cedis c USING (idEmpresa)
					where
						cedis = _cedi
							and Password = _password);
		
    select
		'OK' as resultado
		, _json_res as data;

END //

call sp_api_mob_login('SILODISA', 'silodisa');
