-- --------------------------------------------------------------------------------
-- RACH, jueves, 8 de agosto de 2019 (GMT-5)
-- Note: para obtener viaje desde cedi 
-- --------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS `sp_api_mob_obtener_cedi_desde_viaje`;

DELIMITER // 

CREATE PROCEDURE `sp_api_mob_obtener_cedi_desde_viaje`(
	_viaje INT
)
BEGIN

	DECLARE _msg TEXT;  
    DECLARE _transac BOOLEAN;  
	DECLARE _code CHAR(5) DEFAULT '00000';
    DECLARE _liquidacion VARCHAR(30);


	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN
		
		GET DIAGNOSTICS CONDITION 1
			_code = RETURNED_SQLSTATE,_msg = MESSAGE_TEXT;
            
		IF _transac THEN
			ROLLBACK;
		END IF;
        
		SELECT
			'FAIL' as resultado
			,_msg as info;
            
	END;       

	SET SESSION time_zone = 'America/Mexico_City';
    
    IF NOT EXISTS (		-- valido viaje
		SELECT idViaje
        FROM viajes
        WHERE
			idViaje = _viaje
				) then
		signal sqlstate '45000' set message_text = 'El viaje proporcionado no existe';
        
	END IF;    


	SELECT 
        'OK' as resultado
                ,JSON_OBJECT(
                    'idCedi', v.idCedi
                    ,'paradas', Tiradas
                ) as info
        FROM
            viajes v
        WHERE
                idViaje = _viaje
        GROUP BY v.idViaje ;

END //
