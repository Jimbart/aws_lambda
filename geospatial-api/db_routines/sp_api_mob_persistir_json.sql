-- --------------------------------------------------------------------------------
-- RACH, jueves, 8 de agosto de 2019 (GMT-5)
-- Note: para persistir json enviado desde mobility
-- --------------------------------------------------------------------------------

DROP PROCEDURE IF EXISTS `sp_api_mob_persistir_json`;

DELIMITER // 

CREATE PROCEDURE `sp_api_mob_persistir_json`(
	_viaje INT
    ,_json_mobility JSON
)
BEGIN

	DECLARE _msg TEXT;  
    DECLARE _transac BOOLEAN DEFAULT FALSE;  
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
    
    START TRANSACTION;
    SET _transac = TRUE;

		INSERT INTO json_viajes_mobility (
			timestamp
			,json
			,idViaje
		)
		VALUES (
			NOW()
			,_json_mobility
			,_viaje
		);
        
	COMMIT;
    SET _transac = FALSE;
    
    # resultados
    SELECT 
		'OK' AS result
        ,'' as Data;
    
END //
