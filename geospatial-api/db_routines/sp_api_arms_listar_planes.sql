-- --------------------------------------------------------------------------------
-- RAQUEL miercoles, 20 de marzo de 2019 (GMT-5)
-- Note, Para listar los planes de un cedi
-- --------------------------------------------------------------------------------

drop procedure if exists sp_api_arms_listar_planes;

DELIMITER //

CREATE PROCEDURE `sp_api_arms_listar_planes`(
	   _idCedi int)
BEGIN

    declare _json_pns, _json_plan JSON;
    declare _i int;

    DECLARE _msg TEXT;
    DECLARE _resultado TEXT;
	DECLARE _code CHAR(5) DEFAULT '00000';

	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN

		GET DIAGNOSTICS CONDITION 1
			_code = RETURNED_SQLSTATE,_msg = MESSAGE_TEXT;

        SELECT 'FAIL' AS result, _msg AS data;

	END;

	SET SESSION time_zone = 'America/Mexico_City';
    SET SESSION group_concat_max_len = 4000000;
    
	if not exists (		-- verifico exista el cedi
		select cedis
        from
			cedis
		where
			idCedi = _idCedi
	) then
		SIGNAL SQLSTATE '45000'
			SET MESSAGE_TEXT = 'El cedi no existe';
	end if;

	drop temporary table if exists _planes_json;	-- json x pedido
	create temporary table _planes_json (
        idPlan int
		,json_plan JSON
    );

    
    insert into _planes_json(idPlan, json_plan)
    select
        p.idPlan
        ,JSON_OBJECT(
            'idPlan', p.idPlan
            ,'nombre', p.nombre
            , 'fechaCreacion', DATE_FORMAT(p.fechaCreacion, '%d/%m/%Y %H:%i')
            , 'fechaActualizacion', DATE_FORMAT(ifnull(p.fechaActualizacion, p.fechaCreacion), '%d/%m/%Y %H:%i')
            )
    FROM
    planes p
        JOIN
    viajes v USING (idPlan)
    WHERE
        p.idCedi = _idCedi
    GROUP BY p.idPlan;
    

    SET _json_pns = (select

              concat('[',
                group_concat(
                  json_plan
                ), ']') as json_plan
            from _planes_json);

    -- resultado
    if _json_pns is null then		-- verifico vs nulos
		SIGNAL SQLSTATE '45000'
			SET MESSAGE_TEXT = 'no existen planes';

	else
		SELECT 'OK' AS result, _json_pns AS data;

	end if;

END //

call sp_api_arms_listar_planes(16);