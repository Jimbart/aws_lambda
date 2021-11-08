-- --------------------------------------------------------------------------------
-- RACH 3 de abril de 2019 (GMT-6) para obtener los vehiculos por cedis
-- --------------------------------------------------------------------------------

drop procedure if exists sp_api_arms_obtener_vehiculos_por_cedis;

DELIMITER // 

CREATE PROCEDURE `sp_api_arms_obtener_vehiculos_por_cedis`(
    _idCedi int)
BEGIN	

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

	SET SESSION group_concat_max_len = 100000000;
    
    SET SESSION time_zone = 'America/Mexico_City';
    

		select
				'OK' AS result
				,cast(
					concat('[',
						ifnull(group_concat(
							json_object(
								'idVehiculo', dat.idVehiculo
								,'Cantidad', dat.Cantidad
								,'TipoVehiculo', dat.TipoVehiculo
								,'VolumenSeco', dat.VolumenSeco
								,'PesoSeco', dat.PesoSeco
								,'VolumenFrio', dat.VolumenFrio
								,'PesoFrio', dat.PesoFrio
								,'VolumenMixto', dat.VolumenMixto
								,'RendimientoThermo', dat.RendimientoThermo
								,'Rendimiento', dat.Rendimiento
								,'Talla', dat.Talla
								,'HuellaCarbono', dat.HuellaCarbono
								,'TipoCombustible', dat.TipoCombustible
								,'Fletera', dat.Fletera
							)  
						), '')
					, ']')
				as json) as data
		from (
			select -- distinct
				v.idVehiculo
				, v.Cantidad
				, v.TipoVehiculo
				, v.VolumenSeco
				, v.PesoSeco
				, v.VolumenFrio
				, v.PesoFrio
				, v.VolumenMixto
				, v.RendimientoThermo
				, v.Rendimiento
				, v.Talla
				, v.HuellaCarbono
				, v.TipoCombustible
				, fl.fletera as Fletera
			from 
				vehiculos v
				JOIN fleteras fl USING(idFletera) 
			where
				v.idCedi = _idCedi		# lo desl cedi
				GROUP BY v.idVehiculo
			) dat;
    
END

