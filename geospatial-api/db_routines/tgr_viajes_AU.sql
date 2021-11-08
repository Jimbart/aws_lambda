-- --------------------------------------------------------------------------------
-- RACH, jueves 18 de julio de 2019 (GMT-6)
-- Note: trigger para despues de un update en la tabla de viajes
-- --------------------------------------------------------------------------------

DROP  TRIGGER IF EXISTS tgr_viajes_AU;

DELIMITER //

CREATE  TRIGGER tgr_viajes_AU
AFTER UPDATE ON viajes FOR EACH ROW 
BEGIN 
    declare msg varchar(128);
    SET SESSION time_zone = 'America/Mexico_City';

    # llenar la tabla de log viajes
    
    INSERT INTO log_viajes (idViaje, fecha_actualizacion, idUsuario, status)
    VALUES (new.idViaje, now(), new.idUsuario, new.status);
    
    
END