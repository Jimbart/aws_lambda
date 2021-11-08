-- --------------------------------------------------------------------------------
-- RACH, jueves 18 de julio de 2019 (GMT-6)
-- Note: trigger para despues de un insert en la tabla de viajes
-- --------------------------------------------------------------------------------

DROP  TRIGGER IF EXISTS tgr_viajes_AI;

DELIMITER //

CREATE  TRIGGER tgr_viajes_AI
AFTER INSERT ON viajes FOR EACH ROW 
BEGIN 
    declare msg varchar(128);
    SET SESSION time_zone = 'America/Mexico_City';

    # llenar la tabla de viajes
    INSERT INTO log_viajes (idViaje, fecha_actualizacion, idUsuario, status)
    VALUES (new.idViaje, now(), new.idUsuario, new.status);
    
END