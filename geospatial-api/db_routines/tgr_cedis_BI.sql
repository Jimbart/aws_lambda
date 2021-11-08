DROP  TRIGGER IF EXISTS tgr_cedis_BI;

DELIMITER //

CREATE  TRIGGER tgr_cedis_BI
BEFORE INSERT ON cedis FOR EACH ROW 
BEGIN 
    declare msg varchar(128);
    SET SESSION time_zone = 'America/Mexico_City';

    SET NEW.fechaActualizacion = NOW();

    # candado: cedis con coordenadas en 0
    if 
		(ifnull(new.latitud, 0) = 0)
		or (ifnull(new.longitud, 0) = 0)
			then
		set msg = ('La latitud o longitud del destino es nulo');
        signal sqlstate '45000' set message_text = msg;
    end if;
    
END