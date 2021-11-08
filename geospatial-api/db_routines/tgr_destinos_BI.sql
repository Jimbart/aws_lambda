DROP  TRIGGER IF EXISTS tgr_destinos_BI;

DELIMITER //

CREATE  TRIGGER tgr_destinos_BI
BEFORE INSERT ON destinos FOR EACH ROW 
BEGIN 
    declare msg varchar(128);
    SET SESSION time_zone = 'America/Mexico_City';

    SET NEW.fechaActualizacion = NOW();

    # candado: destinos con coordenadas en 0
    if 
		(ifnull(new.latitud, 0) = 0)
		or (ifnull(new.longitud, 0) = 0)
			then
		set msg = ('La latitud o longitud del destino es nulo');
        signal sqlstate '45000' set message_text = msg;
    end if;
    
END