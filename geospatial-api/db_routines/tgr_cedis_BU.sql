DROP  TRIGGER IF EXISTS tgr_cedis_BU;

DELIMITER //

CREATE  TRIGGER tgr_cedis_BU
BEFORE UPDATE ON cedis FOR EACH ROW 
BEGIN 
    declare msg varchar(128);
    SET SESSION time_zone = 'America/Mexico_City';
    
    # candado: cedis con coordenadas en 0
    if 
		(ifnull(new.latitud, 0) = 0)
		or (ifnull(new.longitud, 0) = 0)
			then
		set msg = ('La latitud o longitud del destino es nulo');
        signal sqlstate '45000' set message_text = msg;
    end if;

    # candado: cedis con coordenadas nuevas
    if 
        new.latitud <> old.latitud
		or new.longitud <> old.longitud
			then
		SET NEW.fechaActualizacion = NOW();
    ELSE
        SET NEW.fechaActualizacion = old.fechaActualizacion;
    end if;
    
END