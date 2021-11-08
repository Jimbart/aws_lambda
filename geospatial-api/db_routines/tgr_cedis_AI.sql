DROP  TRIGGER IF EXISTS tgr_cedis_AI;

DELIMITER //

CREATE  TRIGGER tgr_cedis_AI
AFTER INSERT ON cedis FOR EACH ROW 
BEGIN 
    declare msg varchar(128);
    declare _numero_cedis INT;
    SET SESSION time_zone = 'America/Mexico_City';

    set _numero_cedis =  (SELECT count(idCedi) FROM cedis where idEmpresa = NEW.idEmpresa);

    UPDATE empresas
    SET numeroCedis = _numero_cedis
    WHERE idEmpresa = NEW.idEmpresa;
    
END