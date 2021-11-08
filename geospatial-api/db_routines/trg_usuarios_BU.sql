DROP  TRIGGER IF EXISTS trg_usuarios_BU;

DELIMITER //

CREATE  TRIGGER trg_usuarios_BU
BEFORE UPDATE ON usuarios FOR EACH ROW 
BEGIN 
    declare msg varchar(128);
    SET SESSION time_zone = 'America/Mexico_City';
    
    -- candado para no hacer un update null
    SET new.ssVehiculos = ifnull(new.ssVehiculos, old.ssVehiculos);
    SET new.ssDestinos = ifnull(new.ssDestinos, old.ssDestinos);
    SET new.ssTarifario = ifnull(new.ssTarifario, old.ssTarifario);
    SET new.ssPreferencias = ifnull(new.ssPreferencias, old.ssPreferencias);
    SET new.ssIncompatibilidades = ifnull(new.ssIncompatibilidades, old.ssIncompatibilidades);
    SET new.idCediPreferido = ifnull(new.idCediPreferido, old.idCediPreferido);
    SET new.preferencias_grids = ifnull(new.preferencias_grids, old.preferencias_grids);
    
END