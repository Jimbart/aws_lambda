DROP  TRIGGER IF EXISTS trg_cedis_has_usuario_BU;

DELIMITER //

CREATE  TRIGGER trg_cedis_has_usuario_BU
BEFORE UPDATE ON cedis_has_usuarios FOR EACH ROW 
BEGIN 
    declare msg varchar(128);
    SET SESSION time_zone = 'America/Mexico_City';
    
    -- candado para no hacer un update null
    SET new.ssVehiculos = ifnull(new.ssVehiculos, old.ssVehiculos);
    SET new.ssDestinos = ifnull(new.ssDestinos, old.ssDestinos);
    SET new.ssTarifario = ifnull(new.ssTarifario, old.ssTarifario);
    SET new.ssPreferencias = ifnull(new.ssPreferencias, old.ssPreferencias);
    SET new.ssIncompatibilidades = ifnull(new.ssIncompatibilidades, old.ssIncompatibilidades);
    
END