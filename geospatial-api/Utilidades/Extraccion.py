#-*- coding: utf-8 -*-

from logging import getLogger, basicConfig
from database_services.mysql_service import MySQLConnection
import pandas as pd
import json
import configparser

logger = getLogger()

class Extraccion:
    def __init__(self, *args, **kwargs):
        self.my_sql_conn = MySQLConnection('config.ini')

    def obtener_plan(self, idcedi,snombreplan):
        """listar_cedis"""
        try:
            logger.info('extraer pedidos')
            args = [idcedi,snombreplan]
            res = self.my_sql_conn.spexec('sp_util_extraccion_informacion', args)
            jsondata=pd.DataFrame(res,columns=['Pedido','CEDI','Destino','TipoPedido','Volumen','Peso','Valor','Piezas','VentanaFechaInicioPedido','VentanaFechaFinPedido','VentanaHoraInicioPedido','VentanaHoraFinPedido','TiemDescarga','EntregaRecoleccion','Productos','RecolectaCEDI2','NombreVendedor','CorreoVendedor','DestinoTR2','FechaEntregaTR2']).to_json(orient='records')

            return jsondata

        except Exception as exc:
            logger.info('Ocurrio un error al listar los cedis')
            logger.error(exc)
            return False, str(exc)
