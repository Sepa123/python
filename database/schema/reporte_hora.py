def reporte_hora_schema(reporte_hora):
    return  {
    "Hora": reporte_hora[0],
    "Electrolux": reporte_hora[1], 
    "Sportex":reporte_hora[2],
    "Easy_CD": reporte_hora[3],
    "Tiendas": reporte_hora[4]
    # cambiar Tiendas por Easy_OPL
}

def reportes_hora_schema(reportes_hora):
    # reportes = [reporte_hora_schema(reporte_hora) for reporte_hora in reportes_hora]

    # # print(reportes)
    # total_El = total_Spo = total_easy_CD = total_easy_opl = total_tiendas = 0

    # for i in range(1, 5):

    #     total_El += reportes[i]["Electrolux"]
    #     total_Spo += reportes[i]["Sportex"]
    #     total_easy_CD += reportes[i]["Easy_CD"]
    #     total_tiendas += reportes[i]["Tiendas"]
    #     total_easy_opl += reportes[i]["Easy_OPL"]
  
    # reportes[0]["Electrolux"] -= total_El
    # reportes[0]["Sportex"] -= total_Spo
    # reportes[0]["Easy_CD"] -= total_easy_CD
    # reportes[0]["Tiendas"] -= total_tiendas
    # reportes[0]["Easy_OPL"] -= total_easy_opl

    # reportes = reportes[:1] + reportes[5:]

    # return reportes

    return [reporte_hora_schema(reporte_hora) for reporte_hora in reportes_hora]


def reportes_ultima_hora_schema(reportes_hora):
    reporte = [reporte_hora_schema(reporte_hora) for reporte_hora in reportes_hora]
    reporte.pop()
    
    return reporte


    