def reporte_vehiculos_schemas(reportes_vehiculo):
    return {
        "Compañia": reportes_vehiculo["compañia"],
        "Region origen": reportes_vehiculo["region_origen"],
        "Patente": reportes_vehiculo["patente"],
        "Estado": reportes_vehiculo["estado"],
        "Tipo": reportes_vehiculo["tipo"],
        "Caracteristicas": reportes_vehiculo["caracteristicas"],
        "Marca": reportes_vehiculo["marca"],
        "Modelo": reportes_vehiculo["modelo"],
        "Año": reportes_vehiculo["año"],
        "Region": reportes_vehiculo["region"],
        "Comuna": reportes_vehiculo["comuna"],
    }


def reportes_vehiculos_Schema(reportes_vehiculo):
    return [reporte_vehiculos_schemas(reportes_vehiculo) for reportes_vehiculos in reportes_vehiculo ]
