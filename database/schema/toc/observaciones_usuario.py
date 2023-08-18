def observacion_usuario(obs):
    return {
        "Observacion" : obs[0],
        "Id_usuario" : obs[1],
        "Ids_usuario" : obs[2],
        "Ids_transyanez" : obs[3],
        "Alerta" : obs[4]
    }

def observaciones_usuario_schema(obss):
    return [observacion_usuario(obs) for obs in obss]