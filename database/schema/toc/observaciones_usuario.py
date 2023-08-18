def observacion_usuario(obs):
    return {
        "Observacion" : obs[0],
        "Ids_transyanez" : obs[1],
        "Alerta" : obs[2]
    }

def observaciones_usuario_schema(obss):
    return [observacion_usuario(obs) for obs in obss]