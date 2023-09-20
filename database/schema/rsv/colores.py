def color_rsv(color):
    return {
		"Id" : color[0],
		"Created_at" : color[1],
		"Nombre_color" : color[2],
		"Codigo_html" : color[3],
		"Url_imagen" : color[4],
		"Extension" : color[5]
    }

def colores_rsv_schema(colores):
    return [color_rsv(color) for color in colores]
	