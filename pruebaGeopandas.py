# import geopandas as gpd
from datetime import datetime, timedelta
fecha_actual = datetime.now() 
print(fecha_actual)
# from shapely.geometry import Polygon, Point

# # Crear un DataFrame con los puntos (EASY AV VICUÑA )
# data = {'Latitude': [-33.508195604554686, -33.51376586182658, -33.512302523881594, -33.50769992869531],
#         'Longitude': [-70.61144567995603, -70.60782223433345, -70.60139627998718, -70.60417048054195]}

# # Corregir esta línea
# df = gpd.GeoDataFrame(data, geometry=gpd.points_from_xy(data['Longitude'], data['Latitude']))


# # Crear un objeto Polygon
# polygon = Polygon(zip(df['Longitude'], df['Latitude']))

# # Crear un GeoDataFrame con el polígono
# gdf_polygon = gpd.GeoDataFrame({'geometry': [polygon]}, geometry='geometry')

# # Definir una función para verificar si una ubicación está dentro del polígono
# def point_inside_polygon(lat, lon):
#     point = Point(lon, lat)
#     return point.within(gdf_polygon['geometry'].iloc[0])

# # Ejemplo de uso
# latitud_prueba = input("¿me pasas la latitud? ")
# longitud_prueba = input("¿me pasas la longitud? ")

# if point_inside_polygon(latitud_prueba, longitud_prueba):
#     print(f'La ubicación {latitud_prueba}, {longitud_prueba} está dentro del perímetro de EASY VICUÑA.')
# else:
#     print(f'La ubicación {latitud_prueba}, {longitud_prueba} está fuera del perímetro.')
