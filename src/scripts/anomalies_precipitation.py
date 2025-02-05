import xarray as xr
import pandas as pd
import os
import numpy as np
import rioxarray as rio
import geopandas as gpd

# Procesamiento de datos

def load_grid_data(file_path, variable, shapefile_path=None):
    """
    Carga datos de una malla (grilla) para una variable específica desde un archivo GRIB.
    Si se proporciona un shapefile, recorta los datos a la región especificada.
    
    Parámetros:
    - file_path (str): Ruta del archivo GRIB que contiene los datos.
    - variable (str): Nombre de la variable a extraer del archivo.
    - shapefile_path (str, opcional): Ruta del shapefile que define la región a recortar.

    Retorna:
    - grid_data (xarray.DataArray): Datos de la variable seleccionada, recortados si se proporciona un shapefile.
    """
    #cargar el archivo GRIB y extraer la variable específica
    grid_data = xr.open_dataset(file_path, engine="cfgrib")[variable]

    #si se proporciona un shapefile, recortar los datos a la región especificada
    if shapefile_path:
        
        #cargar el shapefile
        shape = gpd.read_file(shapefile_path)
        
        #asignar el sistema de referencia
        grid_data = grid_data.rio.write_crs("EPSG:4326", inplace=True)

        #recortar usando la geometría del shapefile
        grid_data = grid_data.rio.clip(shape.geometry, shape.crs, drop=True)

    return grid_data

def resample_to_daily_precipitation(grid_data):
    """
    Convierte datos de precipitación horaria en sumas diarias.

    Parámetros:
    - grid_data (xarray.DataArray): Datos de precipitación con resolución horaria.

    Retorna:
    - daily_sum (xarray.DataArray): Datos de precipitación sumados a nivel diario.
    """
    #ajustar a UTC-5 (hora de Colombia)
    grid_data['valid_time'] = grid_data['valid_time'] - pd.Timedelta(hours=5)

    #aplanar la estructura de datos combinando las dimensiones 'time' y 'step'
    grid_data_plain = grid_data.stack(valid_time_dim=("time", "step")).reset_index("valid_time_dim")

    #agrupar por día y sumar los valores de precipitación diaria
    daily_sum = grid_data_plain.groupby(grid_data_plain["valid_time"].dt.floor("D")).sum(dim='valid_time_dim')

    #renombrar la dimensión de tiempo
    daily_sum = daily_sum.rename({"floor": "time"})

    return daily_sum

### Codigos generales para calcular estadisticas y anomalias

def calcular_estadisticas(data):
    """
    Calcula la media y la desviación estándar de los datos agrupados por mes.

    Parámetros:
    - data (xarray.DataArray): Datos a procesar.

    Retorna:
    - mean_tp (xarray.DataArray): Media mensual de los datos.
    - std_tp (xarray.DataArray): Desviación estándar mensual de los datos.
    """
    mean_tp = data.groupby('time.month').mean(dim='time')  #calcula la media mensual
    std_tp = data.groupby('time.month').std(dim='time')    #calcula la desviación estándar mensual
    
    return mean_tp, std_tp

def guardar_estadisticas(data, archivo_salida):
    """
    Guarda los datos en un archivo NetCDF y, si es posible, en un archivo CSV.

    Parámetros:
    - data (xarray.Dataset): Datos a guardar.
    - archivo_salida (str): Nombre del archivo de salida.

    """
    #guardar en formato NetCDF
    data.to_netcdf(archivo_salida)

    try:
        #convertir los datos de NetCDF a DataFrame y guardar como CSV
        subset = data[['mean_tp','std_tp']].to_dataframe().reset_index()
        output_path = archivo_salida.replace('.nc', '.csv')  # Cambiar la extensión de .nc a .csv
        subset.to_csv(output_path, index=False)

    except Exception as e:
        print(f"Error al guardar el archivo CSV: {e}")

def alinear_data(data1, data2, op):
    meses = data1["time"].dt.month

    mean_tp_aligned = data2["mean_tp"].sel(month=meses).drop_vars("month")
    std_tp_aligned = data2["std_tp"].sel(month=meses).drop_vars("month")

    #convertir en un Dataset si es un DataArray
    if isinstance(data1, xr.DataArray):
        data1 = data1.to_dataset(name="tp_daily_sum")

    data1 = data1.assign(mean_tp=mean_tp_aligned, std_tp=std_tp_aligned)
        
    if op == 1:
        return data1.drop_vars("month", errors="ignore")  # Evita errores si "month" no existe

    else:
        return data1

def anomalias(data):
    """
    Calcula las anomalías de precipitación.

    Parámetros:
    - data (xarray.Dataset): Datos con la precipitación diaria y sus estadísticas.

    Retorna:
    - data (xarray.Dataset): Datos con una nueva variable "anomalias".
    """
    return data.assign(anomalias=(data["tp_daily_sum"] - data["mean_tp"]) / data["std_tp"])

# codigos para procesar el indicador de lluvias

def calcular_5_dias_maximo(data):
    """
    Calcula el máximo acumulado de precipitación en un periodo de 5 días consecutivos dentro de cada mes.

    Parámetros:
    - data (xarray.DataArray): Datos de precipitación diaria.

    Retorna:
    - Rx5day (xarray.DataArray): Máximo acumulado de precipitación en 5 días consecutivos por mes.
    """
    # Sumar la precipitación en ventanas móviles de 5 días
    data_5_day_sum = data.rolling(time=5, min_periods=1).sum()

    # Agregar la columna de mes para verificar que los 5 días pertenezcan al mismo mes
    data_5_day_sum['month'] = data_5_day_sum['time'].dt.month

    # Calcular la diferencia de mes entre el primer y último día de la ventana de 5 días
    month_diff = data_5_day_sum['month'] - data_5_day_sum['month'].shift(time=4)

    # Crear una máscara para filtrar solo los valores donde los 5 días están en el mismo mes
    mask = (month_diff == 0)

    # Aplicar la máscara para eliminar valores que cruzan de un mes a otro
    data_5_day_sum = data_5_day_sum.where(mask, drop=True)

    # Seleccionar el máximo acumulado de 5 días dentro de cada mes
    Rx5day = data_5_day_sum.resample(time='1M').max()

    return Rx5day

def calcular_lluvia(data):
    """
    Calcula la media y desviación estándar del máximo acumulado de 5 días de lluvia por mes.

    Parámetros:
    - data (xarray.DataArray): Datos de precipitación diaria.

    Retorna:
    - p (xarray.Dataset): Contiene la media y desviación estándar del acumulado máximo de 5 días.
    """
    # Aplicar la función para obtener el máximo acumulado en 5 días
    Rx5day = calcular_5_dias_maximo(data)

    # Calcular la media y desviación estándar por mes
    mean_tp, std_tp = calcular_estadisticas(Rx5day)

    # Crear un dataset con la media y la desviación estándar
    p = xr.Dataset({
        'mean_tp': mean_tp['tp_daily_sum'],
        'std_tp': std_tp['tp_daily_sum']
    })

    return p

def calcular_anomolias_lluvia(data, p):
    """
    Calcula las anomalías de precipitación basada en el máximo acumulado de 5 días.

    Parámetros:
    - data (xarray.DataArray): Datos de precipitación diaria.
    - p (xarray.Dataset): Dataset con la media y desviación estándar del acumulado máximo de 5 días.

    Retorna:
    - anomalias_lluvia (xarray.Dataset): Anomalías de precipitación.
    """
    # Aplicar la función para obtener el máximo acumulado en 5 días
    Rx5day = calcular_5_dias_maximo(data)

    # Alinear los datos con las estadísticas mensuales de referencia
    Rx5day = alinear_data(Rx5day, p, 1)

    # Calcular anomalías de precipitación
    anomalias_lluvia = anomalias(Rx5day)

    return anomalias_lluvia

# codigos para procesar el indicador de sequia

def count_most_frequent_with_condition(data):
    """
    Encuentra la frecuencia máxima de los valores en el array y ajusta según la condición:
    - Si el valor `0` está presente en los datos, devuelve la frecuencia máxima.
    - Si `0` no está presente, resta 1 a la frecuencia máxima.

    Parámetros:
    - data (numpy array): Datos de entrada.

    Retorna:
    - int: Frecuencia máxima ajustada.
    """
    values, counts = np.unique(data, return_counts=True)

    if 0 in values:
        return counts.max()
    else:
        return counts.max() - 1
    

def calcular_interpolacion(data):
    """
    Calcula la interpolación lineal de la cantidad de días secos consecutivos (CDD).

    Parámetros:
    - data (xarray.Dataset): Datos de precipitación diaria.

    Retorna:
    - CDD (xarray.Dataset): Serie interpolada de días secos consecutivos.
    """
    # Extraer los años únicos presentes en los datos
    años = data['time.year'].values
    años_unicos = np.unique(años)
    años_validos = []

    # Filtrar años que tengan al menos 365 datos de precipitación
    for año in años_unicos:
        datos_año = data.sel(time=data['time.year'] == año)
        if len(datos_año['time']) >= 365:
            años_validos.append(año)

    # Filtrar solo los datos correspondientes a años válidos
    data = data.sel(time=np.isin(data['time.year'], años_validos))

    # Convertir precipitación a una serie binaria (1 si >= 0.001, 0 si < 0.001)
    data = data.where(data < 0.001, other=1)
    data = data.where(data >= 0.001, other=0)

    # Calcular la suma acumulada de días secos dentro de cada año
    data_cumsum = data.groupby('time.year').cumsum(dim='time')
    data_cumsum['time'] = data['time']

    # Aplicar la función para contar la cantidad más frecuente de días secos consecutivos
    count_most_frequent_da = xr.apply_ufunc(
        count_most_frequent_with_condition,
        data_cumsum.groupby('time.year'),
        input_core_dims=[['time']],  # Dimensión sobre la que se aplica
        output_core_dims=[[]],  # Resultado sin dimensiones adicionales
        vectorize=True,  # Vectorizar para múltiples coordenadas y años
        dask="allowed",  # Soporte para Dask si el dataset es grande
    )

    # Obtener el primer año en la serie de datos
    min_year = count_most_frequent_da['year'].min().values

    # Crear una nueva variable de tiempo con años consecutivos
    count_most_frequent_da['year'] = xr.cftime_range(start=str(min_year), periods=len(count_most_frequent_da['year']), freq='YE')

    # Convertir la variable 'year' en formato datetime estándar
    count_most_frequent_da['year'] = count_most_frequent_da['year'].astype('datetime64[ns]')

    # Realizar interpolación lineal para obtener valores mensuales
    CDD = count_most_frequent_da.resample(year='ME').interpolate('linear')
    CDD = CDD.rename({'year': 'time'})

    return CDD

def calcular_sequia(data):
    """
    Calcula la media y desviación estándar de la duración máxima de días secos consecutivos.

    Parámetros:
    - data (xarray.Dataset): Datos de precipitación diaria.

    Retorna:
    - d (xarray.Dataset): Dataset con media y desviación estándar del CDD.
    """
    # Obtener la serie de días secos consecutivos interpolados
    CDD = calcular_interpolacion(data)

    # Calcular media y desviación estándar
    mean_tp, std_tp = calcular_estadisticas(CDD)

    # Crear un dataset con las estadísticas de interés
    d = xr.Dataset({
        'mean_tp': mean_tp['tp_daily_sum'],
        'std_tp': std_tp['tp_daily_sum']
    })

    return d

def calcular_anomolias_sequia(data, p):
    """
    Calcula las anomalías de sequía con base en la duración de días secos consecutivos.

    Parámetros:
    - data (xarray.Dataset): Datos de precipitación diaria.
    - p (xarray.Dataset): Dataset con media y desviación estándar de los días secos consecutivos.

    Retorna:
    - anomalias_sequia (xarray.Dataset): Anomalías de sequía.
    """
    # Obtener la serie interpolada de días secos consecutivos
    CDD = calcular_interpolacion(data)

    # Alinear los datos con las estadísticas de referencia
    CDD = alinear_data(CDD, p, 0)

    # Calcular anomalías
    anomalias_sequia = anomalias(CDD)

    return anomalias_sequia

### Ejecucion de codigos - ACTUALIZAR RUTAS
if __name__ == "__main__":
    ruta = 'c:\\Users\\Usuario\\Downloads'
    #cargar los datos
    
    shapefile = gpd.read_file(ruta+"\\colombia_4326.shp")

    #datos para calcular la estadistica - ACTUALIZAR
    archivo_union = ruta+'\\era5_daily_combined_rain.nc'

    #calcular la lluvia
    tp_daily = xr.open_dataset(archivo_union)

    #recortar el dataset
    tp_daily = tp_daily.rio.set_spatial_dims(x_dim='longitude', y_dim='latitude')
    tp_daily = tp_daily.rio.write_crs("EPSG:4326")
    # Recortar el conjunto de datos
    tp_daily = tp_daily.rio.clip(shapefile.geometry, shapefile.crs)

    #ordenar_dataset
    tp_daily = tp_daily.sortby('time')

    #filtrar para calcular las estadisticas
    tp_filtered = tp_daily.sel(time=slice(f"1960-01-01", f"1990-12-31"))

    #calcular media y desviacion de referencias
    p = calcular_lluvia(tp_filtered) #lluvia
    d = calcular_sequia(tp_filtered) #sequia

    #guardar estadisticas - ACTUALIZAR
    guardar_estadisticas(p,ruta+'\\estadisticas_lluvia.nc')
    guardar_estadisticas(d,ruta+'\\estadisticas_sequia.nc')

    #calcular indices y anomalias (hasta 1990)
    anomalias_lluvia = calcular_anomolias_lluvia(tp_daily, p) #lluvias
    anomalias_sequia = calcular_anomolias_sequia(tp_daily, d) #sequia

    #desde el año 1990 en adelante
    col = ruta+"\\colombia_4326.shp"
    df_list = [] #guardar años

    #iterar sobre los años y cargar los archivos GRIB - ACTUALIZAR FECHAS Y RUTA
    for year in range(1991, 1993):
        file = ruta+f'\\era5_rain_{year}.grib'
        
        try:
            aux = load_grid_data(file, "tp", shapefile_path=col)
            df_list.append(aux)
            print(f"Cargado: {file}")

        except Exception as e:
            print(f"Error con {file}: {e}")

    #Unir todos los datasets en la dimensión `time`
    if df_list:
        ds_combined = xr.concat(df_list, dim="time")

        #aplicar resample
        ds_resampled = resample_to_daily_precipitation(ds_combined)


    #calcular anomalias para esos años
    anomalias_lluvia2 = calcular_anomolias_lluvia(ds_resampled, p) #lluvias
    anomalias_sequia2 = calcular_anomolias_sequia(ds_resampled, d) #sequia

    #combinar anomalias
    anomalias_lluvia_combined = xr.concat([anomalias_lluvia,anomalias_lluvia2], dim="time")
    anomalias_sequia_combined = xr.concat([anomalias_sequia,anomalias_sequia2], dim="time")

    # Guardar las anomalías combinadas en un archivo NetCDF
    anomalias_lluvia_combined.to_netcdf(ruta+"\\anomalias_lluvia.nc")
    anomalias_sequia_combined.to_netcdf(ruta+"\\anomalias_sequia.nc")

    # Calcular el promedio mensual en todo el mapa
    promedio_mensual_lluvia = anomalias_lluvia_combined.groupby("time.year").mean(dim=["latitude", "longitude"])
    promedio_mensual_sequia = anomalias_sequia_combined.groupby("time.year").mean(dim=["latitude", "longitude"])

    # Convertir a DataFrame
    df_lluvia = promedio_mensual_lluvia.to_dataframe().reset_index()
    df_sequia = promedio_mensual_sequia.to_dataframe().reset_index()

    # Guardar en CSV
    df_lluvia.to_csv(ruta+"\\anomalies_precipitation_combined.csv", index=False)
    df_sequia.to_csv(ruta+"\\anomalies_drought_combined.csv", index=False)
