import xarray as xr
import pandas as pd
import os
import numpy as np
import rioxarray as rio
import geopandas as gpd
import pdb

def load_grid_data(file_path, variable, shapefile):
    """
    Carga datos nuevas para una variable específica desde un archivo GRIB.
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
    grid_data = grid_data.rio.write_crs("EPSG:4326")

    #recortar usando la geometría del shapefile
    grid_data = grid_data.rio.clip(shapefile.geometry, shapefile.crs, drop=True)

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


def calcular_anomalias_lluvia(data, p):
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

    return counts.max() - 1
    
    
def calcular_interpolacion(data, aux_year, ruta_salida):
    
    ruta = ruta_salida

    data = data.where(data < 0.001, other=1)
    data = data.where(data >= 0.001, other=0)

    # Calcular la suma acumulada de días secos dentro de cada año
    data_cumsum = data.cumsum(dim='time')  # Asumiendo que data tiene un solo año
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

    count_most_frequent_da = count_most_frequent_da.squeeze("year")

    if aux_year == 1961:
        
        count_most_frequent_da = count_most_frequent_da.expand_dims(month=np.arange(1, 13))
        
        CDD = count_most_frequent_da * (count_most_frequent_da['month'] / 12)
    
    else:
        aux_year -= 1
        data1 = count_most_frequent_da
        data2 = xr.open_dataarray(ruta + '/' +f'datos_maximos_{aux_year}.nc')
        aux_year += 1
        
        data_aligned = data2.sel(latitude=data1.latitude, longitude=data1.longitude, method="nearest")
        
        if isinstance(data1, xr.DataArray):
            data1 = data1.to_dataset(name="tp")  

        data1 = data1.assign(tp_daily_sum = data_aligned)  

        data1 = data1.expand_dims(month=np.arange(1, 13))
        
        data1['new'] = xr.where(
            data1['month'] != 12,
            ((12 - data1['month'])/12)*data1['tp_daily_sum'] + (data1['month']/12)*data1['tp'],  
            data1['tp']
        )


        CDD = data1['new']


    CDD.sel(month=12).to_netcdf(ruta+'/'+ f'datos_maximos_{aux_year}.nc')
    
    new_date = pd.to_datetime(
    dict(year=aux_year, month=CDD['month'].values, day=1))

    # Ahora asignamos la nueva coordenada "date" a lo largo de la dimensión "month"
    CDD = CDD.assign_coords(date=("month", new_date))

    CDD = CDD.rename("tp_daily_sum")
    CDD = CDD.assign_coords(month=CDD.coords["date"].values)
    CDD = CDD.rename({"month": "time"})

    return CDD

def calcular_anomalias_sequia(data, p, year, ruta_salida):
    """
    Calcula las anomalías de sequía con base en la duración de días secos consecutivos.

    Parámetros:
    - data (xarray.Dataset): Datos de precipitación diaria.
    - p (xarray.Dataset): Dataset con media y desviación estándar de los días secos consecutivos.

    Retorna:
    - anomalias_sequia (xarray.Dataset): Anomalías de sequía.
    """
    # Obtener la serie interpolada de días secos consecutivos
    CDD = calcular_interpolacion(data, year, ruta_salida)

    # Alinear los datos con las estadísticas de referencia
    CDD = alinear_data(CDD, p, 0)

    # Calcular anomalías
    anomalias_sequia = anomalias(CDD)

    return anomalias_sequia

def load_estadisticas(estadisticas_file, shapefile_path):
    #calcular la lluvia
    estadisticas_data = xr.open_dataset(estadisticas_file)

    estadisticas_data = estadisticas_data.rio.set_spatial_dims(x_dim='longitude', y_dim='latitude')
    estadisticas_data = estadisticas_data.rio.write_crs("EPSG:4326")
    # Recortar el conjunto de datos
    estadisticas_data = estadisticas_data.rio.clip(shapefile_path.geometry, shapefile_path.crs)

    return estadisticas_data

def procesar_anomalias(est1, est2, file, shapefile, year, ruta_salida):
    
    estadisticas1 = load_estadisticas(est1, shapefile)
    estadisticas2 = load_estadisticas(est2, shapefile)

    archivo = load_grid_data(file, "tp", shapefile)

    #aplicar resample
    ds_resampled = resample_to_daily_precipitation(archivo)
    
    ds_resampled = ds_resampled.sel(time=slice(f"{year}-01-01", f"{year}-12-31"))

    #calcular anomalias para esos años
    anomalias_lluvia = calcular_anomalias_lluvia(ds_resampled, estadisticas1)
    anomalias_sequia = calcular_anomalias_sequia(ds_resampled, estadisticas2, year, ruta_salida)

    anomalias_lluvias_resultado = anomalias_lluvia['anomalias'].groupby("time.month").mean(dim=["latitude", "longitude"])
    anomalias_sequia_resultado = anomalias_sequia['anomalias'].groupby("time.month").mean(dim=["latitude", "longitude"])

    return anomalias_lluvias_resultado, anomalias_sequia_resultado

def ejecutar_codigo(shapefile_path, ruta, ruta_grib, ruta_salida):

    shapefile = gpd.read_file(shapefile_path)
    
    df1 = []
    df2 = []

    for year in range(1961, 2025):
        # Procesar anomalías
        anomalias_mensuales_lluvia, anomalias_mensuales_sequia = procesar_anomalias(ruta + '/'+'era5_lluvias_percentil.nc',ruta +'/'+ 'era5_sequia_percentil.nc', ruta_grib +'/' + f'era5_rain_{year}.grib', shapefile, year, ruta_salida)

        for mes in range(1, 13):
            try:
                anomalia_lluvia_mes = anomalias_mensuales_lluvia.sel(time=anomalias_mensuales_lluvia.time.dt.month == mes, method="nearest").item()
                df1.append({"Año": year, "Mes": mes, "Anomalia_Lluvia": anomalia_lluvia_mes})
                
                anomalia_sequia_mes = anomalias_mensuales_sequia.sel(time=anomalias_mensuales_sequia.time.dt.month == mes, method="nearest").item()
                df2.append({"Año": year, "Mes": mes, "Anomalia_Sequia": anomalia_sequia_mes})

            except KeyError as e:
                print(f"Error con el mes {mes} para el año {year}: {e}")
        print(f'Procesado year {year}')

    # Convertir a DataFrame
    df_lluvia = pd.DataFrame(df1)
    df_sequia = pd.DataFrame(df2)

    # Guardar en CSV
    df_lluvia.to_csv(ruta +'/'+ "anomalies_precipitation_combined.csv", index=False)
    df_sequia.to_csv(ruta +'/'+ "anomalies_drought_combined.csv", index=False)

    return df_lluvia, df_sequia

if __name__ == "__main__":

    shapefile_path = "../../data/shapefiles/colombia_4326.shp"
    ruta = "../../data/processed"
    ruta_grib = "../../data/raw/era5"
    ruta_salida = "../../data/processed"


    df1, df2 = ejecutar_codigo(shapefile_path, ruta, ruta_grib, ruta_salida)
    print('Proceso finalizad')
