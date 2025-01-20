import xarray as xr
import pandas as pd
import os
import numpy as np
import pdb

def calcular_maximos(archivo_entrada, periodo_referencia, variable):
    """
    Calcula las anomalías estandarizadas del máximo de precipitación acumulada en 5 días consecutivos (Rx5day).
    
    Parámetros:
        - archivo_entrada (str): Ruta del archivo NetCDF de entrada.
        - periodo_referencia (tupla): Período de referencia como (año_inicio, año_fin).
        - variable (str): nombre de la variable.
        
    Retorna:
        - xr.Dataset: Dataset con las anomalías estandarizadas calculadas.
    """
    
    #abrir archivo
    dataset = xr.open_dataset(archivo_entrada)

    #convertir tiempo a pandas datetime y ajustar a UTC-5
    dataset['valid_time'] = dataset['valid_time'] - pd.Timedelta(hours=5)

    #aplanar el dataset combinando 'time' y 'step'
    dataset = dataset.stack(valid_time_dim=("time", "step")).reset_index("valid_time_dim")

    #suma de precipitacion diaria por dia
    tp_daily = dataset[variable].groupby(dataset["valid_time"].dt.floor("D")).sum(dim='valid_time_dim')

    #acumulado de de 5 días
    tp_5_day_sum = tp_daily.rolling(floor=5, min_periods=1).sum()

    #validar que esten en el mismo mes
    tp_5_day_sum['month'] = tp_daily['floor'].dt.month #agregar mes
    month_diff = tp_5_day_sum['month'] - tp_5_day_sum['month'].shift(floor=4) #calcular la diferencia de mes
    mask = (month_diff == 0) #si la diferencia es 0, significa que los dias estan en el mismo mes

    #filtrar solo los dias que esten en el mismo mes
    tp_5_day_sum = tp_5_day_sum.where(mask, drop=True)

    #seleccionar el maximo de acumulado de 5 días de lluvia por mes
    Rx5day = tp_5_day_sum.resample(floor='1M').max()

    #agregar al dataset original las variables creadas
    dataset['tp_daily'] = tp_daily
    dataset['tp_5_day_sum'] = tp_5_day_sum
    dataset['Rx5day'] = tp_5_day_sum.resample(floor='1M').max()

    #filtrar por el periodo de referencia
    dataset_filtered = dataset.sel(floor=slice(f"{periodo_referencia[0]}-01-01", f"{periodo_referencia[1]}-12-31"))

    #calcular media y desviación estándar del periodo de referencia
    mean_tp = dataset_filtered['Rx5day'].groupby('floor.month').mean(dim='floor')
    std_tp = dataset_filtered['Rx5day'].groupby('floor.month').std(dim='floor')

    # Crear un nuevo Dataset con las variables tp_max_5_day_sum, mean_tp y std_tp
    p = xr.Dataset({
    'Rx5day': Rx5day,
    'mean_tp': mean_tp,
    'std_tp': std_tp
    })

    return p

def guardar_maximos(p, archivo_salida):
    """
    Guarda los resultados calculados en un archivo NetCDF y CSV.

    Parámetros:
        - p (xr.Dataset): Dataset con los máximos calculados.
        - archivo_salida (str): Ruta del archivo NetCDF de salida.
    """

    p.to_netcdf(archivo_salida)

    try:
        subset = p[['rx5day', 'media_referencia', 'std_referencia']].to_dataframe().reset_index()
        output_path = archivo_salida.replace('.nc', '_precipitaciones.csv')
        subset.to_csv(output_path, index=False)
    
    except Exception as e:
        print(f"Error al guardar el archivo CSV: {e}")

def main():
    ruta_datos = "../../data/processed"
    file = 'era5_rain_union.nc'
    archivo_union = os.path.join(ruta_datos, file)
    archivo_salida = os.path.join(ruta_datos, "era5_precipitaciones_maximo.nc")

    periodo_referencia = (1960, 1990)
    variable = 'tp'  

    try:
        p = calcular_maximos(archivo_union, periodo_referencia, variable)
        guardar_maximos(p, archivo_salida)
        print(f"Archivo creado en: {archivo_salida}")
    except Exception as e:
        print(f"Error al calcular máximos: {e}")

if __name__ == "__main__":
    main()
