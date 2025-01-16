import xarray as xr
import pandas as pd
import os
import numpy as np

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

    #ajustar la fecha
    dataset['time'] = pd.to_datetime(dataset['time'].values)

    #convertir tiempo a pandas datetime y ajustar a UTC-5
    dataset['time'] = dataset['time'] - pd.Timedelta(hours=5)

    #sumar tp por día
    tp_daily = dataset[variable].resample(time='1D').sum(dim='time')

    #crear ventana de 5 días solo dentro del mismo mes
    def rolling_window_within_month(data, window):
        result = data.copy()
        result[:] = np.nan
        for month in range(1, 13):
            monthly_data = data.where(data['time.month'] == month, drop=True)
            rolled = monthly_data.rolling(time=window, min_periods=window).sum()
            result.loc[rolled.time] = rolled
        return result

    tp_5_day_sum = rolling_window_within_month(tp_daily, 5)

    #sacar de acumulado en 5 días
    Rx5day = tp_5_day_sum.resample(time='1M').max()

    #filtrar el dataset para un periodo de referencia
    dataset_filtered = dataset.sel(time=slice(f"{periodo_referencia[0]}-01-01", f"{periodo_referencia[1]}-12-31"))

    #calcular media y desviación estándar del periodo de referencia
    mean_tp = dataset_filtered[variable].groupby('time.month').mean(dim='time')
    std_tp = dataset_filtered[variable].groupby('time.month').std(dim='time')

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
