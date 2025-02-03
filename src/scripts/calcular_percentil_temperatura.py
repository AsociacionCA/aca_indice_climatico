import xarray as xr
import pandas as pd
import os
import pdb


# Function to count days where daily_max > 90th percentile
def count_days_above_90th(x):
    quantile_90 = x.quantile(0.9)
    print(f"Quantile (90th): {quantile_90.values}")  # Debug
    return (x > quantile_90).sum(dim="time")

def calcular_percentiles(archivo_entrada, variable = 't2m'):    

    """

    Calcula los percentiles 10 y 90 de un archivo NetCDF por mes.

    Parámetros:
        archivo_entrada (str): Ruta del archivo NetCDF de entrada.

    Retorna:
        xr.Dataset: Dataset con los percentiles calculados.
    """
    dataset = xr.open_dataset(archivo_entrada)
    
    # Filter the data in between 1961 and 1990
    dataset_filtered = dataset.sel(time=slice('1961', '1990'))
    
    # Resample to daily frequency and calculate daily max and min
    daily_max = dataset_filtered['daily_max']
    daily_min = dataset_filtered['daily_min']

    # If the variable is temperature, convert from Kelvin to Celsius
    if variable == 't2m':
        daily_max -= 273.15
        daily_min -= 273.15
    
    # Combine daily max and min into a single dataset
    daily_data = xr.Dataset({
        'daily_max': daily_max,  
        'daily_min': daily_min   
    })
    
    # Calculate percentiles for each variable
    percentiles_max = daily_data['daily_max'].groupby("time.month").quantile([0.1, 0.9], dim="time")
    percentiles_min = daily_data['daily_min'].groupby("time.month").quantile([0.1, 0.9], dim="time")
    
    # Identify days exceeding the 90th percentile or below the 10th percentile
    
    # Maximum temperature
    ## Temperatures above the 90th percentile (These are from our interest)
    exceed_90_max = daily_data['daily_max'].groupby("time.month") > percentiles_max.sel(quantile=0.9)
    ## Temperatures below the 10th percentile
    below_10_max = daily_data['daily_max'].groupby("time.month") < percentiles_max.sel(quantile=0.1)
    
    # Minimum temperature
    ## Temperatures above the 90th percentile
    exceed_90_min = daily_data['daily_min'].groupby("time.month") > percentiles_min.sel(quantile=0.9)
    ## Temperatures below the 10th percentile (These are from our interest)
    below_10_min = daily_data['daily_min'].groupby("time.month") < percentiles_min.sel(quantile=0.1)

    exceed_90_max_y_m = exceed_90_max.groupby(["time.year", "time.month"]).mean(dim="time")
    mean_max = exceed_90_max_y_m.groupby("month").mean(dim="year")
    std_dev_max = exceed_90_max_y_m.groupby("month").std(dim="year")

    below_10_min_y_m = below_10_min.groupby(["time.year", "time.month"]).mean(dim="time")
    mean_min = below_10_min_y_m.groupby("month").mean(dim="year")
    std_dev_min = below_10_min_y_m.groupby("month").std(dim="year")


    
    # Combine all statistics into a single dataset
    estadisticas = xr.Dataset({
        'percentiles_max': percentiles_max,
        'percentiles_min': percentiles_min,
        'mean_max': mean_max,
        'mean_min': mean_min,
        'std_dev_max': std_dev_max,
        'std_dev_min': std_dev_min
    })
    
    return estadisticas

def guardar_percentiles(estadisticas, archivo_salida, guardar_csv=False):
    """
    Guarda los percentiles calculados en un archivo NetCDF y CSV.

    Parámetros:
        estadisticas (xr.Dataset): Dataset con los percentiles calculados.
        archivo_salida (str): Ruta del archivo NetCDF de salida.
    """
    estadisticas.to_netcdf(archivo_salida)
    
    if guardar_csv:
        subset = estadisticas.sel(latitude=5.9, longitude=-72.99, method = 'nearest')[['month', 'percentiles_max', 'percentiles_min', 'mean_max', 'mean_min', 'std_dev_max', 'std_dev_min']]
        df = subset.to_dataframe().reset_index()
        output_path = "../../data/processed/percentiles.csv"
        df.to_csv(output_path, index=False)

def main():
    ruta_datos = "../../data/processed"
    file = 'era5_daily_combined_tmp.nc'
    archivo_union = os.path.join(ruta_datos, file)
    archivo_salida = os.path.join(ruta_datos, "era5_temperatura_percentil.nc")

    try:
        estadisticas = calcular_percentiles(archivo_union)
        guardar_percentiles(estadisticas, archivo_salida, guardar_csv=True)
        print(f"Archivo de percentiles creado en: {archivo_salida}")
    except Exception as e:
        print(f"Error al calcular percentiles: {e}")

if __name__ == "__main__":
    main()