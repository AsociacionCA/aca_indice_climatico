
import xarray as xr
import numpy as np
import pandas as pd
import os
import pdb
import geopandas as gpd
from calcular_anomalias_temperatura import load_percentiles, load_grid_data, calculate_anomalies


def compute_occurrences(daily_data, percentile):
    # Calcular la cantidad de días que superan el percentil
    count_above = (daily_data > percentile).mean(dim='time')
    return count_above

def calculos_componente_viento(archivo_percentiles, archivo_comparar, year, month, salida_anomalias, shapefile_path=None, save_netcdf=False):
    try:
        variable="wind_speed"
        ds = load_grid_data(archivo_comparar, year,month,variable, shapefile_path)
    except Exception as e:
        print(f"Error al abri archivo: {e}")

    p= 1.23  #constante de la densidad del aire (kg/m3)
    ds["wind_power"] = (p* (ds["wind_speed"]**3))/2 
        
    percentiles=load_percentiles(archivo_percentiles, month, shapefile_path)
    count_above = compute_occurrences(ds['wind_power'], percentiles['percentil_90'])

    # Calcular anomalias
    anomalies = calculate_anomalies(count_above, percentiles['mean_exceeding'], percentiles['std_exceeding'])

    # Crear un nuevo Dataset con los resultados
    anomalies = xr.Dataset({
        'count_above': count_above,
        'anomalies_above': anomalies
    }, attrs={'description': 'Anomalies of wind speed component'})
    
    # Guardar el Dataset en un archivo NetCDF
    if save_netcdf:
        anomalies.to_netcdf(f"../../data/processed/anomalies_wind_{year}_{month}.nc")
    return anomalies.mean(dim=['latitude', 'longitude'], keep_attrs=True)
   

def procesar_anomalias_viento(archivo_percentiles, archivo_comparar_location, output_csv_path, shapefile_path):
    # List all files in the directory
    files = os.listdir(archivo_comparar_location)

    # Initialize an empty list to store monthly datasets
    all_anomalies = []

    # Loop through each year and month
    for year in range(1961, 2025):
        print(f"Processing year {year}...")
        
        # Find all files containing the year
        archivo_comparar = [file for file in files if str(year) in file]
        
        # Filter for GRIB files
        archivo_comparar = [file for file in archivo_comparar if file.endswith(".grib")]
        if not archivo_comparar:
            print(f"Error processing year {year}: No GRIB files found")
            continue
        
        # Check if the file is a wind Hola a file
        archivo_comparar = [file for file in archivo_comparar if "wind" in file]
        if not archivo_comparar:
            print(f"Error processing year {year}: No wind files found")
            continue
        
        if len(archivo_comparar) != 1:
            print(f"Error processing year {year}: More than one wind file found")
            continue
        else:
            archivo_comparar = archivo_comparar[0]

        archivo_comparar = os.path.join(archivo_comparar_location, archivo_comparar)

        for month in range(1, 13):
            try:
                print(f"Processing year {year}, month {month}...")
                ds_month = calculos_componente_viento(
                    archivo_percentiles=archivo_percentiles,
                    archivo_comparar=archivo_comparar,
                    year=year,
                    month=month,
                    salida_anomalias=f"../../data/processed/anomalies_wind_{year}_{month}.nc",
                    shapefile_path=shapefile_path
                )
                ds_month = ds_month.assign_coords(year=year)
                all_anomalies.append(ds_month)
            except Exception as e:
                print(f"Error processing year {year}, month {month}: {e}")
                continue

    # Combine all monthly datasets into one
    combined_anomalies = xr.concat(all_anomalies, dim='time')

    # Convert the xarray.Dataset to a pandas.DataFrame
    anomalies_df = combined_anomalies.to_dataframe().reset_index()

    # Save the DataFrame to a CSV file
    anomalies_df.to_csv(output_csv_path, index=False)
    print(f"Anomalies saved to {output_csv_path}")

if __name__ == "__main__":
    archivo_percentiles = "../../data/processed/era5_wind_percentil.nc"
    archivo_comparar_location = "../../data/raw/era5/"
    output_csv_path = "../../data/processed/anomalies_wind_combined.csv"
    shapefile_path = "../../data/shapefiles/colombia_4326.shp"

    procesar_anomalias_viento(archivo_percentiles, archivo_comparar_location, output_csv_path, shapefile_path)
