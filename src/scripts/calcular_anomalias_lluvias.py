import os
import xarray as xr

def load_grid_data(file_path, year, month, variable):
    """Load grid data for a specific year, month, and variable."""
    #{month:02}: Ensures the month is zero-padded to 2 digits (e.g., 01 for January, 12 for December).
    grid_data = xr.open_dataset(file_path, engine='cfgrib')[variable].sel(time=f"{year}-{month:02}")
    return grid_data    

def resample_to_daily(grid_data):
    """Resample hourly data to daily max and min."""
    daily_tp = grid_data.resample(time='1D').sum(dim='time')
    return daily_tp

def load_maximos(maximos_file, month):
    """Load precomputed maximos for a specific month."""
    maximos_data = xr.open_dataset(maximos_file)
    return maximos_data.sel(month=month)

def calculate_anomalies(rx5day, mean, std_dev):
    """Calculate normalized anomalies."""
    return (rx5day - mean) / std_dev

def drop_unnecessary_coords(data_arrays, coord_name):
    """Drop an unnecessary coordinate from a list of DataArrays."""
    return [data_array.drop(coord_name) for data_array in data_arrays]

def create_anomalies_dataset(variables, attrs):
    """Create an xarray.Dataset for anomalies."""
    return xr.Dataset(variables, attrs=attrs)

def main():
    # File paths and configuration
    ruta_datos_grib = "../../data/raw/era5/"
    file = 'era5_rain_union.nc'
    archivo_comparar = os.path.join(ruta_datos_grib, file)
    ruta_datos_maximos = "../../data/processed"
    datos_precipitacion_archivo = "file2.nc"
    archivo_maximos = os.path.join(ruta_datos_maximos, datos_precipitacion_archivo)
    year, month, variable = 1981, 1, 'tp'

    # Load and preprocess grid data
    grid_data = load_grid_data(archivo_comparar, year, month, variable)
    monthly_tp = resample_to_daily(grid_data)

    # Load maximos
    month_maximos = load_maximos(archivo_maximos, month)
    
    # Calculate anomalies
    anomalies_p = calculate_anomalies(monthly_tp, month_maximos['mean_tp'], month_maximos['std_tp'])
 
    #create anomalies dataset
    anomalies = create_anomalies_dataset({
        'anomalies_p': anomalies_p[0]
    }, attrs={'description': 'Anomalies of precipitation extremes'})

    # Save the dataset
    anomalies.to_netcdf("../../data/processed/era5_precipitacion_anomalias2.nc")

    # Output results
    print(anomalies)

if __name__ == "__main__":
    main()