import os
import xarray as xr
import pdb


    
def load_grid_data(file_path, year, month, variable):
    """Load grid data for a specific year, month, and variable."""
    grid_data = xr.open_dataset(file_path, engine='cfgrib')[variable].sel(time=f"{year}-{month:02}")
    if variable == 't2m':  # Convert temperature to Celsius
        grid_data -= 273.15
    return grid_data

def resample_to_daily(grid_data):
    """Resample hourly data to daily max and min."""
    daily_max = grid_data.resample(time='1D').max(dim='time')
    daily_min = grid_data.resample(time='1D').min(dim='time')
    return daily_max, daily_min

def load_percentiles(percentile_file, month):
    """Load precomputed percentiles for a specific month."""
    percentiles_data = xr.open_dataset(percentile_file)
    return percentiles_data.sel(month=month)

def compute_occurrences(daily_data, percentile_10, percentile_90):
    """Compute occurrences of values above the 90th percentile and below the 10th percentile."""
    count_above_90 = (daily_data > percentile_90).sum(dim='time')
    count_below_10 = (daily_data < percentile_10).sum(dim='time')
    return count_above_90, count_below_10

def calculate_anomalies(count_data, mean, std_dev):
    """Calculate normalized anomalies."""
    return (count_data - mean) / std_dev

def drop_unnecessary_coords(data_arrays, coord_name):
    """Drop an unnecessary coordinate from a list of DataArrays."""
    return [data_array.drop(coord_name) for data_array in data_arrays]

def create_anomalies_dataset(variables, attrs):
    """Create an xarray.Dataset for anomalies."""
    return xr.Dataset(variables, attrs=attrs)

def calcular_amomalias(archivo_percentiles, archivo_comparar, year, month, salida_anomalias):
    # File paths and configuration
    variable = 't2m'

    # Load and preprocess grid data
    grid_data = load_grid_data(archivo_comparar, year, month, variable)
    daily_max, daily_min = resample_to_daily(grid_data)

    # Load percentiles
    month_percentiles = load_percentiles(archivo_percentiles, month)
    percentile_10_max = month_percentiles['percentiles_max'].sel(quantile=0.1)
    percentile_90_max = month_percentiles['percentiles_max'].sel(quantile=0.9)
    percentile_10_min = month_percentiles['percentiles_min'].sel(quantile=0.1)
    percentile_90_min = month_percentiles['percentiles_min'].sel(quantile=0.9)

    # Compute occurrences
    count_above_90_max, count_below_10_max = compute_occurrences(daily_max, percentile_10_max, percentile_90_max)
    count_above_90_min, count_below_10_min = compute_occurrences(daily_min, percentile_10_min, percentile_90_min)

    # Calculate anomalies
    anomalies_above_max = calculate_anomalies(count_above_90_max, month_percentiles['mean_max'], month_percentiles['std_dev_max'])
    anomalies_below_max = calculate_anomalies(count_below_10_max, month_percentiles['mean_max'], month_percentiles['std_dev_max'])
    anomalies_above_min = calculate_anomalies(count_above_90_min, month_percentiles['mean_min'], month_percentiles['std_dev_min'])
    anomalies_below_min = calculate_anomalies(count_below_10_min, month_percentiles['mean_min'], month_percentiles['std_dev_min'])

    # Drop unnecessary coordinates
    variables_to_drop = [count_above_90_max, count_below_10_max, count_above_90_min, count_below_10_min,
                         anomalies_above_max, anomalies_below_max, anomalies_above_min, anomalies_below_min]
    variables_dropped = drop_unnecessary_coords(variables_to_drop, 'quantile')

    # Create anomalies dataset
    anomalies = create_anomalies_dataset({
        'count_above_90_max': variables_dropped[0],
        'count_below_10_max': variables_dropped[1],
        'count_above_90_min': variables_dropped[2],
        'count_below_10_min': variables_dropped[3],
        'anomalies_above_max': variables_dropped[4],
        'anomalies_below_max': variables_dropped[5],
        'anomalies_above_min': variables_dropped[6],
        'anomalies_below_min': variables_dropped[7]
    }, attrs={'description': 'Anomalies and counts of temperature extremes'})

    # Save the dataset
    anomalies.to_netcdf(salida_anomalias)

    # Clean the Dataset
    anomalies_cleaned = anomalies.where(~xr.ufuncs.isinf(anomalies), drop=False)  # Mask infinite values
    anomalies_cleaned = anomalies_cleaned.fillna(0)  # Replace NaNs with 0 (or use an alternative value)

    # Compute the Mean Again
    averages = anomalies_cleaned.mean(dim=['latitude', 'longitude'], keep_attrs=True)

    # Update Description
    averages.attrs['description'] = 'Averages of cleaned anomalies and counts of temperature extremes'

    pdb.set_trace()
    print(averages)

    # Return the average anomalies
    return 

if __name__ == "__main__":
    archivo_percentiles = "../../data/processed/era5_temperatura_percentil.nc"
    archivo_comparar = "../../data/raw/era5/era_small/era5_tmp_1961_1962.grib"
    month = 1
    year = 1961
    calcular_amomalias(archivo_percentiles = archivo_percentiles, archivo_comparar = archivo_comparar, year = year, month = month, salida_anomalias = "../../data/processed/anomalies.nc")
