

# EN CALCULAR ANOMALIAS:
import xarray as xr
import numpy as np
import os

def load_grid_data(file_path, year, month):# antes conocido load_grid_data
    """
    Procesa un archivo GRIB y aplica el tratamiento adecuado según las variables presentes.
    """
    try:
        # Abrir el archivo para inspeccionar las variables
        dataset = xr.open_dataset(file_path, engine='cfgrib')
        variables = list(dataset.variables.keys())

        # Determinar qué tipo de datos contiene el archivo
        if 't2m' in variables:
            return procesar_temperatura(dataset, 't2m', year, month)
        elif 'u10' in variables and 'v10' in variables:
            return procesar_viento(dataset, 'u10', 'v10', year, month)
        
        #Agregar las demás variables
        # elif "precipitacion" in variables:
        #      return procesar_precipitacion(dataset,'precipitacion', year, month)
        else:
            raise ValueError(f"No se encontraron variables relevantes en {file_path}")
    except Exception as e:
        print(f"Error al procesar el archivo {file_path}: {e}")
        return None
    

def procesar_temperatura(dataset, variable, year, month):
        """
        Procesa datos de temperatura en un archivo GRIB.
        """
        try:
            grid_data = dataset[variable].sel(time=f"{year}-{month:02}")
            grid_data -= 273.15  # Convertir de Kelvin a Celsius
            print("Temperatura procesada correctamente.")
            return grid_data
        
        except Exception as e:
            print(f"Error al procesar temperatura: {e}")
            return None
        
def procesar_precipitacion(dataset, variable, year, month):
        try:
                grid_data = dataset[variable].sel(time=f"{year}-{month:02}")
                #Hacer el respectivo proceso...
                print("Precipitacion procesada correctamente.")
                return grid_data
        
        except Exception as e:
            print(f"Error al procesar Precipitacion: {e}")
            return None
        
    

def procesar_viento(dataset, variable_u, variable_v, year, month):
    """
    Procesa datos de viento en un archivo GRIB.
    """
    try:
        u10 = dataset[variable_u].sel(time=f"{year}-{month:02}")
        v10 = dataset[variable_v].sel(time=f"{year}-{month:02}")
        ds_viento = calculos_viento(xr.Dataset({'u10': u10, 'v10': v10}))
        print("Viento procesado correctamente.")
        return ds_viento
    except Exception as e:
        print(f"Error al procesar viento: {e}")
        return None


def calculos_viento(ds):
    """
    Calcula velocidad y dirección del viento.
    """
    ds = manage_problemas(ds, 'u10', 0) 
    ds = manage_problemas(ds, 'v10', 0)
    ds['wind_speed'] = np.sqrt(ds['u10']**2 + ds['v10']**2)
    ds['wind_direction'] = np.arctan2(ds['v10'], ds['u10']) * (180 / np.pi)
    return ds


def manage_problemas(ds, variable, valor_nulo): # valor_nulo= Nan, 0 ...
    """
    Reemplaza valores nulos en una variable del dataset.
    """
    ds[variable] = ds[variable].fillna(valor_nulo) 
    return ds


def componente_viento(ds):
    # RECUERDA CHECKEAR EN QUÉ MEDICIONES SE ENCUENTRAN LOS VALORES

    
    #Calculamos el wind power (WP)
    p= 1.23  #constante de la densidad del aire (kg/m3)
    ds["wind_power"]= (p* (ds['wind_speed']**3))/2 
    

    # Calcular WPu(i, j)
    wp_mean = ds['wind_power'].groupby('time.dayofyear').mean('time')
    wp_std = ds['wind_power'].groupby('time.dayofyear').std('time')

    # Umbral WPu(i, j) = media + 1.28 * sigma
    ds['wpu'] = wp_mean + 1.28 * wp_std

    WP_90=ds['wind_power'].groupby('time.month').reduce(np.percentile, q=90)
    ds['exceedance'] = (ds['wind_power'] > ds['wp90']).astype(int)

    # Calcular la frecuencia mensual de días excedidos (en porcentaje)
    ds['exceedance_freq'] = (
        ds['exceedance']
        .groupby('time.month')
        .mean('time') * 100
    )

    ds['wp90_frequency'] = ds.groupby('time.month')['exceedance'].transform('mean')

    mean_ref = ds['wp90_frequency'].groupby('time.month').transform('mean')
    std_ref = ds['wp90_frequency'].groupby('time.month').transform('std')

    # Estandarizar WP90 para calcular la componente del viento (WC)
    ds['wind_component'] = (ds['wp90_frequency'] - mean_ref) / std_ref
    ds.to_netcdf("../../data/processed/compressed_wind_analysis.nc", encoding={'wind_power': {'zlib': True, 'complevel': 5}})
    return  ds


def get_netcdf(ds,description, dicc_descripcion):
    ds.attrs['description'] = "Análisis del viento basado en WP90 y estandarización"
    ds.attrs['created'] = "2025-01-13"
    ds.to_netcdf("compressed_wind_analysis.nc", encoding={'wind_power': {'zlib': True, 'complevel': 5}})
    # Agregar atributos a las variables clave
    ds['wind_power'].attrs['units'] = "W/m²"
    ds['wp90_frequency'].attrs['description'] = "Frecuencia de WP90 por mes"

def prueba_local():
    """
    Esto lo puedes cambiar:
    """
    ruta_datos_grib = "G:/Unidades compartidas/Indice Climatico Actuarial/2. Datos/era5/otros"
    file = 'era5_all_var_81_82.grib'
    archivo = os.path.join(ruta_datos_grib, file)

    # Directorio temporal para los índices
    temp_dir = "D:\Indicador_climatico\Demas"
    os.makedirs(temp_dir, exist_ok=True)
    
    # u10: Componente zonal (este-oeste) del viento.
    # v10: Componente meridional (norte-sur) del viento.
    try:
        print(f"Archivo a cargar: {archivo}")
        ds = xr.open_dataset(
            archivo,
            engine='cfgrib',
            backend_kwargs={
                'indexpath': os.path.join(temp_dir, 'temp.idx'),
            }
        )

        print(f"Variables disponibles: {list(ds.variables.keys())}")
    except Exception as e:
        print(f"Error al abrir el archivo: {e}")

    print(f"registro :{ds.isel(time=0)}")  # Primer registro temporal


if __name__ == "__main__":
    prueba_local()
    #main()