import xarray as xr
import pdb
import os

def unir_archivos_grib(lista_archivos, salida="era5_2m_temperature_union.nc"):
    """
    unir varios archivos GRIB en un único archivo netCDF.

    Parámetros:
        - lista_archivos (list): Lista de rutas a los archivos GRIB.
        - salida (str): Ruta del archivo GRIB combinado.

    Retorna:
        str: Ruta del archivo combinado netcdf.
    """
    if not lista_archivos:
        raise ValueError("La lista de archivos está vacía.")

    try:
        datasets = [xr.open_dataset(archivo, engine="cfgrib") for archivo in lista_archivos]
        dataset_union = xr.merge(datasets)
        dataset_union.to_netcdf(salida) 
        return salida
 
    except Exception as e:
        raise RuntimeError(f"Error al unir los archivos GRIB: {e}")
 
    finally:
        for ds in datasets:
            ds.close()

if __name__ == "__main__":
    #### tmp
    ruta_datos = "../../data/raw/era5"
    ruta_datos_salida = "../../data/raw/processed"
    files = os.listdir(ruta_datos)

    # Filter files that start with era5_2m_temperature
    files = [file for file in files if file.startswith("era5_tmp_")]

    # Files that ends with .grib
    files = [file for file in files if file.endswith(".grib")]

    # Sort files
    files = sorted(files)

    # Create a list with the full path of the files
    archivos = [os.path.join(ruta_datos, file) for file in files]

    archivo_union = unir_archivos_grib(archivos, salida= os.path.join(ruta_datos_salida, "era5_tmp_union_col.nc"))
    
    ########################## rain
    ruta_datos = "../../data/raw/era5"
    ruta_datos_salida = "../../data/raw/processed"
    files = os.listdir(ruta_datos)

    # Filter files that start with era5_2m_temperature
    files = [file for file in files if file.startswith("era5_rain_")]

    # Files that ends with .grib
    files = [file for file in files if file.endswith(".grib")]

    # Sort files
    files = sorted(files)

    # Create a list with the full path of the files
    archivos = [os.path.join(ruta_datos, file) for file in files]

    archivo_union = unir_archivos_grib(archivos, salida= os.path.join(ruta_datos_salida, "era5_rain_union_col.nc"))

    ########################## wind
    ruta_datos = "../../data/raw/era5"
    ruta_datos_salida = "../../data/raw/processed"
    files = os.listdir(ruta_datos)

    # Filter files that start with era5_2m_temperature
    files = [file for file in files if file.startswith("era5_wind_")]

    # Files that ends with .grib
    files = [file for file in files if file.endswith(".grib")]

    # Sort files
    files = sorted(files)

    # Create a list with the full path of the files
    archivos = [os.path.join(ruta_datos, file) for file in files]

    archivo_union = unir_archivos_grib(archivos, salida= os.path.join(ruta_datos_salida, "era5_wind_union_col.nc"))
    
    # Print output
    print('Proceso finalizado')
    
