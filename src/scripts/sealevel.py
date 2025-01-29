import io
import pandas as pd
import requests

# URLs de las estaciones para datos mensuales (RLR y MET)
urls = {
    "Cartagena": "https://psmsl.org/data/obtaining/met.monthly.data/572.metdata",
    "Buenaventura": "https://psmsl.org/data/obtaining/met.monthly.data/456.metdata",
    "Riohacha": "https://psmsl.org/data/obtaining/met.monthly.data/714.metdata",
    "Tumaco": "https://psmsl.org/data/obtaining/met.monthly.data/639.metdata",
    "San Andres": "https://psmsl.org/data/obtaining/rlr.monthly.data/2116.rlrdata",
}

# Crear un diccionario para almacenar los datos limpios
dataframes = {}

for estacion, url in urls.items():
    response = requests.get(url)
    
    if response.status_code == 200:
        try:
            # Leer los datos descargados en un DataFrame
            data = pd.read_csv(io.StringIO(response.text), sep=';', header=None)

            # Seleccionar las primeras 4 columnas 
            data = data.iloc[:, :4]
            data.columns = ["Year", "Value_mm", "Flag1", "Flag2"]

            # Almacenar los datos sin procesar en el diccionario
            dataframes[estacion] = data
            
        
        except pd.errors.EmptyDataError:
            print(f"Error: El archivo de datos para {estacion} está vacío o tiene un formato incorrecto.")
        except pd.errors.ParserError:
            print(f"Error: No se pudo analizar el archivo de datos para {estacion}.")
        except Exception as e:
            print(f"Error al procesar los datos de {estacion}: {e}")
    else:
        print(f"No se pudo acceder a los datos de {estacion}.")



# Función para convertir año decimal a fecha
def decimal_year_to_date(decimal_year):
    year = int(decimal_year)
    remainder = decimal_year - year
    base_date = pd.Timestamp(year, 1, 1)
    days_in_year = (pd.Timestamp(year + 1, 1, 1) - base_date).days
    target_date = base_date + pd.Timedelta(seconds=round(remainder * days_in_year * 24 * 3600))
    return target_date

# Convertir año decimal a fecha para cada estación
for estacion, df in dataframes.items():
    df["Date"] = df["Year"].apply(decimal_year_to_date)



for estacion, df in dataframes.items():
    # Eliminar valores nulos, 9999 y -9999 en la columna "Value_mm"
    df = df.dropna(subset=["Value_mm"])
    df = df[(df["Value_mm"] != 9999) & (df["Value_mm"] != -99999)]

    # Calcular media y desviación estándar por mes (j)
    omega_ref = df.groupby(df["Date"].dt.month)["Value_mm"].transform("mean")
    sigma_ref = df.groupby(df["Date"].dt.month)["Value_mm"].transform("std")

    # Calcular anomalía estandarizada S_std(j,k)
    df["Anom_std"] = (df["Value_mm"] - omega_ref) / sigma_ref

    # Agregar columnas de media y desviación estándar mensuales
    df["omega_ref"] = omega_ref
    df["sigma_ref"] = sigma_ref
    

    # Actualizar el DataFrame con las nuevas columnas
    dataframes[estacion] = df[["Date", "Value_mm", "omega_ref", "sigma_ref", "Anom_std", "Flag1", "Flag2"]]
    


import matplotlib.pyplot as plt


plt.figure(figsize=(12, 6))

for estacion, df in dataframes.items():
    plt.plot(df["Date"], df["Value_mm"], label=estacion, alpha=0.7)

plt.xlabel("Fecha")
plt.ylabel("Nivel del mar (mm)")
plt.title("Nivel del mar en diferentes regiones")
plt.legend()
plt.grid(True)
plt.xticks(rotation=45)
plt.show()

    