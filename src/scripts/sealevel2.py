import io
import pandas as pd
import requests
import matplotlib.pyplot as plt

def descargar_datos(urls):
    """
    Función para descargar y procesar datos de series temporales desde URLs especificadas
    
    Args:
        urls (dict): Diccionario con nombres de estaciones y URLs de descarga
    
    Returns:
        dict: Diccionario con DataFrames de pandas con los datos procesados
    """
    dataframes = {}
    
    for estacion, url in urls.items():
        try:
            # Descargar datos
            response = requests.get(url)
            response.raise_for_status()  # Lanza error para respuestas no exitosas
            
            # Procesar datos crudos
            data = pd.read_csv(io.StringIO(response.text), sep=';', header=None)
            data = data.iloc[:, :4]
            data.columns = ["Year", "Value_mm", "Flag1", "Flag2"]
            
            dataframes[estacion] = data
            
        except requests.exceptions.RequestException as e:
            print(f"Error de conexión en {estacion}: {str(e)}")
        except pd.errors.EmptyDataError:
            print(f"Error: Archivo vacío en {estacion}")
        except pd.errors.ParserError:
            print(f"Error de formato en {estacion}")
        except Exception as e:
            print(f"Error inesperado en {estacion}: {str(e)}")
    
    return dataframes

# URLs de descarga
urls_estaciones = {
    "Cartagena": "https://psmsl.org/data/obtaining/met.monthly.data/572.metdata",
    "Buenaventura": "https://psmsl.org/data/obtaining/met.monthly.data/456.metdata",
    "Riohacha": "https://psmsl.org/data/obtaining/met.monthly.data/714.metdata",
    "Tumaco": "https://psmsl.org/data/obtaining/met.monthly.data/639.metdata",
    "San Andres": "https://psmsl.org/data/obtaining/rlr.monthly.data/2116.rlrdata",
}

# Descargar y procesar datos
dataframes = descargar_datos(urls_estaciones)

# Función para conversión de fecha
def decimal_year_to_date(decimal_year):
    year = int(decimal_year)
    remainder = decimal_year - year
    base_date = pd.Timestamp(year, 1, 1)
    days_in_year = (pd.Timestamp(year + 1, 1, 1) - base_date).days
    return base_date + pd.Timedelta(seconds=round(remainder * days_in_year * 24 * 3600))

# Procesamiento adicional de datos
for estacion, df in dataframes.items():
    # Convertir fecha
    df["Date"] = df["Year"].apply(decimal_year_to_date)
    
    # Limpieza de datos
    df = df.dropna(subset=["Value_mm"])
    df = df[(df["Value_mm"] != 9999) & (df["Value_mm"] != -99999)]
    
    # Cálculos estadísticos
    omega_ref = df.groupby(df["Date"].dt.month)["Value_mm"].transform("mean")
    sigma_ref = df.groupby(df["Date"].dt.month)["Value_mm"].transform("std")
    
    df["Anom_std"] = (df["Value_mm"] - omega_ref) / sigma_ref
    df["omega_ref"] = omega_ref
    df["sigma_ref"] = sigma_ref
    
    dataframes[estacion] = df[["Date", "Value_mm", "omega_ref", "sigma_ref", "Anom_std", "Flag1", "Flag2"]]

# Visualización
plt.figure(figsize=(12, 6))
for estacion, df in dataframes.items():
    plt.plot(df["Date"], df["Value_mm"], label=estacion, alpha=0.7)

plt.xlabel("Fecha")
plt.ylabel("Nivel del mar (mm)")
plt.title("Nivel del mar en diferentes regiones")
plt.legend()
plt.grid(True)
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()



for estacion, df in dataframes.items():
    print(f"\nDatos de {estacion}:")
    print(df.head())  # Mostrar las primeras 5 filas de cada DataFrame



from scipy.stats import shapiro, kstest, linregress
import pymannkendall as mk
import numpy as np

# Diccionario para almacenar resultados de normalidad
normalidad = {}

# Primera pasada: verificar normalidad para todas las estaciones
for estacion, df in dataframes.items():
    data = df["Value_mm"].dropna()
    
    # Realizar prueba de normalidad
    if len(data) <= 50:
        _, p = shapiro(data)
    else:
        _, p = kstest(data, 'norm', args=(data.mean(), data.std()))
    
    normalidad[estacion] = p > 0.05  # Almacenar resultado

# Segunda pasada: análisis de tendencia
for estacion, df in dataframes.items():
    df_clean = df.dropna(subset=["Value_mm"])
    data = df_clean["Value_mm"]
    
    print(f"📍 Estación: {estacion}")
    
    # Aplicar Mann-Kendall siempre como método principal
    trend_test = mk.original_test(data)
    print(f" Método Mann-Kendall:")
    print(f"Tendencia: {trend_test.trend}")
    print(f"P-valor: {trend_test.p:.4f}")
    print(f"Tau de Kendall: {trend_test.Tau:.4f}")
    
    # Si los datos son normales, añadir regresión lineal
    if normalidad[estacion]:
        x = np.arange(len(data))
        slope, _, r_value, p_value, _ = linregress(x, data)
        print("\n Regresión Lineal (Datos normales):")
        print(f"Pendiente: {slope:.4f} mm/año")
        print(f"P-valor: {p_value:.4f}")
        print(f"R²: {r_value**2:.4f}")
    else:
        print("\n Solo método no paramétrico (datos no normales)")
    
    print("-" * 50)


dataframes_filtrados = dataframes.copy()

# Filtros temporales para Cartagena y Buenaventura
if 'Cartagena' in dataframes_filtrados:
    df_cartagena = dataframes_filtrados['Cartagena']
    mask = (df_cartagena['Date'].dt.year >= 1973) & (df_cartagena['Date'].dt.year <= 1992)
    dataframes_filtrados['Cartagena'] = df_cartagena[mask]

if 'Buenaventura' in dataframes_filtrados:
    df_buenaventura = dataframes_filtrados['Buenaventura']
    mask = (df_buenaventura['Date'].dt.year >= 1975) & (df_buenaventura['Date'].dt.year <= 1992)
    dataframes_filtrados['Buenaventura'] = df_buenaventura[mask]

# Eliminar únicamente Tumaco
dataframes_filtrados.pop('Tumaco', None)


#graficar 
plt.figure(figsize=(14, 7))
colores = {
    'Cartagena': '#1f77b4', 
    'Buenaventura': '#ff7f0e',
    'Riohacha': '#2ca02c',
    'San Andres': '#d62728'
}

for estacion, df in dataframes_filtrados.items():
    # Gráfico principal
    plt.plot(df['Date'], df['Value_mm'], 
             color=colores.get(estacion, '#7f7f7f'),
             linewidth=1.5, 
             alpha=0.7,
             label=f'{estacion} (Datos originales)')
    
    # Tendencia polinómica
    if len(df) > 2:
        x = df['Date'].map(pd.Timestamp.toordinal)
        coef = np.polyfit(x, df['Value_mm'], 1)
        tendencia = np.poly1d(coef)(x)
        plt.plot(df['Date'], tendencia, 
                 color=colores.get(estacion, '#7f7f7f'), 
                 linestyle='--',
                 linewidth=2,
                 label=f'Tendencia {estacion}')

plt.title('Análisis Comparativo del Nivel del Mar (1973-1995)', fontsize=14)
plt.xlabel('Año', fontsize=12)
plt.ylabel('Altura (mm)', fontsize=12)
plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
plt.grid(alpha=0.3)
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# Celda 13 modificada: Análisis extendido
print("\n📊 Análisis Detallado por Estación:")
for estacion, df in dataframes_filtrados.items():
    print(f"\n🔍 {estacion} ({df['Date'].min().year}-{df['Date'].max().year})")
    
    # Estadísticas descriptivas
    desc = df['Value_mm'].describe(percentiles=[0.25, 0.75])
    print(f"\nEstadísticas descriptivas:\n{desc.round(2)}")
    
    # Análisis de tendencia
    mk_result = mk.original_test(df['Value_mm'])
    print(f"\nPrueba Mann-Kendall:")
    print(f"- Tendencia: {mk_result.trend}")
    print(f"- Tau: {mk_result.Tau:.3f}")
    print(f"- p-valor: {mk_result.p:.4f}")
    
    # Cambio absoluto estimado
    años = df['Date'].dt.year.max() - df['Date'].dt.year.min()
    pendiente = mk_result.slope * 365  # mm/año
    cambio_total = pendiente * años
    print(f"\nCambio estimado ({años} años): {cambio_total:.1f} mm")

    # Celda 15: Análisis de normalidad en residuos de regresión
from statsmodels.graphics.gofplots import qqplot
from scipy.stats import normaltest

print("\n🔍 Validación de supuestos para regresión lineal:")

for estacion, df in dataframes_filtrados.items():
    print(f"\n📍 {estacion}")
    
    # 1. Ajustar modelo lineal
    x = df['Date'].map(pd.Timestamp.toordinal).values
    y = df['Value_mm'].values
    slope, intercept, r_value, p_value, _ = linregress(x, y)
    
    # 2. Calcular residuos
    residuos = y - (slope*x + intercept)
    
    # 3. Prueba de normalidad en residuos (D'Agostino-Pearson)
    stat, p = normaltest(residuos)
    normalidad = p > 0.05
    
    # 4. Resultados
    print(f"- Pendiente: {slope*365:.2f} mm/año (p = {p_value:.4f})")
    print(f"- Normalidad residuos: {'Sí' if normalidad else 'No'} (p = {p:.4f})")
    
    # 5. Gráfico Q-Q de residuos
    plt.figure(figsize=(8,4))
    qqplot(residuos, line='s', ax=plt.gca())
    plt.title(f'Q-Q Plot Residuos: {estacion}')
    plt.show()


