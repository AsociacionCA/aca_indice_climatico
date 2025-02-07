import pandas as pd 
import matplotlib.pyplot as plt

def plot_wind_anomalies(df):
    """
    Genera un gráfico de la media móvil de 5 años de la variable 'anomalies_above'.

    Considero importante manejar los mismos nombres de variable que en el texto!!

    Parámetros:
    df (DataFrame): DataFrame con las columnas 'year', 'month' y 'anomalies_above'.
    """

    df['date'] = pd.to_datetime(df[['year', 'month']].assign(day=1))

    df = df.sort_values(by='date')

    df['5yr_moving_avg'] = df['anomalies_above'].rolling(window=60, center=True).mean()

    plt.figure(figsize=(15, 6)) #tamaño de la grafica

    plt.bar(df['date'], df['anomalies_above'], color='lightblue', alpha=1, label="Standardized Anomalies", width=40)
    plt.plot(df['date'], df['5yr_moving_avg'], color='black', linewidth=2, label="5-Year Moving Average")

    plt.xlabel("Year")
    plt.ylabel("Standardized Anomalies")
    plt.legend()
    plt.grid(False)
    plt.show()

if __name__ == "__main__":
    anomalies_wind = "../../data/processed/anomalies_wind_combined.csv"

    plot_wind_anomalies(anomalies_wind)


