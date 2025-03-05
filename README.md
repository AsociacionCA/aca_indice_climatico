# Proyecto de Análisis de Datos Meteorológicos

## Instalación de Librerías

Para instalar las librerías necesarias, se recomienda crear un ambiente virtual con el siguiente comando:

```bash
python3 -m venv myenv
```

Luego, se deben instalar los paquetes requeridos ejecutando:

```bash
pip install -r requirements.txt
```

---

## Estructura del Proyecto

```
├── data/
│   ├── raw/         # Datos crudos descargados
│   ├── processed/   # Datos procesados
│
├── notebooks/       # Jupyter notebooks para exploración de datos
│
├── src/             # Código fuente y scripts
│   ├── scripts/     # Scripts de procesamiento de datos
│
├── articles/        # Documentación y artículos científicos
│
├── README.md        # Instrucciones del proyecto
├── requirements.txt # Dependencias del proyecto
├── .gitignore       # Archivos a excluir en el repositorio
├── LICENSE          # Licencia del proyecto
```

---

## Flujo de Trabajo

### 1. Descargar Datos
Para descargar los datos, ejecutar:

```bash
python src/scripts/descargar_datos.py
```

Este script invoca la función `ecmwf_descarga.py` y almacena los datos en `data/raw/era5`.

### 2. Unir Datos
Para unir los archivos descargados, ejecutar:

```bash
python src/scripts/unir_archivos.py
```

Los datos combinados se guardan en `data/processed`.

### 3. Calcular Percentiles
Para calcular los percentiles de temperatura, lluvia y viento, ejecutar:

```bash
python src/scripts/calcular_percentil_temperatura.py
python src/scripts/calcular_percentil_lluvia.py
python src/scripts/calcular_percentil_viento.py
```

Los resultados se almacenan en `data/processed`.

### 4. Calcular Anomalías
Para calcular anomalías en temperatura, lluvia y viento, ejecutar:

```bash
python src/scripts/calcular_anomalias_temperatura.py
python src/scripts/calcular_anomalias_lluvia.py
python src/scripts/calcular_anomalias_viento.py
```

Los resultados se guardan en `data/processed`.

### 5. Generar Gráficas
Para visualizar los resultados, ejecutar:

```bash
python src/scripts/graficas.py
```

### 6. Análisis del Nivel del Mar
Dado que no se tenía información disponible, se utilizó `psmsl.org` y se creó un script exclusivo para su procesamiento:

```bash
python src/scripts/sealevel.py
```

### 7. Cálculo de Anomalías por Regiones
Para calcular anomalías por regiones usando shapefiles, ejecutar:

```bash
python src/scripts/calcular_anomalias_regiones.py
```

**Nota:** Las áreas analizadas deben ser grandes (mínimo a nivel departamental), ya que áreas más pequeñas pueden no ser representativas.

---

## Diagrama del Proceso

![Diagrama del proceso](https://github.com/user-attachments/assets/8e9416ea-6210-4be5-a6f1-3e51bf23282f)

---

## Licencia
Este proyecto está bajo la licencia especificada en el archivo `LICENSE`.
