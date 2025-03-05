## Instalar las libbrerías necesatioas

Para instalar las librerías necesarias les recomendamos crear un ambiente con el siguiente código de python.

python3 -m venv myenv

Luego de crear el ambiente se debe instalar los paquetes con el siguiente comando 

pip install -r requirements.txt

## Descripción de la Estructura

### `data/`
Almacena todos los datos relacionados al proyecto. Separa los datos crudos (`raw`) de los procesados (`processed`).

### `notebooks/`
Contiene los Jupyter notebooks para explorar datos y documentar el flujo de trabajo.

### `src/`
Código fuente organizado en scripts y utilidades reutilizables.

## Diagrama de el proceso
![diagrama](https://github.com/user-attachments/assets/8e9416ea-6210-4be5-a6f1-3e51bf23282f)

Para realizar el proceso se usaron diferentes códigos.

#### Descargar datos
Para descargar los datos, se debe ejecutar el script `descargar_datos.py` que invoca la función `ecmwf_descarga.py` ubicado en la carpeta `src/scripts`. Este script descarga los datos de la página web del ECMWF y los almacena en la carpeta `data/raw/era5`.

#### Unir los datos
Para unir los datos descargados, se debe ejecutar el script `unir_archivos.py` ubicado en la carpeta `src/scripts`. Este script une los datos descargados y los almacena en la carpeta `data/processed`.

#### Calcular los percentiles
Para calcular los percentiles de los datos unidos, se debe ejecutar el script `calcular_percentil_temperatura.py`, `calcular_percentil_lluvia.py` y `calcular_percentil_viento.py` ubicado en la carpeta `src/scripts`. Este script calcula los percentiles de los datos unidos y los almacena en la carpeta `data/processed`.

#### Calcular anomalías
Para calcular las anomalías de los datos unidos, se debe ejecutar el script `calcular_anomalias_temperatura.py`, `calcular_anomalias_lluvia.py` y `calcular_anomalias_viento.py` ubicado en la carpeta `src/scripts`. Este script calcula las anomalías de los datos unidos y los almacena en la carpeta `data/processed`.

#### Creación de las gráficas
Luego del cálculo de las anomalías y de los percentiles se hacen las gráficas con las salidas usando el código `graficas.py`

#### Nivel del mar
Para el nivel del mar, ya que no se tenía información disponible se usó información de `psmsl.org` y se creó un código completo y exclusivo para su procesamiento llamado `sealevel.py`

#### Calculo de las anomalías por regiones
Para hacer un cálculo rápido de las anomalías por regiones donde se cambia el shapefile de entrada se creó el código `calcular_anomalias_regiones.py` el cual basado en los shapefiles proporcionados crea un recorte para extraér la información. Es importante resaltar que las áreas para realizar cálculos deben ser grandes, por lo menos a nivel departamental, ya que más pequeñas no funcionan.

### `articles/`
Espacio para redactar artículos científicos o reportes.

### Archivos raíz:
- **`README.md`**: Instrucciones iniciales para desarrolladores y usuarios.
- **`requirements.txt`**: Lista de dependencias del proyecto para instalación rápida.
- **`.gitignore`**: Para evitar incluir archivos innecesarios en el repositorio.
- **`LICENSE`**: Licencia que define cómo se puede usar el proyecto.
