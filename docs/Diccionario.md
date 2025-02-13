
# Índice
- [Resumen de Fuentes de Datos](#resumen-de-fuentes-de-datos)
- [IDEAM](#ideam)
- [IGAC](#igac)
- [WorldClim](#worldclim)
- [GFS](#gfs)
- [Meteoblue](#meteoblue)
- [Google Earth Engine](#google-earth-engine)
- [Power NASA](#power-nasa)
- [HUMBOLDT](#humboldt)
- [Invemar](#invemar)
- [Corporaciones autónomas](#corporaciones-autónomas)
- [IRI](#iri)
- [CHIRPS](#chirps)
- [CAR](#car)
- [CPTEC](#cptec)
- [ECMWF](#ecmwf)
- [DNP](#dnp)
- [SIAC](#siac)
- [ANLA](#anla)
- [INMET](#inmet)
- [UNGRD](#ungrd)
- [UNESCO](#unesco)

# Resumen de Fuentes de Datos

| Fuente                  | Información disponible                                                                                  |
|-------------------------|---------------------------------------------------------------------------------------------------------|
| **IDEAM**              | Sí hay información. La mayoría es reciente y puntual de estaciones.                                     |
| **IGAC**               | No tienen información relevante para este estudio.                                                     |
| **WorldClim**          | Sí tienen información desde 1970.                                                                      |
| **GFS**                | Sí tienen información desde 2015.                                                                      |
| **Meteoblue**          | Sí tienen información, pero requiere pago.                                                             |
| **Google Earth Engine**| Sí tienen información, con un límite de cuota. Considerado como paso futuro. Consultan BD como ERA5.    |
| **NASA**               | Tiene datos desde 2015.                                                                                |
| **Humboldt**           | Tienen estudios de diferentes zonas.                                                                   |
| **Invemar**            | Poseen información del nivel del mar. Hay otras fuentes y hemos hecho requerimientos adicionales.       |
| **Corporaciones**      | Principalmente estudios para diferentes regiones.                                                      |
| **IRI**                | Sí tienen información desde 2015.                                                                      |
| **CHIRPS**             | Sí tienen información de lluvias desde 1981.                                                           |
| **CAR**                | Es una fuente de estudios, pero no tienen información directa.                                         |
| **CPTEC**              | Tienen información, pero solo desde 1981.                                                              |
| **ECMWF**              | Sí tienen información de precipitación desde 1950.                                                     |
| **DNP**                | Poseen información de lluvias.                                                                         |
| **SIAC**               | Poseen información de lluvias.                                                                         |
| **ANLA**               | Poseen información de lluvias.                                                                         |
| **InMet**              | Poseen información desde 1960, pero solo en Brasil, no en Colombia.                                    |
| **Power NASA**         | Sí tienen información. Tienen un máximo de 365 datos por solicitud.                                    |
| **UNGRD**              | No tienen información.                                                                                 |
| **UNESCO**             | Posee la información de los niveles del océano sin restricción de uso.                                 |


## IDEAM
### **Fuente de Datos:  [Instituto de Hidrología, Meteorología y Estudios Ambientales (IDEAM)]**
[Atención al Ciudadano](http://dhime.ideam.gov.co/atencionciudadano/)
[Galería de Mapas](https://www.ideam.gov.co/web/guest/galeria-de-mapas)
[Resultados de Cambio Climático](http://www.cambioclimatico.gov.co/resultados#:~:text=El%20%22An%C3%A1lisis%20de%20Vulnerabilidad%20y,de%20equipo%20con%20el%20INVEMAR)
[Datos Abiertos IDEAM](http://www.pronosticosyalertas.gov.co/datos-abiertos-ideam)
[3ra Comunicación Cambio Climático](http://www.cambioclimatico.gov.co/3ra-comunicacion-cambio-climatico)

## **1. Disponibilidad y Acceso**

- **¿Tiene información de lluvias (precipitación), fuertes lluvias?**  
  Sí.

- **¿Tiene información de vientos, velocidad del viento, ráfagas de viento?**  
  Si.

- **¿Tiene información de temperaturas máximas, mínimas, promedio, datos de temperatura horaria?**  
  Sí.
  
- **¿Tiene acceso libre?**  
  Si.
  
- **¿Desde cuándo hay datos?**  
  Dependiendo de la variable y estación a consultar, esta puede variar. Se tienen datos desde el 1995 hasta el 2023.

- **¿Necesitamos un permiso especial?**  
   No es necesario.

- **¿Cómo se puede acceder a los datos?**
  A través de la página http://dhime.ideam.gov.co/atencionciudadano/

- **¿Podemos crear una automatización para la descarga de los datos?**  
  Si.

- **¿Cuál es la frecuencia de actualización de los datos?**  
Diaria.

- **¿En qué formato están disponibles los datos?**  
  Los datos están disponibles en formatos como CSV o XLSX.

- **¿Existen restricciones geográficas en el acceso?**  
  No.
---

## **2. Calidad y Estandarización**

- **¿Qué nivel de resolución espacial tienen los datos?**  
 Los datos tienen resolución por cada estación disponible (punto en el mapa).

- **¿Qué nivel de resolución temporal tienen los datos?**  
  Dependiendo de la variable de selección, puede ser diaria, mensual o anual.


- **¿Los datos han sido sometidos a algún proceso de limpieza o validación?**  
  Sí.

- **¿Incluyen metadatos detallados y consistentes?**  
  Si.

---

## **3. Contenido de los Datos**

- **¿Qué variables están disponibles (temperatura, precipitación, viento, etc.)?**  
Temperatura, precipitación, radiación, variables relacionadas con el viento etc.

- **¿Los datos incluyen mediciones históricas, simulaciones o ambos?**  
  Incluye mediciones históricas.

- **¿Existen variables derivadas (índices climáticos, anomalías, etc.)?**  
  No.

---

## **4. Uso y Licencia**

- **¿Qué tipo de licencia tienen los datos (libre, restringida, comercial)?**  
  Libre.

- **¿Existe un límite en la cantidad de datos que se pueden descargar?**  
  No.

- **¿Se requiere atribuir la fuente en caso de uso?**  
   Sí, se debe citar al IDEAM como fuente.
  
- **¿Es posible redistribuir los datos o integrarlos en nuevos productos?**  
  Si es posible.

---

## **5. Automatización y Escalabilidad**

- **¿Los datos están disponibles a través de APIs?**  
  Sí.

- **¿Existen restricciones en la velocidad o volumen de solicitudes para la descarga automatizada?**  
  No.

- **¿Qué herramientas o lenguajes son compatibles con el acceso a los datos (Python, R, etc.)?**  
  Python, R, entre otros.

--6. Contexto y la Institución**

- **¿Quién es la entidad responsable de los datos?**  
  IDEAM.

- **¿Cuál es la reputación de la fuente en términos de calidad y fiabilidad?**  
  Muy confiable.

- **¿Hay documentación técnica detallada para entender los datos?**  
Sí, disponible en la misma página y atenciones al ciudadano puestas por el mismo IDEAM.
---

## **7. Usabilidad**

- **¿Es necesario transformar los datos para adaptarlos a los objetivos del proyecto?**  
  Si, dependiendo de la base a usar en el proyecto.
  
- **¿Qué tan fácil es integrar los datos con otras fuentes?**  
  Fácil debido a su formato puntual.
  
- **¿Qué soporte técnico está disponible para resolver problemas?**  
  Soporte telefónico, por correo o en oficinas del IDEAM.

## IGAC
### **Fuente de Datos:  [Instituto Geográfico Agustin Codazzi - IGAC]**
[Geoportal IGAC](https://geoportal.igac.gov.co/contenido/tematicos)

## **1. Disponibilidad y Acceso**

- **¿Tiene información de lluvias (precipitación), fuertes lluvias?**  
  No.

- **¿Tiene información de vientos, velocidad del viento, ráfagas de viento?**  
  No.

- **¿Tiene información de temperaturas máximas, mínimas, promedio, datos de temperatura horaria?**  
  No.
  
- **¿Tiene acceso libre?**  
  Si.
  
- **¿Necesitamos un permiso especial?**  
   Es necesario registrarse.

- **¿Cómo se puede acceder a los datos?**
  A través de la página
  
- **¿Podemos crear una automatización para la descarga de los datos?**  
  Si, pero no lo considero necesario.

- **¿Cuál es la frecuencia de actualización de los datos?**  
  No se actualzan.

- **¿En qué formato están disponibles los datos?**  
  Los datos están disponibles en formatos como shapefile o geopack.

- **¿Existen restricciones geográficas en el acceso?**  
  No.
---

## **2. Calidad y Estandarización**

- **¿Qué nivel de resolución espacial tienen los datos?**  
 Los datos están disponibles en distintos formatos espaciales (ya sea por municipio, zona rural, urbana). Escalas desde 1:500.000, 1:250.000, 1:200.000, 1:100.000, 1:50.000, 1: 25.000, 1:10.000, 1:5000, 1:2.000 y 1:1000,
- **¿Qué nivel de resolución temporal tienen los datos?**  
 No tienen resolución temporal.

- **¿Los datos han sido sometidos a algún proceso de limpieza o validación?**  
  Sí.

- **¿Incluyen metadatos detallados y consistentes?**  
  Si.

---

## **3. Contenido de los Datos**

- **¿Qué variables están disponibles (temperatura, precipitación, viento, etc.)?**  
Disponible distintos tipos de mapas, como de terreno, politico-administrativo, entre otros.

- **¿Los datos incluyen mediciones históricas, simulaciones o ambos?**  
  Ninguna de las anteriores.
  
- **¿Existen variables derivadas (índices climáticos, anomalías, etc.)?**  
  No.

---

## **4. Uso y Licencia**

- **¿Qué tipo de licencia tienen los datos (libre, restringida, comercial)?**  
  Libre.

- **¿Existe un límite en la cantidad de datos que se pueden descargar?**  
  No.

- **¿Se requiere atribuir la fuente en caso de uso?**  
   Sí, se debe citar.
  
- **¿Es posible redistribuir los datos o integrarlos en nuevos productos?**  
  Si es posible.

---

## **5. Automatización y Escalabilidad**

- **¿Los datos están disponibles a través de APIs?**  
  Sí.

- **¿Existen restricciones en la velocidad o volumen de solicitudes para la descarga automatizada?**  
  No.

- **¿Qué herramientas o lenguajes son compatibles con el acceso a los datos (Python, R, etc.)?**  
  Python, R, entre otros.

---

## **6. Contexto y la Institución**

- **¿Quién es la entidad responsable de los datos?**  
INSTITUTO GEOGRÁFICO AGUSTÍN CODAZZI - IGAC
  
- **¿Cuál es la reputación de la fuente en términos de calidad y fiabilidad?**  
  Muy confiable.

- **¿Hay documentación técnica detallada para entender los datos?**  
Sí, disponible en la misma página.

---

## **7. Usabilidad**

- **¿Es necesario transformar los datos para adaptarlos a los objetivos del proyecto?**  
Considero que los datos proporcionados son útiles para el proyecto si se trabaja con datos espaciales de área, o se necesitan hacer recortes.
  
- **¿Qué tan fácil es integrar los datos con otras fuentes?**  
  Fácil debido a su formato shapefile.
  
- **¿Qué soporte técnico está disponible para resolver problemas?**  
  Correos, atención al público y líneas de atención.


## WorldClim
### **Fuente de Datos:  [WorldClim]**
[Geoportal IGAC](https://geoportal.igac.gov.co/contenido/tematicos)

## **1. Disponibilidad y Acceso**

- **¿Tiene información de lluvias (precipitación), fuertes lluvias?**  
  Si.

- **¿Tiene información de vientos, velocidad del viento, ráfagas de viento?**  
  Si.

- **¿Tiene información de temperaturas máximas, mínimas, promedio, datos de temperatura horaria?**  
  Si.
  
- **¿Tiene acceso libre?**  
  Si.

- **¿Desde cuándo hay datos?**  
  Desde 1970 para datos históricos.
  
- **¿Necesitamos un permiso especial?**  
   No.
  
- **¿Cómo se puede acceder a los datos?**
  A través de la página  https://www.worldclim.org/data/worldclim21.html.
  
- **¿Podemos crear una automatización para la descarga de los datos?**  
  Si.
  
- **¿Cuál es la frecuencia de actualización de los datos?**  
  No se actualzan.

- **¿En qué formato están disponibles los datos?**  
  TIFF (raster).

- **¿Existen restricciones geográficas en el acceso?**  
  No.
---

## **2. Calidad y Estandarización**

- **¿Qué nivel de resolución espacial tienen los datos?**  
 Los datos están disponibles entre 30 segundos (~1 km2) a 10 minutos (~340 km2).
- **¿Qué nivel de resolución temporal tienen los datos?**  
 Ventana de tiempo de cada 10 años. Se puede descargar promedios mensuales.

- **¿Los datos han sido sometidos a algún proceso de limpieza o validación?**  
  Sí.

- **¿Incluyen metadatos detallados y consistentes?**  
  Si.

---

## **3. Contenido de los Datos**

- **¿Qué variables están disponibles (temperatura, precipitación, viento, etc.)?**  
Temperatura mínima, máxima, promedio, precipitación, radiación solar, velocidad del viento, presión de vapor del agua.
- **¿Los datos incluyen mediciones históricas, simulaciones o ambos?**  
  Ambas.
  
- **¿Existen variables derivadas (índices climáticos, anomalías, etc.)?**  
  No.

---

## **4. Uso y Licencia**

- **¿Qué tipo de licencia tienen los datos (libre, restringida, comercial)?**  
  Libre.

- **¿Existe un límite en la cantidad de datos que se pueden descargar?**  
  No.

- **¿Se requiere atribuir la fuente en caso de uso?**  
   Sí, se debe citar.
  
- **¿Es posible redistribuir los datos o integrarlos en nuevos productos?**  
  Si es posible.

---

## **5. Automatización y Escalabilidad**

- **¿Los datos están disponibles a través de APIs?**  
  Sí.

- **¿Existen restricciones en la velocidad o volumen de solicitudes para la descarga automatizada?**  
  No.

- **¿Qué herramientas o lenguajes son compatibles con el acceso a los datos (Python, R, etc.)?**  
  Python, R, entre otros.

---

## **6. Contexto y la Institución**

- **¿Quién es la entidad responsable de los datos?**  
worldclim
  
- **¿Cuál es la reputación de la fuente en términos de calidad y fiabilidad?**  
  Confiable.

- **¿Hay documentación técnica detallada para entender los datos?**  
No hay mucha documentación sobre los datos.

---

## **7. Usabilidad**

- **¿Es necesario transformar los datos para adaptarlos a los objetivos del proyecto?**  
De acuerdo con los objetivos del proyecto, podría ser necesario.
  
- **¿Qué tan fácil es integrar los datos con otras fuentes?**  
  Fácil debido a su formato shapefile.
  
- **¿Qué soporte técnico está disponible para resolver problemas?**  
Por correo a info@worldclim.org 



---
## GFS
**Fuente de Datos: Global Forecast System**

## **1. Disponibilidad y Acceso**

- **¿Tiene información de lluvias (precipitación), fuertes lluvias?**  
  Sí.


- **¿Tiene información de vientos, velocidad del viento, ráfagas de viento?**  
  Sí.

- **¿Tiene información de temperaturas máximas, mínimas, promedio, datos de temperatura horaria?**  
  Sí.

- **¿Tiene acceso libre?**  
  Sí, el acceso es libre.

- **¿Desde cuándo hay datos?**  
  Desde 2015 para datos históricos; las proyecciones actuales están disponibles continuamente.

- **¿Necesitamos un permiso especial?**  
  No, el acceso no requiere permisos especiales.

- **¿Cómo se puede acceder a los datos?**  
  A través de servidores FTP/HTTP proporcionados por la NOAA o mediante APIs de servicios secundarios.

- **¿Podemos crear una automatización para la descarga de los datos?**  
  Sí, es posible automatizar la descarga con scripts que accedan a los servidores o APIs.

- **¿Cuál es la frecuencia de actualización de los datos?**  
  Los datos se actualizan cada 6 horas.

- **¿En qué formato están disponibles los datos?**  
  Generalmente en formato GRIB2 o NetCDF.

- **¿Existen restricciones geográficas en el acceso?**  
  No, los datos tienen cobertura global sin restricciones geográficas.

## **2. Calidad y Estandarización**

- **¿Qué nivel de resolución espacial tienen los datos?**  
  Resolución global con cuadrículas de 0.25°, 0.5°, o 1.0°, dependiendo de la versión y producto. 1° de latitud es aproximadamente constante en todo el mundo y equivale a 111 kilómetros, por lo tanto la resoluciones que hay disponibles son a 28, 55 y 111 kilómetros respectivamente.

- **¿Qué nivel de resolución temporal tienen los datos?**  
  Datos cada 6 horas.

- **¿Los datos han sido sometidos a algún proceso de limpieza o validación?**  
  Los datos se generan con modelos numéricos avanzados que incluyen procesos de validación y ajustes durante la asimilación.

- **¿Incluyen metadatos detallados y consistentes?**  
  Sí, los metadatos están incluidos en los archivos y documentación técnica.

## **3. Contenido de los Datos**

- **¿Qué variables están disponibles (temperatura, precipitación, viento, etc.)?**  
  Temperatura, precipitación, viento, presión, humedad, radiación, y muchas más.

- **¿Los datos incluyen mediciones históricas, simulaciones o ambos?**  
  Incluyen ambos: mediciones históricas (reanálisis) y simulaciones pronosticadas.

- **¿Existen variables derivadas (índices climáticos, anomalías, etc.)?**  
  Sí, aunque las derivadas suelen estar disponibles en bases de datos especializadas.

## **4. Uso y Licencia**

- **¿Qué tipo de licencia tienen los datos (libre, restringida, comercial)?**  
  Libre, con atribución a la NOAA.

- **¿Existe un límite en la cantidad de datos que se pueden descargar?**  
  Generalmente no, aunque puede haber restricciones de ancho de banda.

- **¿Se requiere atribuir la fuente en caso de uso?**  
  Sí, se debe atribuir a la NOAA como fuente.

- **¿Es posible redistribuir los datos o integrarlos en nuevos productos?**  
  Sí, bajo las condiciones de la licencia abierta.

## **5. Automatización y Escalabilidad**

- **¿Los datos están disponibles a través de APIs?**  
  Indirectamente, mediante servicios de terceros como Open-Meteo o APIs personalizadas.

- **¿Existen restricciones en la velocidad o volumen de solicitudes para la descarga automatizada?**  
  Puede haber restricciones de ancho de banda dependiendo del servidor utilizado.

- **¿Qué herramientas o lenguajes son compatibles con el acceso a los datos (Python, R, etc.)?**  
  Compatible con Python, R, C++, y cualquier lenguaje que maneje GRIB2 o NetCDF.

## **6. Contexto y la Institución**

- **¿Quién es la entidad responsable de los datos?**  
  La NOAA (National Oceanic and Atmospheric Administration) de Estados Unidos.

- **¿Cuál es la reputación de la fuente en términos de calidad y fiabilidad?**  
  Muy alta, considerada una de las fuentes más confiables en meteorología.

- **¿Hay documentación técnica detallada para entender los datos?**  
  Sí, disponible en el sitio oficial de la NOAA y en guías asociadas.

## **7. Usabilidad**

- **¿Es necesario transformar los datos para adaptarlos a los objetivos del proyecto?**  
  Dependiendo del proyecto, puede requerirse reformatar o interpolar los datos.

- **¿Qué tan fácil es integrar los datos con otras fuentes?**  
  Fácil, gracias a su estandarización y formatos comunes.

- **¿Qué soporte técnico está disponible para resolver problemas?**  
  Documentación oficial, foros de usuarios, y soporte comunitario.

## MeteoBlue
**Fuente de Datos: MeteoBlue**

## **1. Disponibilidad y Acceso**

- **¿Tiene información de lluvias (precipitación), fuertes lluvias?**  
  Sí, MeteoBlue proporciona datos detallados sobre precipitación, incluyendo intensidad y acumulación.

- **¿Tiene información de vientos, velocidad del viento, ráfagas de viento?**  
  Sí, ofrece información sobre velocidad y dirección del viento, así como ráfagas.

- **¿Tiene información de temperaturas máximas, mínimas, promedio, datos de temperatura horaria?**  
  Sí, proporciona datos de temperatura máxima, mínima, promedio y registros horarios.

- **¿Tiene acceso libre?**  
  MeteoBlue ofrece una API meteorológica gratuita para uso no comercial durante un año.

- **¿Desde cuándo hay datos?**  
  Los datos históricos están disponibles desde 1940.

- **¿Necesitamos un permiso especial?**  
  Para usos comerciales o acceso a datos avanzados, se requiere una suscripción o acuerdo específico.

- **¿Cómo se puede acceder a los datos?**  
  A través de diversas APIs que proporcionan datos en formatos como CSV y JSON.

- **¿Podemos crear una automatización para la descarga de los datos?**  
  Sí, las APIs permiten la automatización en la descarga de datos.

- **¿Cuál es la frecuencia de actualización de los datos?**  
  Los datos de previsión se actualizan constantemente para garantizar la máxima precisión.

- **¿En qué formato están disponibles los datos?**  
  Los datos están disponibles en formatos como CSV y JSON.

- **¿Existen restricciones geográficas en el acceso?**  
  No, los datos están disponibles para cualquier ubicación en el mundo.

---

## **2. Calidad y Estandarización**

- **¿Qué nivel de resolución espacial tienen los datos?**  
  La resolución espacial varía según el modelo, llegando hasta 1 km en algunas regiones.

- **¿Qué nivel de resolución temporal tienen los datos?**  
  Los datos están disponibles en resoluciones horarias y diarias.

- **¿Los datos han sido sometidos a algún proceso de limpieza o validación?**  
  Sí, los datos se someten a procesos de validación para garantizar su precisión.

- **¿Incluyen metadatos detallados y consistentes?**  
  Sí, se proporcionan metadatos detallados para facilitar su interpretación.

---

## **3. Contenido de los Datos**

- **¿Qué variables están disponibles (temperatura, precipitación, viento, etc.)?**  
  Están disponibles variables como temperatura, precipitación, viento, humedad, radiación solar, entre otras.

- **¿Los datos incluyen mediciones históricas, simulaciones o ambos?**  
  Incluyen tanto datos históricos como previsiones y simulaciones.

- **¿Existen variables derivadas (índices climáticos, anomalías, etc.)?**  
  Sí, se ofrecen variables derivadas como índices climáticos y anomalías.

---

## **4. Uso y Licencia**

- **¿Qué tipo de licencia tienen los datos (libre, restringida, comercial)?**  
  Las licencias varían desde uso libre para fines no comerciales hasta licencias comerciales para usos empresariales.

- **¿Existe un límite en la cantidad de datos que se pueden descargar?**  
  Sí, existen límites que dependen del plan de suscripción o acuerdo establecido.

- **¿Se requiere atribuir la fuente en caso de uso?**  
  Para usos no comerciales, se recomienda atribuir la fuente; en usos comerciales, depende del acuerdo.

- **¿Es posible redistribuir los datos o integrarlos en nuevos productos?**  
  La redistribución y la integración en nuevos productos están sujetas a los términos de la licencia adquirida.

---

## **5. Automatización y Escalabilidad**

- **¿Los datos están disponibles a través de APIs?**  
  Sí, MeteoBlue ofrece diversas APIs para acceder a los datos.

- **¿Existen restricciones en la velocidad o volumen de solicitudes para la descarga automatizada?**  
  Sí, se imponen límites en la cantidad de llamadas por minuto y por día, dependiendo del plan.

- **¿Qué herramientas o lenguajes son compatibles con el acceso a los datos (Python, R, etc.)?**  
  Las APIs son compatibles con cualquier lenguaje que soporte solicitudes HTTP, como Python, R, entre otros.

---

## **6. Contexto y la Institución**

- **¿Quién es la entidad responsable de los datos?**  
  MeteoBlue AG, una empresa suiza especializada en datos meteorológicos.

- **¿Cuál es la reputación de la fuente en términos de calidad y fiabilidad?**  
  Alta reputación por su precisión y confiabilidad en el sector meteorológico.

- **¿Hay documentación técnica detallada para entender los datos?**  
  Sí, hay documentación técnica extensa disponible.

---

## **7. Usabilidad**

- **¿Es necesario transformar los datos para adaptarlos a los objetivos del proyecto?**  
  Depende del objetivo, pero los datos suelen estar listos para su análisis directo.

- **¿Qué tan fácil es integrar los datos con otras fuentes?**  
  Relativamente fácil debido a su formato estándar y metadatos consistentes.

- **¿Qué soporte técnico está disponible para resolver problemas?**  
  Se ofrece soporte técnico a través de la documentación y contacto directo para usuarios comerciales.


## Google Earth Engine
**Fuente de Datos: Google Earth Engine**

## **1. Disponibilidad y Acceso**

- **¿Tiene información de lluvias (precipitación), fuertes lluvias?**  
  Sí, a través de datasets como CHIRPS y ERA5, que incluyen datos de precipitación histórica y estimada.

- **¿Tiene información de vientos, velocidad del viento, ráfagas de viento?**  
  Sí, se pueden consultar datos de modelos globales como ERA5 y NCEP/NCAR.

- **¿Tiene información de temperaturas máximas, mínimas, promedio, datos de temperatura horaria?**  
  Sí, está disponible en datasets como ERA5 y TerraClimate.

- **¿Tiene acceso libre?**  
  Sí, Google Earth Engine es gratuito para usos de investigación, educativos y no comerciales.

- **¿Desde cuándo hay datos?**  
  La disponibilidad varía según el dataset; algunos tienen datos desde la década de 1940, como ERA5.

- **¿Necesitamos un permiso especial?**  
  No para la mayoría de los usos académicos y de investigación, pero se deben aceptar los términos de servicio.

- **¿Cómo se puede acceder a los datos?**  
  A través de la plataforma Google Earth Engine mediante programación en JavaScript o Python.

- **¿Podemos crear una automatización para la descarga de los datos?**  
  Sí, usando scripts en Python con la API de Earth Engine.

- **¿Cuál es la frecuencia de actualización de los datos?**  
  Varía según el dataset; algunos datos son actualizados diariamente (e.g., MODIS).

- **¿En qué formato están disponibles los datos?**  
  Los datos se procesan en formatos de imágenes rasterizadas y vectores, y se pueden exportar en formatos como GeoTIFF y CSV.

- **¿Existen restricciones geográficas en el acceso?**  
  No, los datos son globales, pero algunos datasets pueden estar restringidos por licencias locales.

---

## **2. Calidad y Estandarización**

- **¿Qué nivel de resolución espacial tienen los datos?**  
  Depende del dataset; por ejemplo, MODIS tiene una resolución de 500 m, mientras que Landsat tiene 30 m.

- **¿Qué nivel de resolución temporal tienen los datos?**  
  Varía entre diaria (e.g., MODIS) y mensual (e.g., CHIRPS).

- **¿Los datos han sido sometidos a algún proceso de limpieza o validación?**  
  Sí, la mayoría de los datasets han sido preprocesados y validados por las instituciones que los generan.

- **¿Incluyen metadatos detallados y consistentes?**  
  Sí, cada dataset incluye descripciones detalladas y metadatos.

---

## **3. Contenido de los Datos**

- **¿Qué variables están disponibles (temperatura, precipitación, viento, etc.)?**  
  Variables como precipitación, temperatura, humedad, viento, índices de vegetación, entre otras.

- **¿Los datos incluyen mediciones históricas, simulaciones o ambos?**  
  Incluyen ambos; hay datos históricos de sensores remotos y simulaciones de modelos climáticos.

- **¿Existen variables derivadas (índices climáticos, anomalías, etc.)?**  
  Sí, se incluyen índices como NDVI, EVI, y anomalías climáticas.

---

## **4. Uso y Licencia**

- **¿Qué tipo de licencia tienen los datos (libre, restringida, comercial)?**  
  Los datos varían en sus licencias; muchos son gratuitos para uso público, pero algunos requieren permisos adicionales.

- **¿Existe un límite en la cantidad de datos que se pueden descargar?**  
  Sí, Google Earth Engine tiene políticas de cuota que limitan la capacidad de procesamiento y descarga.

- **¿Se requiere atribuir la fuente en caso de uso?**  
  Sí, se debe atribuir tanto a Google Earth Engine como a la institución propietaria del dataset.

- **¿Es posible redistribuir los datos o integrarlos en nuevos productos?**  
  La redistribución está sujeta a las licencias individuales de cada dataset.

---

## **5. Automatización y Escalabilidad**

- **¿Los datos están disponibles a través de APIs?**  
  Sí, Google Earth Engine proporciona una API potente para Python y JavaScript.

- **¿Existen restricciones en la velocidad o volumen de solicitudes para la descarga automatizada?**  
  Sí, existen cuotas que limitan las solicitudes por usuario y día.

- **¿Qué herramientas o lenguajes son compatibles con el acceso a los datos (Python, R, etc.)?**  
  Compatible con Python, JavaScript, y puede integrarse con R mediante paquetes como `rgee`.

---

## **6. Contexto y la Institución**

- **¿Quién es la entidad responsable de los datos?**  
  Google y las instituciones propietarias de los datasets, como NASA, NOAA, y ECMWF.

- **¿Cuál es la reputación de la fuente en términos de calidad y fiabilidad?**  
  Altamente reputada por su precisión y soporte global.

- **¿Hay documentación técnica detallada para entender los datos?**  
  Sí, la plataforma incluye documentación detallada para cada dataset.

---

## **7. Usabilidad**

- **¿Es necesario transformar los datos para adaptarlos a los objetivos del proyecto?**  
  Puede ser necesario dependiendo del análisis; las herramientas de Earth Engine facilitan estas transformaciones.

- **¿Qué tan fácil es integrar los datos con otras fuentes?**  
  Muy fácil debido al soporte de formatos estándar y APIs flexibles.

- **¿Qué soporte técnico está disponible para resolver problemas?**  
  Soporte a través de la documentación, foros de usuarios, y recursos educativos en línea.

## Power NASA

## **1. Disponibilidad y Acceso**

- **¿Tiene información de lluvias (precipitación), fuertes lluvias?**  
  Sí.
- **¿Tiene información de vientos, velocidad del viento, ráfagas de viento?**  
  Sí.
- **¿Tiene información de temperaturas máximas, mínimas, promedio, datos de temperatura horaria?**  
  Sí.
- **¿Tiene acceso libre?**  
  Sí.
- **¿Desde cuándo hay datos?**  
  Desde 1981.
- **¿Necesitamos un permiso especial?**  
  No.
- **¿Cómo se puede acceder a los datos?**  
  Descargas en formato CSV o acceso mediante JSON.
- **¿Podemos crear una automatización para la descarga de los datos?**  
  Buscando alternativa para automatizar.
- **¿Cuál es la frecuencia de actualización de los datos?**  
  Diaria.
- **¿En qué formato están disponibles los datos?**  
  CSV, ASCII, JSON.
- **¿Existen restricciones geográficas en el acceso?**  
  Sí, solo se pueden tomar regiones de tamaño 10° por archivo CSV.

---

## **2. Calidad y Estandarización**

- **¿Qué nivel de resolución espacial tienen los datos?**  
  A escala global con resolución adaptativa.
- **¿Qué nivel de resolución temporal tienen los datos?**  
  Datos desde 1981 hasta la actualidad.
- **¿Los datos han sido sometidos a algún proceso de limpieza o validación?**  
  Sí.
- **¿Incluyen metadatos detallados y consistentes?**  
  Sí.

---

## **3. Contenido de los Datos**

- **¿Qué variables están disponibles (temperatura, precipitación, viento, etc.)?**  
  Contiene las cinco variables del índice del clima actuarial mencionadas en el PDF.  
- **¿Los datos incluyen mediciones históricas, simulaciones o ambos?**  
  Ambos.  
- **¿Existen variables derivadas (índices climáticos, anomalías, etc.)?**  
  No.

---

## **4. Uso y Licencia**

- **¿Qué tipo de licencia tienen los datos (libre, restringida, comercial)?**  
  Libre.  
- **¿Existe un límite en la cantidad de datos que se pueden descargar?**  
  Cada archivo CSV permite descargar por periodos de un año; sin embargo, no hay límite de archivos descargables.  
- **¿Se requiere atribuir la fuente en caso de uso?**  
  Sí.  
- **¿Es posible redistribuir los datos o integrarlos en nuevos productos?**  
  Sí, cumpliendo las condiciones establecidas por la NASA. Los datos son de dominio público, pero su redistribución debe respetar los términos de uso.

---
- **¿Desde cuándo hay datos?**  
  Desde 2015 para datos históricos; las proyecciones actuales están disponibles continuamente.
## **5. Automatización y Escalabilidad**

- **¿Los datos están disponibles a través de APIs?**  
  Sí. [API de NASA POWER](https://power.larc.nasa.gov/api/temporal/daily/point?parameters=T2M&community=AG&longitude=-77.0369&latitude=38.9072&start=20230101&end=20231231&format=JSON)  
- **¿Existen restricciones en la velocidad o volumen de solicitudes para la descarga automatizada?**  
  - **Límite de solicitudes por minuto/hora:** Entre 60 y 120 solicitudes por minuto.  
  - **Volumen de datos:** Máximo de 366 días por solicitud.  
  - **Uso justo:** El servicio está diseñado para fines científicos y educativos; solicitudes masivas podrían restringirse.  
- **¿Qué herramientas o lenguajes son compatibles con el acceso a los datos (Python, R, etc.)?**  
  Python, R, MATLAB.

---

## **6. Contexto y la Institución**

- **¿Quién es la entidad responsable de los datos?**  
  La NASA, específicamente a través de la herramienta NASA POWER del Langley Research Center (LaRC).  
- **¿Cuál es la reputación de la fuente en términos de calidad y fiabilidad?**  
  La NASA tiene una reputación mundialmente reconocida por la calidad y fiabilidad de sus datos. Los datos de NASA POWER son validados mediante estaciones meteorológicas y modelos precisos.  
- **¿Hay documentación técnica detallada para entender los datos?**  
  Sí, incluye metodología, variables y ejemplos prácticos.

---

## **7. Usabilidad**

- **¿Es necesario transformar los datos para adaptarlos a los objetivos del proyecto?**  
  Los datos están en formatos estándar como CSV o JSON, adaptables a diversas herramientas.  
- **¿Qué tan fácil es integrar los datos con otras fuentes?**  
  - **Formatos estándar:** Compatibles con Python, R, MATLAB, Power BI y Tableau.  
  - **API accesible:** Permite integrar datos en flujos de trabajo automatizados.  
- **¿Qué soporte técnico está disponible para resolver problemas?**  
  - **Documentación detallada:** Con ejemplos y guías.  
  - **Correo de soporte:** [power@larc.nasa.gov](mailto:power@larc.nasa.gov).  
  - **FAQ:** Página oficial con preguntas frecuentes.  

---
## HUMBOLDT

### **1. Disponibilidad y Acceso**
[Visor I2D](http://i2d.humboldt.org.co/visor-I2D/)
[**El siguiente link contiene la información que proporciona HUMBOLDT**](http://geonetwork.humboldt.org.co/geonetwork/srv/spa/catalog.search#/search?facet.q=topicCat%2Fenvironment&resultType=details&fast=index&_content_type=json&from=1&to=100&sortBy=relevance)


- **¿Tiene información de lluvias (precipitación), fuertes lluvias?**
  Sí
- **¿Tiene información de vientos, velocidad del viento, rafagas de viento?**
  Sí
- **¿Tiene información de temperaturas máximas, mínimas, promedio, datos de temperatura horaria?**
  Sí
- **¿Tiene acceso libre?**
  Sí
- **¿Desde cuándo hay datos?**
  En bases desde el 2012, en forma de estudios 1986
- **¿Necesitamos un permiso especial?**
  No
- **¿Cómo se puede acceder a los datos?**
  Directamente por la página
- **¿Podemos crear una automatización para la descarga de los datos?**
  Sí 
- **¿Cuál es la frecuencia de actualización de los datos?**
  Soy estudios sobre datos del IDEAM, se podría revisar el IDEAM
- **¿En qué formato están disponibles los datos?**
  metadata,  Csv, Tsv, Rdf, XML
- **¿Existen restricciones geográficas en el acceso?**
  No

### **2. Calidad y Estandarización**
- **¿Qué nivel de resolución espacial tienen los datos?**
Son de toda Colombia
- **¿Qué nivel de resolución temporal tienen los datos?**
De decada
- **¿Los datos han sido sometidos a algún proceso de limpieza o validación?**
Sí son datos del IDEAM
- **¿Incluyen metadatos detallados y consistentes?**
Sí

### **3. Contenido de los Datos**
- **¿Qué variables están disponibles (temperatura, precipitación, viento, etc.)?**
- Temperatura, precipitación y geológicas de todo tipo
- **¿Los datos incluyen mediciones históricas, simulaciones o ambos?**
  temperatura máxima y humedad máxima o míninma sobre mes.
- **¿Existen variables derivadas (índices climáticos, anomalías, etc.)?**
  No, solo mediciones más no indicadores

### **4. Uso y Licencia**
- **¿Qué tipo de licencia tienen los datos (libre, restringida, comercial)?**
  libre
- **¿Existe un límite en la cantidad de datos que se pueden descargar?**
  No
- **¿Se requiere atribuir la fuente en caso de uso?**
  Sí
- **¿Es posible redistribuir los datos o integrarlos en nuevos productos?**
  Sí

### **5. Automatización y Escalabilidad**
- **¿Los datos están disponibles a través de APIs?**
  POsiblemente, en formato json se puede scrapear, más tener una Apu pública no.
- **¿Existen restricciones en la velocidad o volumen de solicitudes para la descarga automatizada?**
  NO
- **¿Qué herramientas o lenguajes son compatibles con el acceso a los datos (Python, R, etc.)?**
Python

### **6. Contexto y la Institución**
- **¿Quién es la entidad responsable de los datos?**
  IDEAM y el propio instituto Humbold
- **¿Cuál es la reputación de la fuente en términos de calidad y fiabilidad?**
  TIenen clificaciones de usuarios lo cuál puede dar veracidad
- **¿Hay documentación técnica detallada para entender los datos?**
  Sí

### **7. Usabilidad**
- **¿Es necesario transformar los datos para adaptarlos a los objetivos del proyecto?**
  Sí
- **¿Qué tan fácil es integrar los datos con otras fuentes?**
  Hay que confirmarlo
- **¿Qué soporte técnico está disponible para resolver problemas?**
  No
---

## INVEMAR
**Fuente de Datos:  [Invemar ]**

### **1. Disponibilidad y Acceso**

- **¿Tiene información de lluvias (precipitación), fuertes lluvias?**
  No
- **¿Tiene información de vientos, velocidad del viento, rafagas de viento?**
  No
- **¿Tiene información de temperaturas máximas, mínimas, promedio, datos de temperatura horaria?**
  No
- **¿Tiene acceso libre?**
  Sí
- **¿Desde cuándo hay datos?**
  De los últimos 10 años (2024)
- **¿Necesitamos un permiso especial?**
  No
- **¿Cómo se puede acceder a los datos?**
  Mediante API, yaque está directamente en la página de datos abiertos
- **¿Podemos crear una automatización para la descarga de los datos?**
  Sí
- **¿Cuál es la frecuencia de actualización de los datos?**
  Sin definir
- **¿En qué formato están disponibles los datos?**
  Csv, Tsv, Rdf, XML
- **¿Existen restricciones geográficas en el acceso?**
  No

### **2. Calidad y Estandarización**
- **¿Qué nivel de resolución espacial tienen los datos?**
  Tiene Polygonos, o puntos (Lat, long)
- **¿Qué nivel de resolución temporal tienen los datos?**
  Tiene Día, mes y año
- **¿Los datos han sido sometidos a algún proceso de limpieza o validación?**
  Sí
- **¿Incluyen metadatos detallados y consistentes?**
- Sí

### **3. Contenido de los Datos**
- **¿Qué variables están disponibles (temperatura, precipitación, viento, etc.)?**
  Puntos geoespaciales donde se encuentran arrecifes y Activos de Información, Áreas Coralinas 
- **¿Los datos incluyen mediciones históricas, simulaciones o ambos?**
  Mediciones Históricas
- **¿Existen variables derivadas (índices climáticos, anomalías, etc.)?**
  No

### **4. Uso y Licencia**
- **¿Qué tipo de licencia tienen los datos (libre, restringida, comercial)?**
    libre
- **¿Existe un límite en la cantidad de datos que se pueden descargar?**
  Sí, pero es manejo de la API, se puede hacer para que tome más de 1000 registros
- **¿Se requiere atribuir la fuente en caso de uso?**
  No
- **¿Es posible redistribuir los datos o integrarlos en nuevos productos?**
  Sí

### **5. Automatización y Escalabilidad**
- **¿Los datos están disponibles a través de APIs?**
  Sí
- **¿Existen restricciones en la velocidad o volumen de solicitudes para la descarga automatizada?**
  No
- **¿Qué herramientas o lenguajes son compatibles con el acceso a los datos (Python, R, etc.)?**
  Python, R y Java
### **6. Contexto y la Institución**
- **¿Quién es la entidad responsable de los datos?**
  INVEMAR 
- **¿Cuál es la reputación de la fuente en términos de calidad y fiabilidad?**
  Es buena
- **¿Hay documentación técnica detallada para entender los datos?**
  Sí, en la misma página de datos abiertos
  
### **7. Usabilidad**
- **¿Es necesario transformar los datos para adaptarlos a los objetivos del proyecto?**
  Sí, hacer uniones entre diferentes valores y variables
- **¿Qué tan fácil es integrar los datos con otras fuentes?**
  Al tener geoespaciales, se pueden manejar los centroides de los polygonos para diferentes estudios.
- **¿Qué soporte técnico está disponible para resolver problemas?**
  El de datosabiertos

---

## Corporaciones Autónomas
**Fuente de Datos: Corporaciones Autónomas Regionales**
Sobre este, para entrar a estudiar su acceso hay que pedir requerimientos escritos a dicha asosiación. De igual manera algunas compañias que hacen parte de este, tienen bases de datos abiertas, muy textuales. Son estudios locales donde los datos son de la región y la mayoría de la información es compartida en formato PDF.

---

## IRI
**Fuente de Datos: International Research Institute for Climate and Society: IRI**

## **1. Disponibilidad y Acceso**

- **¿Tiene información de lluvias (precipitación), fuertes lluvias?**  
  Sí.


- **¿Tiene información de vientos, velocidad del viento, ráfagas de viento?**  
  Sí.

- **¿Tiene información de temperaturas máximas, mínimas, promedio, datos de temperatura horaria?**  
  Sí.

- **¿Tiene acceso libre?**  
  Sí, el acceso es libre.

- **¿Desde cuándo hay datos?**  
  Desde 2015 para datos históricos; las proyecciones actuales están disponibles continuamente.

- **¿Necesitamos un permiso especial?**  
  No, el acceso no requiere permisos especiales.

- **¿Cómo se puede acceder a los datos?**  
  A través de servidores FTP/HTTP proporcionados por la NOAA o mediante APIs de servicios secundarios.

- **¿Podemos crear una automatización para la descarga de los datos?**  
  Sí, es posible automatizar la descarga con scripts que accedan a los servidores o APIs.

- **¿Cuál es la frecuencia de actualización de los datos?**  
  Los datos se actualizan cada 6 horas.

- **¿En qué formato están disponibles los datos?**  
  Generalmente en formato GRIB2 o NetCDF.

- **¿Existen restricciones geográficas en el acceso?**  
  No, los datos tienen cobertura global sin restricciones geográficas.

## **2. Calidad y Estandarización**

- **¿Qué nivel de resolución espacial tienen los datos?**  
  Resolución global con cuadrículas de 0.25°, 0.5°, o 1.0°, dependiendo de la versión y producto. 1° de latitud es aproximadamente constante en todo el mundo y equivale a 111 kilómetros, por lo tanto la resoluciones que hay disponibles son a 28, 55 y 111 kilómetros respectivamente.

- **¿Qué nivel de resolución temporal tienen los datos?**  
  Datos cada 6 horas.

- **¿Los datos han sido sometidos a algún proceso de limpieza o validación?**  
  Los datos se generan con modelos numéricos avanzados que incluyen procesos de validación y ajustes durante la asimilación.

- **¿Incluyen metadatos detallados y consistentes?**  
  Sí, los metadatos están incluidos en los archivos y documentación técnica.

## **3. Contenido de los Datos**

- **¿Qué variables están disponibles (temperatura, precipitación, viento, etc.)?**  
  Temperatura, precipitación, viento, presión, humedad, radiación, y muchas más.

- **¿Los datos incluyen mediciones históricas, simulaciones o ambos?**  
  Incluyen ambos: mediciones históricas (reanálisis) y simulaciones pronosticadas.

- **¿Existen variables derivadas (índices climáticos, anomalías, etc.)?**  
  Sí, aunque las derivadas suelen estar disponibles en bases de datos especializadas.

## **4. Uso y Licencia**

- **¿Qué tipo de licencia tienen los datos (libre, restringida, comercial)?**  
  Libre, con atribución a la NOAA.

- **¿Existe un límite en la cantidad de datos que se pueden descargar?**  
  Generalmente no, aunque puede haber restricciones de ancho de banda.

- **¿Se requiere atribuir la fuente en caso de uso?**  
  Sí, se debe atribuir a la NOAA como fuente.

- **¿Es posible redistribuir los datos o integrarlos en nuevos productos?**  
  Sí, bajo las condiciones de la licencia abierta.

## **5. Automatización y Escalabilidad**

- **¿Los datos están disponibles a través de APIs?**  
  Indirectamente, mediante servicios de terceros como Open-Meteo o APIs personalizadas.

- **¿Existen restricciones en la velocidad o volumen de solicitudes para la descarga automatizada?**  
  Puede haber restricciones de ancho de banda dependiendo del servidor que sea utilizado.

- **¿Qué herramientas o lenguajes son compatibles con el acceso a los datos (Python, R, etc.)?**  
  Compatible con Python, R, C++, y cualquier lenguaje que maneje GRIB2 o NetCDF.

## **6. Contexto y la Institución**

- **¿Quién es la entidad responsable de los datos?**  
  La NOAA (National Oceanic and Atmospheric Administration) de Estados Unidos.

- **¿Cuál es la reputación de la fuente en términos de calidad y fiabilidad?**  
  Muy alta, considerada una de las fuentes más confiables en meteorología.

- **¿Hay documentación técnica detallada para entender los datos?**  
  Sí, disponible en el sitio oficial de la NOAA y en guías asociadas.

## **7. Usabilidad**

- **¿Es necesario transformar los datos para adaptarlos a los objetivos del proyecto?**  
  Dependiendo del proyecto, puede requerirse reformatar o interpolar los datos.

- **¿Qué tan fácil es integrar los datos con otras fuentes?**  
  Fácil, gracias a su estandarización y formatos comunes.

- **¿Qué soporte técnico está disponible para resolver problemas?**  
  Documentación oficial, foros de usuarios, y soporte comunitario.

  ## **Fuente de Datos: International Research institute [IRI](https://iridl.ldeo.columbia.edu/)**

La Biblioteca de Datos del IRI es un almacén de datos disponible libremente en línea. Permite hacer búsquedas por la fuente de los datos, por categoría (aunque está habilitado en la página en lo personal no me permite realizar esta búsqueda, pero seria muy útil) y por mapas organizados por palabras clave. La biblioteca cuenta con un visualizador de datos incorporado. 
La base de datos evaluada es la base ENACTS_V1 Y ENACTS_V2. Son tablas consolidadas para temperatura y precipitación con datos diarios consolidados de las siguientes estaciones:
Temperatura: Stations from IDEAM + MERRA2
Precipitación: Stations from IDEAM + CHIRPS

=======
## CHIRPS
**Fuente de Datos:  [CHIRPS: Rainfall Estimates from Rain Gauge and Satellite Observations]**

### **1. Disponibilidad y Acceso**

- **¿Tiene información de lluvias (precipitación), fuertes lluvias?**
  Precipitación global diaria y mensual
- **¿Tiene información de vientos, velocidad del viento, rafagas de viento?**
  No.
- **¿Tiene información de temperaturas máximas, mínimas, promedio, datos de temperatura horaria?**
 global 2-m maximum temperature (Tmax)  a monthly estimate of the daily maximum temperature for the 1983-2016 time period.
- **¿Tiene acceso libre?**
  Si.
- **¿Desde cuándo hay datos?**
  Precipitación 1981 en adenlante para colombia
- **¿Necesitamos un permiso especial?**
  No.
- **¿Cómo se puede acceder a los datos?**
  Descargarlos a través de la página https://data.chc.ucsb.edu/products/CHIRPS-2.0/
- **¿Podemos crear una automatización para la descarga de los datos?**
- No es claro si se puede
- **¿Cuál es la frecuencia de actualización de los datos?**
  Los CHIRPS finales (todos los datos de la estación) estarán disponibles en algún momento de la tercera semana del mes 
  siguiente.
- **¿En qué formato están disponibles los datos?**
  .tif,  .bil,   .netcdf
- **¿Existen restricciones geográficas en el acceso?**
  No.

### **2. Calidad y Estandarización**
- **¿Qué nivel de resolución espacial tienen los datos?**
  CAmer-Carib 23.5N-6N, 93W- 57W @0.05 deg resolution = maps 720 x 350 pixels
- **¿Qué nivel de resolución temporal tienen los datos?**
  Desde 1981 en adelante y tiempo real
- **¿Los datos han sido sometidos a algún proceso de limpieza o validación?**
Un producto de datos en formato gridded construido utilizando un enfoque de modelado geoespacial basado en regresiones de ventana móvil e interpolación por ponderación de la distancia inversa. Este enfoque combina campos satelitales, indicadores fisiográficos en grilla y datos climáticos normales in situ provenientes de estaciones meteorológicas. La climatología global de precipitación resultante, con una resolución de 0.05° y datos mensuales para 12 meses, puede superar en desempeño a productos globales de climatología similares, especialmente en áreas con terreno complejo y baja densidad de estaciones meteorológicas.
- **¿Incluyen metadatos detallados y consistentes?**
  Si, los metadatos están incluidos en los archivos y documentación técnica.

### **3. Contenido de los Datos**
- **¿Qué variables están disponibles (temperatura, precipitación, viento, etc.)?**
Temperatura y precipitación y sequía
- **¿Los datos incluyen mediciones históricas, simulaciones o ambos?**
  Historicas y forecasting del tiempo y clima por temporados no a nivel de escenarios de cambio climatico
- **¿Existen variables derivadas (índices climáticos, anomalías, etc.)?**
NA

### **4. Uso y Licencia**
- **¿Qué tipo de licencia tienen los datos (libre, restringida, comercial)?**
  Libre.
- **¿Existe un límite en la cantidad de datos que se pueden descargar?**
  No
- **¿Se requiere atribuir la fuente en caso de uso?**
  Si, es necesario citar
- **¿Es posible redistribuir los datos o integrarlos en nuevos productos?**
  Si

### **5. Automatización y Escalabilidad**
- **¿Los datos están disponibles a través de APIs?**
  No es claro, en teoria si
- **¿Existen restricciones en la velocidad o volumen de solicitudes para la descarga automatizada?**
  No
- **¿Qué herramientas o lenguajes son compatibles con el acceso a los datos (Python, R, etc.)?**
  R, python, julia

### **6. Contexto y la Institución**
- **¿Quién es la entidad responsable de los datos?**
  University of California, Santa Barbara
- **¿Cuál es la reputación de la fuente en términos de calidad y fiabilidad?**
  Muy confiable.
- **¿Hay documentación técnica detallada para entender los datos?**
  Si, disponible en la misma página.

### **7. Usabilidad**
- **¿Es necesario transformar los datos para adaptarlos a los objetivos del proyecto?**
  Considero que los datos e infomación SON UTILES para el proyecto.
- **¿Qué tan fácil es integrar los datos con otras fuentes?**
  Es viable integralos con otras fuentes de información
- **¿Qué soporte técnico está disponible para resolver problemas?**
Pete Peterson
pete@geog.ucsb.edu
https://wiki.chc.ucsb.edu/CHIRPS_FAQ
  _________________________

## CAR
**Fuente de Datos:  [CAR] Corporación Autónomoa Regional de Cundinamarca**

### **1. Disponibilidad y Acceso**

- **¿Tiene información de lluvias (precipitación), fuertes lluvias?**
- No
- **¿Tiene información de vientos, velocidad del viento, rafagas de viento?**
  No.
- **¿Tiene información de temperaturas máximas, mínimas, promedio, datos de temperatura horaria?**
 No.
- **¿Tiene acceso libre?**
  No.
- **¿Desde cuándo hay datos?**
  No.
- **¿Necesitamos un permiso especial?**
  No.
- **¿Cómo se puede acceder a los datos?**
  Podemos indagar haciendo una solciitud formal. En la pagina web solo hay estudios y resultados pero no datos para acceso libre
- **¿Podemos crear una automatización para la descarga de los datos?**
- No
- **¿Cuál es la frecuencia de actualización de los datos?**
  NA
- **¿En qué formato están disponibles los datos?**
 NA
- **¿Existen restricciones geográficas en el acceso?**
 NA

### **2. Calidad y Estandarización**
- **¿Qué nivel de resolución espacial tienen los datos?**
 NA
- **¿Qué nivel de resolución temporal tienen los datos?**
No esta definido
- **¿Los datos han sido sometidos a algún proceso de limpieza o validación?**
NA
- **¿Incluyen metadatos detallados y consistentes?**
  NA

### **3. Contenido de los Datos**
- **¿Qué variables están disponibles (temperatura, precipitación, viento, etc.)?**
NA
- **¿Los datos incluyen mediciones históricas, simulaciones o ambos?**
 NA
- **¿Existen variables derivadas (índices climáticos, anomalías, etc.)?**
NA

### **4. Uso y Licencia**
- **¿Qué tipo de licencia tienen los datos (libre, restringida, comercial)?**
  NA
- **¿Existe un límite en la cantidad de datos que se pueden descargar?**
  NA
- **¿Se requiere atribuir la fuente en caso de uso?**
NA
- **¿Es posible redistribuir los datos o integrarlos en nuevos productos?**
 NA

### **5. Automatización y Escalabilidad**
- **¿Los datos están disponibles a través de APIs?**
  NA
- **¿Existen restricciones en la velocidad o volumen de solicitudes para la descarga automatizada?**
NA
- **¿Qué herramientas o lenguajes son compatibles con el acceso a los datos (Python, R, etc.)?**
  NA

### **6. Contexto y la Institución**
- **¿Quién es la entidad responsable de los datos?**
  CAR CUNDINAMARCA
- **¿Cuál es la reputación de la fuente en términos de calidad y fiabilidad?**
 NA
- **¿Hay documentación técnica detallada para entender los datos?**
  NO

### **7. Usabilidad**
- **¿Es necesario transformar los datos para adaptarlos a los objetivos del proyecto?**
  Considero que los datos e infomación NO es útil para el proyecto.
- **¿Qué tan fácil es integrar los datos con otras fuentes?**
  NA
- **¿Qué soporte técnico está disponible para resolver problemas?**
NA
________________________________________
__________________________
## CPTEC
**Fuente de Datos:  [Climate data library: INPE CPTEC latam]**

### **1. Disponibilidad y Acceso**
- **¿Tiene información de lluvias (precipitación), fuertes lluvias?**
  Precipitación global diaria y mensual
- **¿Tiene información de vientos, velocidad del viento, rafagas de viento?**
  No.
- **¿Tiene información de temperaturas máximas, mínimas, promedio, datos de temperatura horaria?**
 NO
- **¿Tiene acceso libre?**
  Si.
- **¿Desde cuándo hay datos?**
  Precipitación 1981 en adenlante para colombia
- **¿Necesitamos un permiso especial?**
  No.
- **¿Cómo se puede acceder a los datos?**
  Descargarlos a través de la página https://iridl.ldeo.columbia.edu/SOURCES/.INPE/.CPTEC/.latam/.CoSch/
- **¿Podemos crear una automatización para la descarga de los datos?**
- No es claro si se puede
- **¿Cuál es la frecuencia de actualización de los datos?**
Última Actualización: Mon, 23 Oct 2023
- **¿En qué formato están disponibles los datos?**
  .tif,  .bil,   .netcdf
- **¿Existen restricciones geográficas en el acceso?**
  No.

### **2. Calidad y Estandarización**
- **¿Qué nivel de resolución espacial tienen los datos?**
  Time (time)	grid: /T (days since 1960-01-01) ordered (2 Jun 2000) to (2 Jun 2024) by 1.0 N= 8767 pts :grid
Longitude (longitude)	grid: /X (degree_east) ordered (115.95W) to (30.14999W) by 0.1 N= 859 pts :grid
Latitude (latitude)	grid: /Y (degree_north) ordered (59.95S) to (32.85N) by 0.1 N= 929 pts :grid
- **¿Qué nivel de resolución temporal tienen los datos?**
 Time (time)	grid: /T (days since 1960-01-01) ordered (2 Jun 2000) to (2 Jun 2024) by 1.0 N= 8767 pts :grid
1981 PARA colombia
- **¿Los datos han sido sometidos a algún proceso de limpieza o validación?**
Durante este proyecto, se implementaron varias estrategias de validación para verificar el rendimiento del conjunto de datos de LatAm. La primera es un enfoque estadístico clásico que utiliza un proceso de correlación cruzada con pluviómetros diarios como referencia en terreno (ground truth). La segunda estrategia se basa en pérdidas relevantes para el sector asegurador ocurridas en terreno en América Central y del Sur, utilizando herramientas e información desarrolladas y recopiladas por el IRI a través de esfuerzos relacionados con el IIF y otras iniciativas.

Con estas herramientas, se demuestra que el conjunto de datos de LatAm es una fuente comparable a otros conjuntos de datos que se utilizan actualmente para productos de seguros índice operativos. Para más información, consulte el siguiente enlace: ftp://ftp.cptec.inpe.br/rainfall/cif.latam/output_americas/documents/20180501_LatAmDatasetValidation.docx.
- **¿Incluyen metadatos detallados y consistentes?**
  Si, los metadatos están incluidos en los archivos y documentación técnica.

### **3. Contenido de los Datos**
- **¿Qué variables están disponibles (temperatura, precipitación, viento, etc.)?**
precipitación 
- **¿Los datos incluyen mediciones históricas, simulaciones o ambos?**
  Historicas
- **¿Existen variables derivadas (índices climáticos, anomalías, etc.)?**
NA

### **4. Uso y Licencia**
- **¿Qué tipo de licencia tienen los datos (libre, restringida, comercial)?**
  Libre.
- **¿Existe un límite en la cantidad de datos que se pueden descargar?**
  No
- **¿Se requiere atribuir la fuente en caso de uso?**
  Si, es necesario citar
- **¿Es posible redistribuir los datos o integrarlos en nuevos productos?**
  Si

### **5. Automatización y Escalabilidad**
- **¿Los datos están disponibles a través de APIs?**
  No es claro, en teoria si
- **¿Existen restricciones en la velocidad o volumen de solicitudes para la descarga automatizada?**
  No
- **¿Qué herramientas o lenguajes son compatibles con el acceso a los datos (Python, R, etc.)?**
  R, python, julia

### **6. Contexto y la Institución**
- **¿Quién es la entidad responsable de los datos?**
https://satelite.cptec.inpe.br/latamdataset/
- **¿Cuál es la reputación de la fuente en términos de calidad y fiabilidad?**
  COnfiable.
- **¿Hay documentación técnica detallada para entender los datos?**
  Si, disponible en la misma página.

### **7. Usabilidad**
- **¿Es necesario transformar los datos para adaptarlos a los objetivos del proyecto?**
  Considero que los datos e infomación SON UTILES para el proyecto.
- **¿Qué tan fácil es integrar los datos con otras fuentes?**
  Es viable integralos con otras fuentes de información
- **¿Qué soporte técnico está disponible para resolver problemas?**
https://satelite.cptec.inpe.br/latamdataset/
  
## ECMWF
**Fuente de Datos: ECMWF (European Centre for Medium-Range Weather Forecasts)**

## **1. Disponibilidad y Acceso**

- **¿Tiene información de lluvias (precipitación), fuertes lluvias?**  
  Sí, proporciona datos detallados de precipitación acumulada y tasas de lluvia, a través de modelos como ERA5 y datos operacionales.

- **¿Tiene información de vientos, velocidad del viento, ráfagas de viento?**  
  Sí, ofrece información sobre la velocidad y dirección del viento, así como ráfagas, en distintos niveles de altitud.

- **¿Tiene información de temperaturas máximas, mínimas, promedio, datos de temperatura horaria?**  
  Sí, se incluyen temperaturas máximas, mínimas, promedio y datos horarios.

- **¿Tiene acceso libre?**  
  Algunos datos, como los de ERA5, están disponibles de forma gratuita para fines académicos y de investigación, pero otros conjuntos de datos pueden requerir licencia.

- **¿Desde cuándo hay datos?**  
  Los datos de ERA5 están disponibles desde 1950, y otros modelos pueden ofrecer datos desde la década de 1970.

- **¿Necesitamos un permiso especial?**  
  Para datos restringidos o comerciales, es necesario obtener una licencia específica.

- **¿Cómo se puede acceder a los datos?**  
  A través del servicio Copernicus Climate Data Store (CDS) o mediante APIs del ECMWF.

- **¿Podemos crear una automatización para la descarga de los datos?**  
  Sí, las APIs permiten la descarga automatizada de datos.

- **¿Cuál es la frecuencia de actualización de los datos?**  
  Los datos operativos se actualizan en tiempo real, mientras que los conjuntos como ERA5 tienen actualizaciones mensuales o anuales.

- **¿En qué formato están disponibles los datos?**  
  Los datos están disponibles en formatos como GRIB y NetCDF.

- **¿Existen restricciones geográficas en el acceso?**  
  No, los datos son globales y cubren todo el planeta.

---

## **2. Calidad y Estandarización**

- **¿Qué nivel de resolución espacial tienen los datos?**  
  Varía según el modelo; por ejemplo, ERA5 tiene una resolución de 31 km, y otros modelos operativos tienen resoluciones más altas.

- **¿Qué nivel de resolución temporal tienen los datos?**  
  Resolución horaria para la mayoría de los modelos como ERA5.

- **¿Los datos han sido sometidos a algún proceso de limpieza o validación?**  
  Sí, los datos de ECMWF son ampliamente validados antes de su distribución.

- **¿Incluyen metadatos detallados y consistentes?**  
  Sí, los conjuntos de datos incluyen metadatos completos y estándares reconocidos.

---

## **3. Contenido de los Datos**

- **¿Qué variables están disponibles (temperatura, precipitación, viento, etc.)?**  
  Variables como temperatura, precipitación, viento, humedad, presión, radiación solar, entre otras.

- **¿Los datos incluyen mediciones históricas, simulaciones o ambos?**  
  Incluyen ambos; mediciones históricas reanalizadas y simulaciones de modelos climáticos y meteorológicos.

- **¿Existen variables derivadas (índices climáticos, anomalías, etc.)?**  
  Sí, incluyen índices como el NAO, anomalías climáticas y otros indicadores derivados.

---

## **4. Uso y Licencia**

- **¿Qué tipo de licencia tienen los datos (libre, restringida, comercial)?**  
  Algunos datos son gratuitos bajo el programa Copernicus, mientras que otros requieren una licencia comercial.

- **¿Existe un límite en la cantidad de datos que se pueden descargar?**  
  Sí, existen límites en la cantidad de datos descargables por usuario, dependiendo del servicio.

- **¿Se requiere atribuir la fuente en caso de uso?**  
  Sí, es obligatorio atribuir a ECMWF o Copernicus en publicaciones y productos derivados.

- **¿Es posible redistribuir los datos o integrarlos en nuevos productos?**  
  La redistribución está sujeta a los términos de la licencia del dataset.

---

## **5. Automatización y Escalabilidad**

- **¿Los datos están disponibles a través de APIs?**  
  Sí, ECMWF proporciona APIs para el acceso y descarga de datos.

- **¿Existen restricciones en la velocidad o volumen de solicitudes para la descarga automatizada?**  
  Sí, hay límites en las solicitudes por hora o día, dependiendo del nivel de acceso del usuario.

- **¿Qué herramientas o lenguajes son compatibles con el acceso a los datos (Python, R, etc.)?**  
  Compatible con Python y otros lenguajes que soporten solicitudes HTTP. También hay bibliotecas específicas como `ecmwf-api-client`.

---

## **6. Contexto y la Institución**

- **¿Quién es la entidad responsable de los datos?**  
  ECMWF, en colaboración con el programa Copernicus y otras instituciones.

- **¿Cuál es la reputación de la fuente en términos de calidad y fiabilidad?**  
  Es una de las instituciones líderes a nivel mundial en predicción y reanálisis climático y meteorológico.

- **¿Hay documentación técnica detallada para entender los datos?**  
  Sí, la documentación técnica está disponible para todos los conjuntos de datos.

---

## **7. Usabilidad**

- **¿Es necesario transformar los datos para adaptarlos a los objetivos del proyecto?**  
  Puede ser necesario dependiendo del análisis; herramientas como Python y R facilitan la manipulación de los datos.

- **¿Qué tan fácil es integrar los datos con otras fuentes?**  
  Relativamente fácil, especialmente debido al uso de formatos estándar como GRIB y NetCDF.

- **¿Qué soporte técnico está disponible para resolver problemas?**  
  Soporte a través de documentación, foros de usuarios y contacto técnico directo para usuarios registrados.

## DNP
**Fuente de Datos:  Departamento Nacional de Planeación

### **1. Disponibilidad y Acceso**

- **¿Tiene información de lluvias (precipitación), fuertes lluvias?**
  Si, los datos cuentan con información de precipitaciones diarias.
  
- **¿Tiene información de vientos, velocidad del viento, rafagas de viento?**
  No.
  
- **¿Tiene información de temperaturas máximas, mínimas, promedio, datos de temperatura horaria?**
  Si, cuenta con histórico de temperatura máxima, promedio y mínima.
  
- **¿Tiene acceso libre?**
  Si
  
- **¿Desde cuándo hay datos?**
  Temperatura: 1981-2020
  Precipitaciones: 1981-2016
  
- **¿Necesitamos un permiso especial?**
  No
  
- **¿Cómo se puede acceder a los datos?**
  A través de DODS o en formato netCDF
  
- **¿Podemos crear una automatización para la descarga de los datos?**
  Si, a través de OPeNDAP se puede realizar la descarga directamente de software como Matlab o Python. A través de algunas librerías de python se puede leer a        través de netCDF
- **¿Cuál es la frecuencia de actualización de los datos?**
  Temperatura: Última Actualización: Wed, 19 Apr 2023 18:23:12 GMT
  Precipitación: Última Actualización: Thu, 20 Jan 2022 14:33:50 GMT

- **¿En qué formato están disponibles los datos?**
  DODS, netCDF
  Para algunos programas específicos: Ingrid, CPT, ferret, GrADS, matlab, NCL-NCAR, WinDisp

- **¿Existen restricciones geográficas en el acceso?**
  Al ser una base de datos consolidada las coordenadas donde se consolidan las series son:
Longitude (longitude)	grid: /X (degree_east) ordered (80W) to (65W) by 0.1 N= 151 pts :grid
Latitude (latitude)	grid: /Y (degree_north) ordered (5S) to (15N) by 0.1 N= 201 pts :grid


### **2. Calidad y Estandarización**
- **¿Qué nivel de resolución espacial tienen los datos?**
  0.1 x 0.1
- **¿Qué nivel de resolución temporal tienen los datos?**
  Diaria
- **¿Los datos han sido sometidos a algún proceso de limpieza o validación?**
  Las series están consolidadas de dos estaciones antes descritas, lo que indica un posible proceso de limpieza y validación 
- **¿Incluyen metadatos detallados y consistentes?**
  Si

### **3. Contenido de los Datos**
- **¿Qué variables están disponibles (temperatura, precipitación, viento, etc.)?**
  Temperatura: (Celsius_scale) [ Longitude Latitude Time ]
  Precipitación: (mm) [ Longitude Latitude Time ]

- **¿Los datos incluyen mediciones históricas, simulaciones o ambos?**
  No incluyen mediciones o simulacines
  
- **¿Existen variables derivadas (índices climáticos, anomalías, etc.)?**
  No se identifican variales derivadas

### **4. Uso y Licencia**
- **¿Qué tipo de licencia tienen los datos (libre, restringida, comercial)?**
  Libre
- **¿Existe un límite en la cantidad de datos que se pueden descargar?**
  En una version premilimar no se identifica un limite a la cantidad de datos a descargar 
- **¿Se requiere atribuir la fuente en caso de uso?**
  Es es una práctica estándar en el uso de datos provenientes de instituciones académicas o científicas, ya que ayuda a reconocer su trabajo y mantener la transparencia en el uso de los datos.
- **¿Es posible redistribuir los datos o integrarlos en nuevos productos?**
  No parece tener el soporte adecuado para ese fin

### **5. Automatización y Escalabilidad**
- **¿Los datos están disponibles a través de APIs?**
  No es necesaria una API para la descarga de los datos
- **¿Existen restricciones en la velocidad o volumen de solicitudes para la descarga automatizada?**
  En una revisión preliminar no parecen haber restricciones al volumen de descarga. No logro identificar si existen restricciones a la velocidad de descarga 
- **¿Qué herramientas o lenguajes son compatibles con el acceso a los datos (Python, R, etc.)?**
  Python, Matlab o cualquier lenguaje que soporte NetCDF.

### **6. Contexto y la Institución**
- **¿Quién es la entidad responsable de los datos?**
  Bases consolidadas por el IRI de las estaciones del IDEAM, CHIRPS Y MERRA2
- **¿Cuál es la reputación de la fuente en términos de calidad y fiabilidad?**
  El IRI hace parte del Earth Institute de la Universidad de Columbia y tiene una sólida reputación internacional como una de las principales instituciones en investigación climática y servicios climáticos. Trabaja estrechamente con organizaciones como la Organización Meteorológica Mundial (OMM), el Programa Mundial de Investigación del Clima (WCRP) o el Banco Mundial 
- **¿Hay documentación técnica detallada para entender los datos?**
  Cuenta con una descripción de los datos y un visualizador de la serie 

### **7. Usabilidad**
- **¿Es necesario transformar los datos para adaptarlos a los objetivos del proyecto?**
  En una revisión preliminar la información parece no necesitar ninguna transformación
- **¿Qué tan fácil es integrar los datos con otras fuentes?**
  La base de datos parece estar en formatos comunes, lo que permite integración con otras fuentes de datos
- **¿Qué soporte técnico está disponible para resolver problemas?**
  La biblioteca de datos cuenta con una sección de Ayuda y Recursos que incluye tutoriales básicos y estadísticos, la documentación de las funciones, y otros recursos que permiten explotar la Biblioteca de Datos.


## SIAC
**Fuente de Datos: : Sistema de Información Ambiental de Colombia - SIAC**

[SIAC - Link Datos Abiertos](https://siac-datosabiertos-mads.hub.arcgis.com/)

## **1. Disponibilidad y Acceso**

- **¿Tiene información de lluvias (precipitación), fuertes lluvias?**  
  Sí.

- **¿Tiene información de vientos, velocidad del viento, ráfagas de viento?**  
  No.


- **¿Necesitamos un permiso especial?**  
   No es necesario.



- **¿Podemos crear una automatización para la descarga de los datos?**  
  Considero que no es necesario. Pero si.

- **¿Cuál es la frecuencia de actualización de los datos?**  
No hay actualización.

- **¿En qué formato están disponibles los datos?**  
  Los datos están disponibles en formatos como CSV.

- **¿Existen restricciones geográficas en el acceso?**  
  No.
---

## **2. Calidad y Estandarización**

- **¿Qué nivel de resolución espacial tienen los datos?**  
 Solo en el departamento de Cundinamarca.

- **¿Qué nivel de resolución temporal tienen los datos?**  
  Anual.

- **¿Los datos han sido sometidos a algún proceso de limpieza o validación?**  
  Sí.

- **¿Incluyen metadatos detallados y consistentes?**  
  No.

---

## **3. Contenido de los Datos**


- **¿Existen variables derivadas (índices climáticos, anomalías, etc.)?**  
  No.

---

## **4. Uso y Licencia**

- **¿Qué tipo de licencia tienen los datos (libre, restringida, comercial)?**  
  Libre.

- **¿Existe un límite en la cantidad de datos que se pueden descargar?**  
  No.

- **¿Se requiere atribuir la fuente en caso de uso?**  
   Si.
  
- **¿Es posible redistribuir los datos o integrarlos en nuevos productos?**  
  Si.

---

## **5. Automatización y Escalabilidad**

- **¿Los datos están disponibles a través de APIs?**  
  Sí.

- **¿Existen restricciones en la velocidad o volumen de solicitudes para la descarga automatizada?**  
  No

- **¿Qué herramientas o lenguajes son compatibles con el acceso a los datos (Python, R, etc.)?**  
  Python, R, entre otros.

---

## **6. Contexto y la Institución**

- **¿Quién es la entidad responsable de los datos?**  
  SIAC.

- **¿Cuál es la reputación de la fuente en términos de calidad y fiabilidad?**  
  Confiable.

- **¿Hay documentación técnica detallada para entender los datos?**  
  Hay documentación técnica.

---

## **7. Usabilidad**

- **¿Es necesario transformar los datos para adaptarlos a los objetivos del proyecto?**  
  Considero que esta página no es útil para el proyecto, su información es limitada en cuanto a espacio y tiempo. Además, no hay actualizaciones. Sin embargo, es útil para ver otras bases de datos de otras organizaciones.

- **¿Qué tan fácil es integrar los datos con otras fuentes?**  
  Relativamente fácil debido a su formato estándar y metadatos consistentes.

- **¿Qué soporte técnico está disponible para resolver problemas?**  
  Por correo a sigsiac.tic@minambiente.gov.co
  

## ANLA


## 1. Disponibilidad y Acceso
- **¿Tiene información de lluvias (precipitación), fuertes lluvias?**  
  Sí, la ANLA dispone de información sobre precipitaciones como parte de monitoreos climáticos en proyectos de evaluación ambiental, aunque puede estar enfocada en áreas específicas bajo licenciamiento ambiental.

- **¿Tiene información de vientos, velocidad del viento, ráfagas de viento?**  
  La información de vientos puede estar disponible en estudios ambientales de proyectos con requerimientos específicos, pero no es un dato de acceso estándar como en meteorología.

- **¿Tiene información de temperaturas máximas, mínimas, promedio, datos de temperatura horaria?**  
  Los datos de temperatura pueden estar disponibles en los informes de impacto ambiental asociados con licencias específicas, aunque no son centralizados ni de alta resolución.

- **¿Tiene acceso libre?**  
  No, los datos no son de acceso completamente libre; algunos están en informes públicos, pero otros requieren permisos o solicitudes específicas.

- **¿Desde cuándo hay datos?**  
  Los datos pueden variar según el proyecto. Generalmente, se recopilan desde el inicio del proceso de evaluación ambiental.

- **¿Necesitamos un permiso especial?**  
  Sí, para acceder a información específica o confidencial relacionada con proyectos licenciados.

- **¿Cómo se puede acceder a los datos?**  
  A través de solicitudes formales a la ANLA o por medio de informes públicos publicados en su sitio web.

- **¿Podemos crear una automatización para la descarga de los datos?**  
  No, ya que los datos no suelen estar en APIs ni en plataformas diseñadas para descargas masivas.

- **¿Cuál es la frecuencia de actualización de los datos?**  
  Depende del proyecto y de la periodicidad de los reportes asociados (mensual, anual, etc.).

- **¿En qué formato están disponibles los datos?**  
  PDF para informes, ocasionalmente en tablas de Excel o formatos geográficos como shapefiles.

- **¿Existen restricciones geográficas en el acceso?**  
  Los datos suelen estar restringidos al área de estudio de los proyectos licenciados.

---

## 2. Calidad y Estandarización
- **¿Qué nivel de resolución espacial tienen los datos?**  
  Resolución específica al área de impacto de los proyectos. Puede ser alta a nivel local, pero no a nivel nacional.

- **¿Qué nivel de resolución temporal tienen los datos?**  
  Resolución baja (mensual, anual o por evento). No se ofrecen datos en tiempo real.

- **¿Los datos han sido sometidos a algún proceso de limpieza o validación?**  
  Sí, los datos se validan en el contexto de estudios de impacto ambiental.

- **¿Incluyen metadatos detallados y consistentes?**  
  Sí, en los informes técnicos se incluyen descripciones metodológicas y metadatos.

---

## 3. Contenido de los Datos
- **¿Qué variables están disponibles?**  
  Variables climáticas (precipitación, temperatura, viento), calidad del aire, calidad del agua, entre otras relacionadas con impactos ambientales.

- **¿Los datos incluyen mediciones históricas, simulaciones o ambos?**  
  Predominantemente mediciones históricas recopiladas durante el proceso de evaluación.

- **¿Existen variables derivadas?**  
  Sí, como índices de impacto ambiental o cálculos de capacidad de carga ambiental.

---

## 4. Uso y Licencia
- **¿Qué tipo de licencia tienen los datos?**  
  Los datos están bajo licencias restringidas. Parte de ellos son de dominio público, pero otros son confidenciales.

- **¿Existe un límite en la cantidad de datos que se pueden descargar?**  
  Sí, según las restricciones impuestas por la ANLA.

- **¿Se requiere atribuir la fuente en caso de uso?**  
  Sí, la ANLA debe ser citada como fuente.

- **¿Es posible redistribuir los datos o integrarlos en nuevos productos?**  
  No sin autorización expresa.

---

## 5. Automatización y Escalabilidad
- **¿Los datos están disponibles a través de APIs?**  
  No, la ANLA no ofrece APIs públicas.

- **¿Existen restricciones en la velocidad o volumen de solicitudes?**  
  Sí, ya que el acceso no es automatizado.

- **¿Qué herramientas o lenguajes son compatibles con el acceso a los datos?**  
  PDF y Excel pueden analizarse con Python o R tras una conversión manual.

---

## 6. Contexto y la Institución
- **¿Quién es la entidad responsable de los datos?**  
  La Autoridad Nacional de Licencias Ambientales (ANLA).

- **¿Cuál es la reputación de la fuente en términos de calidad y fiabilidad?**  
  Alta, ya que cumple estándares técnicos rigurosos.

- **¿Hay documentación técnica detallada para entender los datos?**  
  Sí, en los informes asociados a los proyectos licenciados.

---

## 7. Usabilidad
- **¿Es necesario transformar los datos para adaptarlos a los objetivos del proyecto?**  
  Frecuentemente, sí, ya que no están en formatos listos para análisis inmediato.

- **¿Qué tan fácil es integrar los datos con otras fuentes?**  
  Moderadamente fácil con herramientas de procesamiento de datos.

- **¿Qué soporte técnico está disponible para resolver problemas?**  
  Limitado, depende de consultas formales a la ANLA.




[Geovisores y Mapas ANLA](https://www.anla.gov.co/eureka/geovisores-y-mapas)

- **¿Cómo se puede acceder a los datos?**  
  A través de la página de datos abiertos.


## INMET

**Análisis de Datos Climáticos - INMET (Instituto Nacional de Meteorología, Brasil)**

---

#### **1. Disponibilidad y Acceso**
- **¿Tiene información de lluvias (precipitación), fuertes lluvias?**  
  Sí.  
- **¿Tiene información de vientos, velocidad del viento, ráfagas de viento?**  
  Sí.  
- **¿Tiene información de temperaturas máximas, mínimas, promedio, datos de temperatura horaria?**  
  Sí.  
- **¿Tiene acceso libre?**  
  Sí, requiere registro.  
- **¿Desde cuándo hay datos?**  
  Desde 1961, pero no tiene datos en colombia.  
- **¿Necesitamos un permiso especial?**  
  No, solo registro.  
- **¿Cómo se puede acceder a los datos?**  
  A través de su sitio web o API.  
- **¿Podemos crear una automatización para la descarga de los datos?**  
  Sí.  
- **¿Cuál es la frecuencia de actualización de los datos?**  
  Diaria.  
- **¿En qué formato están disponibles los datos?**  
  CSV, TXT.  
- **¿Existen restricciones geográficas en el acceso?**  
  No.  

---

#### **2. Calidad y Estandarización**
- **¿Qué nivel de resolución espacial tienen los datos?**  
  Estaciones meteorológicas.  
- **¿Qué nivel de resolución temporal tienen los datos?**  
  Diaria y horaria.  
- **¿Los datos han sido sometidos a algún proceso de limpieza o validación?**  
  Sí.  
- **¿Incluyen metadatos detallados y consistentes?**  
  Sí.  

---

#### **3. Contenido de los Datos**
- **¿Qué variables están disponibles (temperatura, precipitación, viento, etc.)?**  
  - Temperatura (máxima, mínima, promedio).  
  - Precipitación.  
  - Velocidad del viento.  
  - Humedad relativa.  
- **¿Los datos incluyen mediciones históricas, simulaciones o ambos?**  
  Mediciones históricas.  
- **¿Existen variables derivadas (índices climáticos, anomalías, etc.)?**  
  No.  

---

#### **4. Uso y Licencia**
- **¿Qué tipo de licencia tienen los datos (libre, restringida, comercial)?**  
  Libre, con registro.  
- **¿Existe un límite en la cantidad de datos que se pueden descargar?**  
  No.  
- **¿Se requiere atribuir la fuente en caso de uso?**  
  Sí.  
- **¿Es posible redistribuir los datos o integrarlos en nuevos productos?**  
  Sí.  

---

#### **5. Automatización y Escalabilidad**
- **¿Los datos están disponibles a través de APIs?**  
  Sí.  
- **¿Existen restricciones en la velocidad o volumen de solicitudes para la descarga automatizada?**  
  No.



[INMET](https://portal.inmet.gov.br/).


## Power NASA

## **1. Disponibilidad y Acceso**

- **¿Tiene información de lluvias (precipitación), fuertes lluvias?**  
  Sí.
- **¿Tiene información de vientos, velocidad del viento, ráfagas de viento?**  
  Sí.
- **¿Tiene información de temperaturas máximas, mínimas, promedio, datos de temperatura horaria?**  
  Sí.
- **¿Tiene acceso libre?**  
  Sí.
- **¿Desde cuándo hay datos?**  
  Desde 1981.
- **¿Necesitamos un permiso especial?**  
  No.
- **¿Cómo se puede acceder a los datos?**  
  Descargas en formato CSV o acceso mediante JSON.
- **¿Podemos crear una automatización para la descarga de los datos?**  
  Buscando alternativa para automatizar.
- **¿Cuál es la frecuencia de actualización de los datos?**  
  Diaria.
- **¿En qué formato están disponibles los datos?**  
  CSV, ASCII, JSON.
- **¿Existen restricciones geográficas en el acceso?**  
  Sí, solo se pueden tomar regiones de tamaño 10° por archivo CSV.

---

## **2. Calidad y Estandarización**

- **¿Qué nivel de resolución espacial tienen los datos?**  
  A escala global con resolución adaptativa.
- **¿Qué nivel de resolución temporal tienen los datos?**  
  Datos desde 1981 hasta la actualidad.
- **¿Los datos han sido sometidos a algún proceso de limpieza o validación?**  
  Sí.
- **¿Incluyen metadatos detallados y consistentes?**  
  Sí.

---

## **3. Contenido de los Datos**

- **¿Qué variables están disponibles (temperatura, precipitación, viento, etc.)?**  
  Contiene las cinco variables del índice del clima actuarial mencionadas en el PDF.  
- **¿Los datos incluyen mediciones históricas, simulaciones o ambos?**  
  Ambos.  
- **¿Existen variables derivadas (índices climáticos, anomalías, etc.)?**  
  No.

---

## **4. Uso y Licencia**

- **¿Qué tipo de licencia tienen los datos (libre, restringida, comercial)?**  
  Libre.  
- **¿Existe un límite en la cantidad de datos que se pueden descargar?**  
  Cada archivo CSV permite descargar por periodos de un año; sin embargo, no hay límite de archivos descargables.  
- **¿Se requiere atribuir la fuente en caso de uso?**  
  Sí.  
- **¿Es posible redistribuir los datos o integrarlos en nuevos productos?**  
  Sí, cumpliendo las condiciones establecidas por la NASA. Los datos son de dominio público, pero su redistribución debe respetar los términos de uso.

---

## **5. Automatización y Escalabilidad**

- **¿Los datos están disponibles a través de APIs?**  
  Sí. [API de NASA POWER](https://power.larc.nasa.gov/api/temporal/daily/point?parameters=T2M&community=AG&longitude=-77.0369&latitude=38.9072&start=20230101&end=20231231&format=JSON)  
- **¿Existen restricciones en la velocidad o volumen de solicitudes para la descarga automatizada?**  
  - **Límite de solicitudes por minuto/hora:** Entre 60 y 120 solicitudes por minuto.  
  - **Volumen de datos:** Máximo de 366 días por solicitud.  
  - **Uso justo:** El servicio está diseñado para fines científicos y educativos; solicitudes masivas podrían restringirse.  
- **¿Qué herramientas o lenguajes son compatibles con el acceso a los datos (Python, R, etc.)?**  
  Python, R, MATLAB.

---

## **6. Contexto y la Institución**

- **¿Quién es la entidad responsable de los datos?**  
  La NASA, específicamente a través de la herramienta NASA POWER del Langley Research Center (LaRC).  
- **¿Cuál es la reputación de la fuente en términos de calidad y fiabilidad?**  
  La NASA tiene una reputación mundialmente reconocida por la calidad y fiabilidad de sus datos. Los datos de NASA POWER son validados mediante estaciones meteorológicas y modelos precisos.  
- **¿Hay documentación técnica detallada para entender los datos?**  
  Sí, incluye metodología, variables y ejemplos prácticos.

---

## **7. Usabilidad**

- **¿Es necesario transformar los datos para adaptarlos a los objetivos del proyecto?**  
  Los datos están en formatos estándar como CSV o JSON, adaptables a diversas herramientas.  
- **¿Qué tan fácil es integrar los datos con otras fuentes?**  
  - **Formatos estándar:** Compatibles con Python, R, MATLAB, Power BI y Tableau.  
  - **API accesible:** Permite integrar datos en flujos de trabajo automatizados.  
- **¿Qué soporte técnico está disponible para resolver problemas?**  
  - **Documentación detallada:** Con ejemplos y guías.  
  - **Correo de soporte:** [power@larc.nasa.gov](mailto:power@larc.nasa.gov).  
  - **FAQ:** Página oficial con preguntas frecuentes.  

---

## **Power NASA**  
**[https://power.larc.nasa.gov/](https://power.larc.nasa.gov/)**


- **¿Qué variables están disponibles (temperatura, precipitación, viento, etc.)?**  
  Están disponibles variables como temperatura, precipitación.

- **¿Los datos incluyen mediciones históricas, simulaciones o ambos?**  
  No.

---

## UNGRD

### **1. Disponibilidad y Acceso**

- **¿Tiene información de lluvias (precipitación), fuertes lluvias?**  
  No.  
- **¿Tiene información de vientos, velocidad del viento, ráfagas de viento?**  
  No.  
- **¿Tiene información de temperaturas máximas, mínimas, promedio, datos de temperatura horaria?**  
  No.  
- **¿Tiene acceso libre?**  
  Sí.  
- **¿Desde cuándo hay datos?**  
  1980.  
- **¿Necesitamos un permiso especial?**  
  No.  
- **¿Cómo se puede acceder a los datos?**  
  En datos abiertos gov o directamente desde su página web.  
- **¿Podemos crear una automatización para la descarga de los datos?**  
  No.  
- **¿Cuál es la frecuencia de actualización de los datos?**  
  Anual.  
- **¿En qué formato están disponibles los datos?**  
  CSV.  
- **¿Existen restricciones geográficas en el acceso?**  
  No.  

---

### **2. Calidad y Estandarización**

- **¿Qué nivel de resolución espacial tienen los datos?**  
  No hay.  
- **¿Qué nivel de resolución temporal tienen los datos?**  
  No especificado.  
- **¿Los datos han sido sometidos a algún proceso de limpieza o validación?**  
  No.  
- **¿Incluyen metadatos detallados y consistentes?**  
  Sí.  

---

### **3. Contenido de los Datos**

- **¿Qué variables están disponibles (temperatura, precipitación, viento, etc.)?**  
  Fecha, tipo de catástrofe y lugar.  
- **¿Los datos incluyen mediciones históricas, simulaciones o ambos?**  
  Ninguna.  
- **¿Existen variables derivadas (índices climáticos, anomalías, etc.)?**  
  No.  

---

### **4. Uso y Licencia**

- **¿Qué tipo de licencia tienen los datos (libre, restringida, comercial)?**  
  Libre.  
- **¿Existe un límite en la cantidad de datos que se pueden descargar?**  
  No.  
- **¿Se requiere atribuir la fuente en caso de uso?**  
  Sí.  
- **¿Es posible redistribuir los datos o integrarlos en nuevos productos?**  
  Sí.  

---

### **5. Automatización y Escalabilidad**

- **¿Los datos están disponibles a través de APIs?**  
  Sí.  
- **¿Existen restricciones en la velocidad o volumen de solicitudes para la descarga automatizada?**  
  No.  
- **¿Qué herramientas o lenguajes son compatibles con el acceso a los datos (Python, R, etc.)?**  
  Python, R.  

---

### **6. Contexto y la Institución**

- **¿Quién es la entidad responsable de los datos?**  
  La Unidad Nacional para la Gestión del Riesgo de Desastres (UNGRD) en Colombia.  
- **¿Cuál es la reputación de la fuente en términos de calidad y fiabilidad?**  
  Buena.  
- **¿Hay documentación técnica detallada para entender los datos?**  
  No.  

---

### **7. Usabilidad**

- **¿Es necesario transformar los datos para adaptarlos a los objetivos del proyecto?**  
  Sí.  
- **¿Qué tan fácil es integrar los datos con otras fuentes?**  
  Complicado.  
- **¿Qué soporte técnico está disponible para resolver problemas?**  
  No especificado.  

---

## **[Repositorio UNGRD](https://repositorio.gestiondelriesgo.gov.co/handle/20.500.11762/27179)**

[Geoportal SGC](https://www2.sgc.gov.co/sgc/mapas/Paginas/geoportal.aspx)


---

[Geoportal SGC](https://www2.sgc.gov.co/sgc/mapas/Paginas/geoportal.aspx)

---

## **UNESCO**

[SEA LEVEL STATION MONITORING FACILITY](https://www.ioc-sealevelmonitoring.org/list.php?showall=a&output=performance&order=country&dir=asc)

### **1. Disponibilidad y Acceso**

Esta base de datos contiene información del nivel del mar. Está por niveles relativos y niveles absolutos. La base es de acceso libre sin restricción geográfica. La actualización de estos dependen de la estación (zona donde se tome la estación). La información temporal depende de la estación: la más antigua contiene datos del 2010 (pacífico colombiano). No hay restricciones geográficas para acceder a la base de datos. No se ve una forma de descargar los datos en un formato especial, pero se podrían extraer a través del código fuente.

### **2. Calidad y Estandarización**

Para Colombia, hay un total de 19 estaciones distribuidas en zonas costeras del país: Buenaventura, San Andrés, Tumaco, Cartagena, Coveñas, Turbo, Malpelo, entre otras. Los datos se pueden descargar por minuto. Si se hacen validaciones.


### **3. Contenido de los Datos**

Esta disponible la información a nivel del mar. No hay simulaciones, ni mediciones históricas, ni tampoco variables derivadas.


### **4. Uso y Licencia**
Es de uso libre, no hay limite para la descarga de datos, se requiere citarlos, y es posible integrarlos a nuevos productos.

### **5. Automatización y Escalabilidad**
Es posible descargar y automatizar la descarga o consulta de estos datos. Las velocidades o volumen dependen del equipo. Se podría automatizar la extracción de información en Python.

### **6. Contexto y la Institución**
La entidad es la UNESCO junto con la Intergovernmental Oceanographic Commission. Los datos son confiables, pues son datos proporcionados por distintos institudos gubernamentales del mundo. No hay mucha documentación técnica para entender los datos.


### **7. Usabilidad**
Si sería necesario adaptarlos, y sería fácil integrarlos con otras fuentes, dependiendo de como se desarrolle el proyecto. Para resolver problemas técnicos, por correo a la entidad.

---

# **PSMSL**

A continuación, se detallan las respuestas sobre la disponibilidad y acceso a los datos en cada una de las estaciones, con base en la selección de la **PSMSL** como fuente de información para el nivel del mar.

## **1. Disponibilidad y Acceso por Estación**

### **Estación: PSMSL (Nivel del Mar)**

- **¿Tiene información de lluvias (precipitación), fuertes lluvias?**  
   No. La PSMSL solo proporciona datos de nivel del mar.  

- **¿Tiene información de vientos, velocidad del viento, ráfagas de viento?**  
   No. La PSMSL no recopila datos de viento.  

- **¿Tiene información de temperaturas máximas, mínimas, promedio, datos de temperatura horaria?**  
   No. Solo se enfoca en datos de nivel del mar.  

- **¿Tiene acceso libre?**  
   Sí.  

- **¿Desde cuándo hay datos?**  
   La PSMSL tiene registros históricos desde principios del siglo XX hasta la actualidad, dependiendo de la estación.  

- **¿Necesitamos un permiso especial?**  
   No.  

- **¿Cómo se puede acceder a los datos?**  
   A través del portal oficial de la PSMSL: [https://www.psmsl.org/](https://www.psmsl.org/)  

- **¿Podemos crear una automatización para la descarga de los datos?**  
  Sí, los datos pueden descargarse automáticamente mediante scripts en Python o R.  

- **¿Cuál es la frecuencia de actualización de los datos?**  
  La actualización depende de la estación, generalmente mensual o anual.  

- **¿En qué formato están disponibles los datos?**  
  CSV y TXT.  

- **¿Existen restricciones geográficas en el acceso?**  
  No, la información es pública y global.  

## **2. Calidad y Estandarización**

- **¿Qué nivel de resolución espacial tienen los datos?**  
  Depende de la estación, generalmente datos de nivel del mar en puntos específicos.  

- **¿Qué nivel de resolución temporal tienen los datos?**  
  Mensual o anual.  

- **¿Los datos han sido sometidos a algún proceso de limpieza o validación?**  
  Sí, la PSMSL realiza controles de calidad en los datos.  

- **¿Incluyen metadatos detallados y consistentes?**  
  Sí, cada estación tiene metadatos específicos disponibles.  

## **3. Contenido de los Datos**

- **¿Qué variables están disponibles (temperatura, precipitación, viento, etc.)?**  
  Solo nivel del mar.  

- **¿Los datos incluyen mediciones históricas, simulaciones o ambos?**  
  Solo mediciones históricas.  

- **¿Existen variables derivadas (índices climáticos, anomalías, etc.)?**  
  Sí, algunos análisis incluyen anomalías del nivel del mar.  

## **4. Uso y Licencia**

- **¿Qué tipo de licencia tienen los datos (libre, restringida, comercial)?**  
  Libre y de acceso público.  

- **¿Existe un límite en la cantidad de datos que se pueden descargar?**  
  No.  

- **¿Se requiere atribuir la fuente en caso de uso?**  
  Sí, se recomienda citar la PSMSL.  

- **¿Es posible redistribuir los datos o integrarlos en nuevos productos?**  
  Sí, con la debida atribución.  

## **5. Automatización y Escalabilidad**

- **¿Los datos están disponibles a través de APIs?**  
  No, pero se pueden descargar automáticamente desde su portal.  

- **¿Existen restricciones en la velocidad o volumen de solicitudes para la descarga automatizada?**  
  No.  

- **¿Qué herramientas o lenguajes son compatibles con el acceso a los datos (Python, R, etc.)?**  
  Python, R, y cualquier lenguaje que pueda leer CSV o TXT.  

## **6. Contexto y la Institución**

- **¿Quién es la entidad responsable de los datos?**  
  PSMSL (Permanent Service for Mean Sea Level).  

- **¿Cuál es la reputación de la fuente en términos de calidad y fiabilidad?**  
  Alta, es una de las principales referencias en nivel del mar.  

- **¿Hay documentación técnica detallada para entender los datos?**  
  Sí, disponible en su sitio web.  

## **7. Usabilidad**

- **¿Es necesario transformar los datos para adaptarlos a los objetivos del proyecto?**  
  Depende del uso, pueden requerir normalización o filtrado.  

- **¿Qué tan fácil es integrar los datos con otras fuentes?**  
  Relativamente fácil, ya que están en formatos estándar.  



______________________________________
## **Fuente de Datos:  [Nombre de la Fuente]**

### **1. Disponibilidad y Acceso**

- **¿Tiene información de lluvias (precipitación), fuertes lluvias?** 
- **¿Tiene información de vientos, velocidad del viento, rafagas de viento?** 
- **¿Tiene información de temperaturas máximas, mínimas, promedio, datos de temperatura horaria?** 
- **¿Tiene acceso libre?** 
- **¿Desde cuándo hay datos?** 
- **¿Necesitamos un permiso especial?** 
- **¿Cómo se puede acceder a los datos?** 
- **¿Podemos crear una automatización para la descarga de los datos?** 
- **¿Cuál es la frecuencia de actualización de los datos?** 
- **¿En qué formato están disponibles los datos?** 
- **¿Existen restricciones geográficas en el acceso?**

### **2. Calidad y Estandarización**
- **¿Qué nivel de resolución espacial tienen los datos?** 
- **¿Qué nivel de resolución temporal tienen los datos?** 
- **¿Los datos han sido sometidos a algún proceso de limpieza o validación?** 
- **¿Incluyen metadatos detallados y consistentes?**

### **3. Contenido de los Datos**
- **¿Qué variables están disponibles (temperatura, precipitación, viento, etc.)?** 
- **¿Los datos incluyen mediciones históricas, simulaciones o ambos?** 
- **¿Existen variables derivadas (índices climáticos, anomalías, etc.)?**

### **4. Uso y Licencia**
- **¿Qué tipo de licencia tienen los datos (libre, restringida, comercial)?** 
- **¿Existe un límite en la cantidad de datos que se pueden descargar?** 
- **¿Se requiere atribuir la fuente en caso de uso?** 
- **¿Es posible redistribuir los datos o integrarlos en nuevos productos?**

### **5. Automatización y Escalabilidad**
- **¿Los datos están disponibles a través de APIs?** 
- **¿Existen restricciones en la velocidad o volumen de solicitudes para la descarga automatizada?** 
- **¿Qué herramientas o lenguajes son compatibles con el acceso a los datos (Python, R, etc.)?**

### **6. Contexto y la Institución**
- **¿Quién es la entidad responsable de los datos?** 
- **¿Cuál es la reputación de la fuente en términos de calidad y fiabilidad?** 
- **¿Hay documentación técnica detallada para entender los datos?**

### **7. Usabilidad**
- **¿Es necesario transformar los datos para adaptarlos a los objetivos del proyecto?** 
- **¿Qué tan fácil es integrar los datos con otras fuentes?** 

