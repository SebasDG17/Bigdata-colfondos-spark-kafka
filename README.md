# Análisis Big Data del portafolio de inversiones de Colfondos

Proyecto del curso **Big Data** (UNAD) que analiza datos abiertos de inversiones de fondos de pensiones y cesantías de Colombia, combinando procesamiento **batch** con Spark y procesamiento en **tiempo real** con Kafka y Spark Streaming.

## 1. Problema
¿Cómo se distribuye el portafolio de inversión por tipo de inversión y moneda, y cuánto valor ha ganado o perdido frente a su costo de compra?

## 2. Dataset
- **Fuente:** datos.gov.co (conjunto `nug7-ugd3`)
- **Enlace:** https://www.datos.gov.co/api/v3/views/nug7-ugd3/query.csv
- **Tamaño:** 8.437 registros y 116 columnas
- **Entidad:** COLFONDOS S.A. Pensiones y Cesantías

## 3. Arquitectura
| Componente | Uso |
|---|---|
| Hadoop (HDFS) | Almacenamiento del dataset |
| Apache Spark (PySpark) | Procesamiento batch |
| Apache Kafka | Simulación de datos en tiempo real |
| Spark Streaming | Consumo y análisis del flujo |
| Ubuntu en VirtualBox + PuTTY | Entorno de trabajo |

## 4. Estructura del repositorio
- `codigo/`: scripts de Python (batch y streaming)
- `imagenes/`: capturas con la evidencia de cada paso

## 5. Paso a Paso 
## 1. En este paso se evidencia la descarga del dataset desde datos.gov.co con 'wget'

<img width="823" height="522" alt="Dataset wget " src="https://github.com/user-attachments/assets/84d60574-4abf-4ce1-9546-3266f40c330e" />

## 2. Se creó la carpeta `/Tarea4` en HDFS y se copió allí el dataset con `hdfs dfs -put`. El listado confirma que `dataset.csv` quedó cargado (10.594.147 bytes).

<img width="832" height="70" alt="dataset HDFS" src="https://github.com/user-attachments/assets/24ec220b-31e7-4842-94f6-f1c68c410dd0" />

### 3. Lectura inicial del dataset con Spark (batch)
Se ejecutó `batch1.py` con PySpark para leer el CSV directamente desde HDFS. Spark cargó **8.437 registros y 116 columnas**. La salida también muestra el esquema de las columnas clave (nombre de la entidad, clase de inversión, moneda, valor de compra y valor de mercado, ambos de tipo `double`) y las primeras 10 filas, que corresponden a títulos de COLFONDOS en distintas monedas (euro, peso mexicano y peso colombiano).

<img width="1121" height="871" alt="limpieza de datos py1 " src="https://github.com/user-attachments/assets/aaa4dc31-2114-4b70-a892-73a722e9676d" />

### 4. Limpieza, transformación y análisis exploratorio (batch)
Se ejecutó `batch2.py` con PySpark sobre el dataset almacenado en HDFS. El script eliminó duplicados, nulos y valores no válidos, y pasó de **8.437 a 8.372 registros**. Luego creó la columna `variacion_pesos` (valor de mercado menos valor de compra) y calculó estadísticas básicas: en promedio, cada título tiene un valor de compra de unos 5.342 millones de pesos y un valor de mercado de unos 5.672 millones, es decir, una valorización media cercana a **330 millones de pesos por título**. También se agrupó el valor de mercado por clase de inversión: los **TES UVR** lideran con 10.308,9 miles de millones de pesos (568 títulos), seguidos por los fondos que replican índices accionarios (8.883,4) y los fondos de capital privado en el exterior (5.464,2).

<img width="925" height="953" alt="limpieza bach2" src="https://github.com/user-attachments/assets/3fc4926d-4428-47ef-87d2-407b68cdefbc" />

<img width="927" height="948" alt="limpieza bach 22" src="https://github.com/user-attachments/assets/a4597b33-37fc-45af-a50f-8d8a99f4ff32" />

### 5. Resultados del análisis batch: moneda, entidad y visualización
La segunda parte de la salida de `batch2.py` resume la composición del portafolio. Por **moneda**, de 8.372 títulos limpios, 5.966 están en pesos colombianos (71 %), 1.527 en dólares americanos, 603 en UVR y 212 en euros; el resto se reparte entre libra británica, peso mexicano, yen y otras monedas. Por **entidad**, el dataset contiene únicamente a COLFONDOS, con un valor de mercado total de 47.487,7 miles de millones de pesos. Por último, el script genera una gráfica de barras en consola que compara el valor de mercado por clase de inversión, y la misma información se presenta en una gráfica de columnas más clara.

<img width="911" height="265" alt="grafica " src="https://github.com/user-attachments/assets/ae9c7789-38be-4704-a054-085031d3dbca" />

<img width="912" height="433" alt="grafica valores" src="https://github.com/user-attachments/assets/5a2fe9f2-7154-4f84-a89f-ea033ba55f15" />

<img width="907" height="441" alt="mercado " src="https://github.com/user-attachments/assets/4ee58e7f-6bd1-4a34-a952-5d920b3656c9" />

### 6. Procesamiento en tiempo real con Kafka y Spark Streaming
Con Kafka y ZooKeeper activos y el topic `inversiones` creado, se ejecutaron dos procesos al mismo tiempo. A la derecha, el **productor** (`inv_producer.py`) envía un evento por segundo con la clase de inversión, la moneda y el valor de mercado (datos simulados a partir de las categorías reales del dataset). A la izquierda, el **consumidor** (`inv_consumer.py`) con Spark Streaming lee el topic en micro-lotes, agrupa los eventos en **ventanas de 1 minuto por moneda** y calcula el número de eventos y el valor de mercado promedio en miles de millones de pesos. La captura corresponde al **Batch 48**, y se ve cómo la tabla se actualiza continuamente a medida que llegan nuevos datos.

<img width="1802" height="761" alt="bach " src="https://github.com/user-attachments/assets/a19d92df-901e-4887-9d8a-6c705130b48d" />

