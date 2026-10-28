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
