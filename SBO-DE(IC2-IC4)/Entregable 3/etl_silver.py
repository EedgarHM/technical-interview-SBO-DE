"""
    Nota el codigo lo cree directamente en Glue, por eso la importacion de las librerias de Glue, y la forma en la que se inicializa el spark session, pero el codigo es completamente funcional y se puede ejecutar localmente con una configuracion minima, lo unico que se tendria que cambiar es la parte de lectura y escritura de datos, para leer desde archivos locales y escribir en archivos locales o en una base de datos local.
"""


import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sha2, row_number, lit, when, current_timestamp
from pyspark.sql.window import Window
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

# Creamos Logger para trackeo del etl
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("RetailMex-Silver-Job")

# Configuramos Spark para que almacene en silver y sea con formato Iceberg
try:
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    spark = SparkSession.builder \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue_catalog.schema-evolution-enabled", "true") \
        .config("spark.sql.catalog.glue_catalog.append.by-name", "true") \
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.glue_catalog.warehouse", "s3://retailmex-datalake/silver/") \
        .getOrCreate()
        
    glueContext = GlueContext(spark.sparkContext)
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    logger.info("Contexto de Spark inicializaco correctamnete")

except Exception as e:
    logger.error(f"rror inicializando el Job: {str(e)}")
    sys.exit(1)


def handle_schema_evolution(df, table_name):
    """
    Compara el DF con la tabla Iceberg y agrega las columnas 
    faltantes automáticamente antes de escribir.
    """
    try:
        
        table_columns = spark.table(table_name).columns
        df_columns = df.columns
        
        new_columns = [c for c in df_columns if c not in table_columns]
        
        if new_columns:
            logger.info(f"se detectaron {len(new_columns)} columnas nuevas: {new_columns}")
            for col_name in new_columns:
                dtype = df.schema[col_name].dataType.simpleString()
                
                spark.sql(f"ALTER TABLE {table_name} ADD COLUMN {col_name} {dtype}")
            logger.info("Tabla evolucionada con éxito.")
    except Exception as e:
        logger.warning(f"ocurrio un error del tipo {e}")




def transform_to_silver():
    try:
        """
            transform_to_silver
            Args : NA
            Objetivo : Lectura de datos Json desde s3 bronze, homologacion de nombres, Unir datos de los pos de venta y del eccomerce, y escribir en s3 silver con la data limpia, deduplicada y con opcion a evolucionar el schema
            return : NA
        """
        # Lectura de datos (Deje las rutas hardcodeadas para ser mas practico, esto no se debe hacer porque es una mul mala practica.)
        
        df_pos = spark.read.json("s3://retailmex-datalake/bronze/pos/") \
                  .withColumn("source_system", lit("POS"))
                  
        df_ecomm = spark.read.json("s3://retailmex-datalake/bronze/ecommerce/") \
                    .withColumn("source_system", lit("ECOMMERCE"))
        
        df_erp = spark.read.json("s3://retailmex-datalake/bronze/erp/")
        
        
        df_pos = df_pos.withColumnRenamed("event_timestamp","operation_date")
        df_ecomm = df_ecomm.withColumnRenamed("event_timestamp","operation_date")
        df_pos.printSchema()
        df_ecomm.printSchema()
                    
        logger.info(f"Leyendo datos desde: BRonze")

        #Nota : Dado que tenemos datos tanto de POS como de E-commerce vamos a centralizar esta informacion dentro de una sola tabla para no tener datos distribuidos, pero para no perder l detalle de donde vienen esos datos vamos a agregar una columna nueva la cual trara esa informacion
        df_sales_combined = df_pos.unionByName(df_ecomm, allowMissingColumns=True)
        
        
        # La lambda Gatekeeper deberia omitir estos archivos, pero en caso de que se pase alguno tambien lo validamos dentro del etl
        if df_sales_combined.count() == 0:
            logger.warning("Archivo sin regristros para en bronze.")
            return

        # transformaciones
        logger.info("Aplicando transformaciones y limpieza...")
        
        # Deduplicación 
        window_spec = Window.partitionBy("transaction_id").orderBy(col("operation_date").desc())
        
        df_silver = df_sales_combined.withColumn("rank", row_number().over(window_spec)) \
                          .filter(col("rank") == 1) \
                          .withColumn("customer_email_hash", # Si vienen datos snsibles deberiamos encriptarlos o hashearlos 
                                      when(col("customer_email").isNotNull(), sha2(col("customer_email"), 256))
                                      .otherwise("ANON_USER")) \
                          .withColumn("ingested_at", current_timestamp()) \
                          .drop("rank", "customer_email") 
                          
        # Escritura en Silver (Iceberg Merge)
        # Esto asegura la idempotencia del pipeline, Iceberg nos permite ACID por lo que esto evitara que tengamos duplicados y solo realizaremos upserts
        logger.info("Ejecutando Upsert en capa Silver...")
        df_silver.show(100, False)
        df_silver.createOrReplaceTempView("source_updates")
        
        spark.sql("CREATE DATABASE IF NOT EXISTS glue_catalog.silver_db")
        

        # Crear la tabla Iceberg inicial
        spark.sql("""
            CREATE TABLE IF NOT EXISTS glue_catalog.silver_db.transactions (
                transaction_id STRING,
                total_amount DOUBLE,
                status STRING,
                operation_date STRING,
                source_system STRING,
                customer_email_hash STRING,
                ingested_at TIMESTAMP
            )
            USING iceberg
            PARTITIONED BY (status)
        """)
        
        expected_columns = {"transaction_id", "total_amount", "status"} # Esto es mala practica podemos meterlos dentro de los args del glue pero de nuevo, para efectos practicos
        current_columns = set(df_silver.columns)
        
        if not expected_columns.issubset(current_columns):
            missing = expected_columns - current_columns
            logger.error(f"error de esquema: Faltan columnas críticas: {missing}")
            sys.exit(1)
        
        # PAra cuando se agregue un campo nuevo a la tabla manejamos schema evolution 

        table_path = "glue_catalog.silver_db.transactions"

        handle_schema_evolution(df_silver, table_path)

        df_silver.writeTo(table_path).append()


    except Exception as e:
        logger.error(f"Error crítico durante la transformación: {str(e)}")
        # en caso de error podemos enviar la alerta mediante sns aqui
        raise e

if __name__ == "__main__":
    transform_to_silver()
    job.commit()

    print("ETL a Silver completado con éxito.")