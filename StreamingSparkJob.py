# %%
# Welcome to your new notebook
# Type here in the cell editor to add code!
print("hello")

# %%
#%pip install fastavro
#%pip install azure-schemaregistry-avroencoder

# %%
import json
import os
import notebookutils
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import udf
from pyspark.sql.functions import col, from_json
os.environ["AZURE_TENANT_ID"] = "150305b3-cc4b-46dd-9912-425678db1498"
os.environ["AZURE_CLIENT_ID"] = "799ef93e-1b4c-4bb1-8930-5d93daa13ff3"
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
sp_client_secret = notebookutils.credentials.getSecret('https://kv-anildwaa684447902659.vault.azure.net/', 'fabric-sp-client-secret')
os.environ["AZURE_CLIENT_SECRET"] = sp_client_secret
fully_qualified_namespace = 'anildwaeventhubns.servicebus.windows.net'
eventhub_name = 'eh4'
GROUP_NAME_AVRO = "anilgroup_avro"
CATALOG_SALES_SCHEMA_AVRO = """ 
{
  "namespace": "Microsoft.Azure.Data.SchemaRegistry.example",
  "type": "record",
  "name": "DataEvent",
  "fields": [
    { "name": "cs_sold_date_sk", "type": ["null", "int"], "default": null },
    { "name": "cs_sold_time_sk", "type": ["null", "int"], "default": null },
    { "name": "cs_ship_date_sk", "type": ["null", "int"], "default": null },
    { "name": "cs_bill_customer_sk", "type": ["null", "int"], "default": null },
    { "name": "cs_bill_cdemo_sk", "type": ["null", "int"], "default": null },
    { "name": "cs_bill_hdemo_sk", "type": ["null", "int"], "default": null },
    { "name": "cs_bill_addr_sk", "type": ["null", "int"], "default": null },
    { "name": "cs_ship_customer_sk", "type": ["null", "int"], "default": null },
    { "name": "cs_ship_cdemo_sk", "type": ["null", "int"], "default": null },
    { "name": "cs_ship_hdemo_sk", "type": ["null", "int"], "default": null },
    { "name": "cs_ship_addr_sk", "type": ["null", "int"], "default": null },
    { "name": "cs_call_center_sk", "type": ["null", "int"], "default": null },
    { "name": "cs_catalog_page_sk", "type": ["null", "int"], "default": null },
    { "name": "cs_ship_mode_sk", "type": ["null", "int"], "default": null },
    { "name": "cs_warehouse_sk", "type": ["null", "int"], "default": null },
    { "name": "cs_item_sk", "type": ["null", "int"], "default": null },
    { "name": "cs_promo_sk", "type": ["null", "int"], "default": null },
    { "name": "cs_order_number", "type": ["null", "int"], "default": null },
    { "name": "cs_quantity", "type": ["null", "int"], "default": null },
    { "name": "cs_wholesale_cost", "type": ["null", "double"], "default": null },
    { "name": "cs_list_price", "type": ["null", "double"], "default": null },
    { "name": "cs_sales_price", "type": ["null", "double"], "default": null },
    { "name": "cs_ext_discount_amt", "type": ["null", "double"], "default": null },
    { "name": "cs_ext_sales_price", "type": ["null", "double"], "default": null },
    { "name": "cs_ext_wholesale_cost", "type": ["null", "double"], "default": null },
    { "name": "cs_ext_list_price", "type": ["null", "double"], "default": null },
    { "name": "cs_ext_tax", "type": ["null", "double"], "default": null },
    { "name": "cs_coupon_amt", "type": ["null", "double"], "default": null },
    { "name": "cs_ext_ship_cost", "type": ["null", "double"], "default": null },
    { "name": "cs_net_paid", "type": ["null", "double"], "default": null },
    { "name": "cs_net_paid_inc_tax", "type": ["null", "double"], "default": null },
    { "name": "cs_net_paid_inc_ship", "type": ["null", "double"], "default": null },
    { "name": "cs_net_paid_inc_ship_tax", "type": ["null", "double"], "default": null },
    { "name": "cs_net_profit", "type": ["null", "double"], "default": null },
    { "name": "session_id", "type": ["null", "string"], "default": null },
    { "name": "CreatedTime", "type": ["null", { "type": "long", "logicalType": "timestamp-seconds" }], "default": null }
  ]
}

"""

# %%
os.environ["AZURE_TENANT_ID"]

# %%
from azure.schemaregistry import SchemaRegistryClient
from azure.schemaregistry.encoder.avroencoder import AvroEncoder
from azure.identity import DefaultAzureCredential as SyncDefaultAzureCredential
token_credential = SyncDefaultAzureCredential()
token = token_credential.get_token("https://eventhubs.azure.net/.default")


# %%
schema_registry = SchemaRegistryClient(
    fully_qualified_namespace=fully_qualified_namespace,
    credential=token_credential,
)



encoder = AvroEncoder(
    client=schema_registry, group_name=GROUP_NAME_AVRO, auto_register=False,
)

encoder._get_schema.cache_clear()
encoder._get_schema_id.cache_clear()


schema_properties = schema_registry.get_schema_properties(group_name=GROUP_NAME_AVRO, 
                                            name="Microsoft.Azure.Data.SchemaRegistry.example.DataEvent", 
                                            format="avro",
                                            definition=CATALOG_SALES_SCHEMA_AVRO)
avro_schema_definition = schema_registry.get_schema(schema_id=schema_properties.id).definition
print(f"✅ Registered Schema ID: {schema_properties.id}")




# %%

def decode_event_data_message(encoder, event_data):
    # encoder.decode would extract the schema id from the content_type,
    # retrieve schema from Schema Registry and cache the schema locally.
    # If the schema id is in the local cache, the call won't trigger a service call.
    decoded_content = encoder.decode(event_data)

    
    return decoded_content


def decode_body(body):
    try:
        return decode_event_data_message(encoder, body)  # Apply your decoding function
    except Exception as e:
        return str(e)  # Capture decoding errors

decode_udf = udf(decode_body, StringType())

# %%
eh_connection_eh4_string = notebookutils.credentials.getSecret('https://kv-anildwaa684447902659.vault.azure.net/', 'eventhub-connectionstring-eh4')


# Define detailed data schema
data_event_schema  = StructType([
    StructField("cs_sold_date_sk", IntegerType(), True),
    StructField("cs_sold_time_sk", IntegerType(), True),
    StructField("cs_ship_date_sk", IntegerType(), True),
    StructField("cs_bill_customer_sk", IntegerType(), True),
    StructField("cs_bill_cdemo_sk", IntegerType(), True),
    StructField("cs_bill_hdemo_sk", IntegerType(), True),
    StructField("cs_bill_addr_sk", IntegerType(), True),
    StructField("cs_ship_customer_sk", IntegerType(), True),
    StructField("cs_ship_cdemo_sk", IntegerType(), True),
    StructField("cs_ship_hdemo_sk", IntegerType(), True),
    StructField("cs_ship_addr_sk", IntegerType(), True),
    StructField("cs_call_center_sk", IntegerType(), True),
    StructField("cs_catalog_page_sk", IntegerType(), True),
    StructField("cs_ship_mode_sk", IntegerType(), True),
    StructField("cs_warehouse_sk", IntegerType(), True),
    StructField("cs_item_sk", IntegerType(), True),
    StructField("cs_promo_sk", IntegerType(), True),
    StructField("cs_order_number", IntegerType(), True),
    StructField("cs_quantity", IntegerType(), True),
    StructField("cs_wholesale_cost", DoubleType(), True),
    StructField("cs_list_price", DoubleType(), True),
    StructField("cs_sales_price", DoubleType(), True),
    StructField("cs_ext_discount_amt", DoubleType(), True),
    StructField("cs_ext_sales_price", DoubleType(), True),
    StructField("cs_ext_wholesale_cost", DoubleType(), True),
    StructField("cs_ext_list_price", DoubleType(), True),
    StructField("cs_ext_tax", DoubleType(), True),
    StructField("cs_coupon_amt", DoubleType(), True),
    StructField("cs_ext_ship_cost", DoubleType(), True),
    StructField("cs_net_paid", DoubleType(), True),
    StructField("cs_net_paid_inc_tax", DoubleType(), True),
    StructField("cs_net_paid_inc_ship", DoubleType(), True),
    StructField("cs_net_paid_inc_ship_tax", DoubleType(), True),
    StructField("cs_net_profit", DoubleType(), True),
    StructField("session_id", StringType(), True),
    StructField("CreatedTime", TimestampType(), True) 
])

# Define event schema containing JSON data field
event_schema = StructType([
    StructField("eventId", StringType(), True), 
    StructField("timestamp", StringType(), True), 
    StructField("data", StringType(), True)  # Incoming JSON data as a string
])

startingEventPosition = {
  "offset": "@latest",  
  "seqNo": -1,            #not in use
  "enqueuedTime": None,   #not in use
  "isInclusive": True
}

ehConf = {}

# For versions before 2.3.15, set the connection string without encryption
# ehConf['eventhubs.connectionString'] = connectionString

# For 2.3.15 version and above, the configuration dictionary requires that connection string be encrypted.
ehConf['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(eh_connection_eh4_string)
ehConf['eventhubs.consumerGroup'] = "cg1"
ehConf["eventhubs.startingPosition"] = json.dumps(startingEventPosition)

lakehouse_path = "abfss://fabriccontainer@anildwafabricadlsgen2.dfs.core.windows.net/event_data2/"
checkpoint_location = "abfss://fabriccontainer@anildwafabricadlsgen2.dfs.core.windows.net/checkpoint2/"



# %%
print(ehConf)

# %%
broadcast_config = spark.sparkContext.broadcast({
    "fully_qualified_namespace": fully_qualified_namespace,
    "group_name": GROUP_NAME_AVRO,
    "avro_definition": avro_schema_definition
})

def decode_body(body):
    try:
        from azure.schemaregistry import SchemaRegistryClient
        from azure.identity import DefaultAzureCredential
        import fastavro
        import io
        import json
        # Reinitialize Schema Registry Client within the UDF
        schema_registry_client = SchemaRegistryClient(
            fully_qualified_namespace=broadcast_config.value["fully_qualified_namespace"],
            credential=DefaultAzureCredential()
        )
        encoder = AvroEncoder(
            client=schema_registry_client,
            group_name=broadcast_config.value["group_name"],
            auto_register=False,
        )

        #schema_properties = schema_registry_client.get_schema_properties(
        #    group_name=broadcast_config.value["group_name"],
        #    name="Microsoft.Azure.Data.SchemaRegistry.example.DataEvent",
        #    format="avro",
        #    definition=broadcast_config.value["avro_definition"]
        #)
        #schema_definition = schema_registry_client.get_schema(schema_id=schema_properties.id).definition


        if isinstance(body, str):
            body = body.encode("utf-8")
        elif isinstance(body, bytearray):
            body = bytes(body)

        avro_schema = broadcast_config.value["avro_definition"]
        if isinstance(avro_schema, str):
            avro_schema = json.loads(avro_schema)


        # Decode Avro message using FastAvro
        bytes_reader = io.BytesIO(body)
        decoded_event = fastavro.schemaless_reader(bytes_reader, avro_schema)

        # Convert decoded event to JSON string
        return json.dumps(decoded_event)

    except Exception as e:
        return str(e)  # Handle errors gracefully

decode_udf = udf(decode_body, StringType())

# %%
print("Starting Streaming Reciever..")

# %%
#query.stop()

# %%
df_raw = spark \
    .readStream \
    .format("eventhubs") \
    .options(**ehConf) \
    .load()

df_decoded = df_raw.withColumn("decoded_body", decode_udf(df_raw.body))

df_raw = df_decoded.selectExpr("CAST(decoded_body AS STRING) as json_data")
df_parsed = df_raw.withColumn("parsed_data", from_json(col("json_data"), data_event_schema))
df_final = df_parsed.select("parsed_data.*")

#debug using in-memory df
#query = df_final.writeStream \
#    .format("memory") \
#    .queryName("structured_data") \
#    .outputMode("append") \
#    .start()

# %%
query = df_final.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", checkpoint_location) \
  .start(lakehouse_path)

query.awaitTermination()

# %%
#display(spark.sql("SELECT CreatedTime FROM structured_data"))

# %%
#df_raw = spark \
#    .readStream \
#    .format("eventhubs") \
#    .options(**ehConf) \
#    .load()


#df_raw = df_raw.selectExpr("CAST(body AS STRING) as json_data")
#df_parsed = df_raw.withColumn("parsed_data", from_json(col("json_data"), data_event_schema))
#df_final = df_parsed.select("parsed_data.*")

#debug using in-memory df
#query = df_final.writeStream \
#    .format("memory") \
#    .queryName("structured_data") \
#    .outputMode("append") \
#    .start()


# %%
#query = df_final.writeStream \
#  .format("delta") \
#  .outputMode("append") \
#  .option("checkpointLocation", checkpoint_location) \
#  .start(lakehouse_path)

#query.awaitTermination()


