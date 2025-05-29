from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table.catalog import HiveCatalog
from pyflink.table.descriptors import Schema, Json, Kafka, Rowtime

def create_kafka_table(table_env, topic_name):
    """Create a table connected to Kafka"""
    table_env.execute_sql(f"""
    CREATE TABLE kafka_source (
        message STRING,
        event_time TIMESTAMP(3) METADATA FROM 'timestamp',
        WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{topic_name}',
        'properties.bootstrap.servers' = 'broker:29092',
        'properties.group.id' = 'pyflink-consumer',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json'
    )
    """)

def create_print_table(table_env):
    """Create a print sink table"""
    table_env.execute_sql("""
    CREATE TABLE print_sink (
        message STRING,
        event_time TIMESTAMP(3),
        processing_time TIMESTAMP(3)
    ) WITH (
        'connector' = 'print'
    )
    """)

def process_data(table_env):
    """Process data from Kafka and send to sink"""
    # Process with SQL
    table_env.execute_sql("""
    INSERT INTO print_sink
    SELECT 
        message,
        event_time,
        CURRENT_TIMESTAMP as processing_time
    FROM kafka_source
    """)

def main():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    
    # Create a Table environment
    settings = EnvironmentSettings.new_instance() \
                .in_streaming_mode() \
                .build()
    
    table_env = StreamTableEnvironment.create(env, settings)
    
    # Make jars available to PyFlink
    table_env.get_config().set("pipeline.jars", "file:///opt/flink/lib/flink-connector-kafka-1.17.1.jar;file:///opt/flink/lib/kafka-clients-3.4.0.jar")
    
    # Create source and sink tables
    create_kafka_table(table_env, "fastapi-topic")
    create_print_table(table_env)
    
    # Process the data
    process_data(table_env)
    
    print("Flink Kafka Job is running. Check the Flink Dashboard for results.")

if __name__ == "__main__":
    main() 