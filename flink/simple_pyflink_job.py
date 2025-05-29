import os
from pyflink.table import EnvironmentSettings, TableEnvironment

def main():
    # Set JAVA_HOME environment variable
    os.environ["JAVA_HOME"] = "/opt/java/openjdk"
    
    # Create a TableEnvironment
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)
    
    # Add necessary JAR files
    table_env.get_config().set("pipeline.jars", 
                              "file:///opt/flink/lib/flink-connector-kafka-1.17.1.jar;" + 
                              "file:///opt/flink/lib/kafka-clients-3.4.0.jar")
    
    # Create Kafka source table
    table_env.execute_sql("""
    CREATE TABLE kafka_source (
        message STRING
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'fastapi-topic',
        'properties.bootstrap.servers' = 'broker:29092',
        'properties.group.id' = 'pyflink-consumer',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json'
    )
    """)
    
    # Create a sink table for printing results
    table_env.execute_sql("""
    CREATE TABLE print_sink (
        message STRING
    ) WITH (
        'connector' = 'print'
    )
    """)
    
    # Query and insert into sink
    table_env.execute_sql("""
    INSERT INTO print_sink
    SELECT message
    FROM kafka_source
    """)
    
    print("Flink job submitted successfully!")

if __name__ == "__main__":
    main() 