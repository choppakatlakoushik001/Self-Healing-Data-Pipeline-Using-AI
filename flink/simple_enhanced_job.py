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
        message STRING,
        proc_time AS PROCTIME()
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'fastapi-topic',
        'properties.bootstrap.servers' = 'broker:29092',
        'properties.group.id' = 'pyflink-consumer',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json'
    )
    """)
    
    # Create a sink table for raw messages
    table_env.execute_sql("""
    CREATE TABLE print_sink (
        message STRING,
        process_time TIMESTAMP(3)
    ) WITH (
        'connector' = 'print'
    )
    """)
    
    # Create a sink table for word count
    table_env.execute_sql("""
    CREATE TABLE word_count_sink (
        word STRING,
        word_count BIGINT
    ) WITH (
        'connector' = 'print'
    )
    """)
    
    # Create a sink table for message length statistics
    table_env.execute_sql("""
    CREATE TABLE message_stats_sink (
        avg_length DOUBLE,
        min_length INT,
        max_length INT,
        total_messages BIGINT
    ) WITH (
        'connector' = 'print'
    )
    """)
    
    # 1. Basic processing - print the raw messages
    table_env.execute_sql("""
    INSERT INTO print_sink
    SELECT 
        message,
        CURRENT_TIMESTAMP as process_time
    FROM kafka_source
    """)
    
    # 2. Word count analysis
    table_env.execute_sql("""
    INSERT INTO word_count_sink
    SELECT 
        word,
        COUNT(*) as word_count
    FROM (
        SELECT LOWER(T.word) as word 
        FROM kafka_source, LATERAL TABLE(STRING_SPLIT(message, ' ')) AS T(word)
    )
    GROUP BY word
    """)
    
    # 3. Message statistics - compute aggregations on message length
    table_env.execute_sql("""
    INSERT INTO message_stats_sink
    SELECT
        AVG(CHAR_LENGTH(message)) AS avg_length,
        MIN(CHAR_LENGTH(message)) AS min_length,
        MAX(CHAR_LENGTH(message)) AS max_length,
        COUNT(*) AS total_messages
    FROM kafka_source
    """)
    
    print("Enhanced Flink job submitted successfully!")

if __name__ == "__main__":
    main() 