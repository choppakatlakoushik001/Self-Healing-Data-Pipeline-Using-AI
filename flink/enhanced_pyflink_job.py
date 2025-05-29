from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

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
    """Create a print sink table for raw messages"""
    table_env.execute_sql("""
    CREATE TABLE print_sink (
        message STRING,
        event_time TIMESTAMP(3),
        processing_time TIMESTAMP(3)
    ) WITH (
        'connector' = 'print'
    )
    """)

def create_word_count_sink(table_env):
    """Create a print sink table for word counts"""
    table_env.execute_sql("""
    CREATE TABLE word_count_sink (
        word STRING,
        word_count BIGINT,
        window_start TIMESTAMP(3),
        window_end TIMESTAMP(3)
    ) WITH (
        'connector' = 'print'
    )
    """)

def create_message_stats_sink(table_env):
    """Create a print sink table for message statistics"""
    table_env.execute_sql("""
    CREATE TABLE message_stats_sink (
        window_start TIMESTAMP(3),
        window_end TIMESTAMP(3),
        message_count BIGINT,
        avg_length DOUBLE,
        min_length BIGINT,
        max_length BIGINT
    ) WITH (
        'connector' = 'print'
    )
    """)

def process_data(table_env):
    """Process data from Kafka with multiple transformations"""
    
    # 1. Basic passthrough to see raw messages
    table_env.execute_sql("""
    INSERT INTO print_sink
    SELECT 
        message,
        event_time,
        CURRENT_TIMESTAMP as processing_time
    FROM kafka_source
    """)
    
    # 2. Word count analysis - split messages into words and count them
    table_env.execute_sql("""
    INSERT INTO word_count_sink
    SELECT 
        word,
        COUNT(*) AS word_count,
        window_start,
        window_end
    FROM (
        SELECT 
            LOWER(word) AS word,
            event_time,
            window_start,
            window_end
        FROM kafka_source, 
        LATERAL TABLE(
            STRING_SPLIT(message, ' ')
        ) AS T(word),
        TUMBLE(TABLE kafka_source, DESCRIPTOR(event_time), INTERVAL '1' MINUTE) AS window
    )
    GROUP BY word, window_start, window_end
    """)
    
    # 3. Message statistics - compute aggregations on message length
    table_env.execute_sql("""
    INSERT INTO message_stats_sink
    SELECT 
        window_start,
        window_end,
        COUNT(*) AS message_count,
        AVG(CHAR_LENGTH(message)) AS avg_length,
        MIN(CHAR_LENGTH(message)) AS min_length,
        MAX(CHAR_LENGTH(message)) AS max_length
    FROM kafka_source,
    TUMBLE(TABLE kafka_source, DESCRIPTOR(event_time), INTERVAL '1' MINUTE) AS window
    GROUP BY window_start, window_end
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
    create_word_count_sink(table_env)
    create_message_stats_sink(table_env)
    
    # Process the data
    process_data(table_env)
    
    print("Enhanced Flink Kafka Job is running with transformations and aggregations.")
    print("Check the Flink Dashboard for results.")

if __name__ == "__main__":
    main() 