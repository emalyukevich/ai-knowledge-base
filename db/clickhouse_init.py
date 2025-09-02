import clickhouse_connect

def init_clickhouse():
    client = clickhouse_connect.get_client(
        host='localhost',
        port=8123,
        username='default',
        password='default_pass',
    )

    try:
        client.command("SET allow_experimental_json_type = 1")
        print("Настройка allow_experimental_json_type установлена")
    except Exception as e:
        print(f"Ошибка установки настройки: {e}")

    client.command("""
        CREATE TABLE IF NOT EXISTS embeddings (       
            id UUID DEFAULT generateUUIDv4(),
            text String,
            metadata JSON,
            vector Array(Float32)
        ) ENGINE=MergeTree
        ORDER BY id
    """)
    print("Таблица embeddings создана (если её не было)")

if __name__ == '__main__':
    init_clickhouse()