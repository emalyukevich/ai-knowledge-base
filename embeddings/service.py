import clickhouse_connect
from embeddings.embeddings import EmbeddingsModel

def get_clickhouse_client():
    return clickhouse_connect.get_client(
        host='clickhouse',
        port=8123,
        username='default',
        password='default_pass'
    )

def store_embeddings(client, model: EmbeddingsModel, texts: list[str], meta: dict):
    vectors = model.encode(texts)
    rows = [
        (ch, meta, vec) for ch, vec in zip(texts, vectors)
    ]

    client.insert("embeddings", rows, column_names=["text", "metadata", "vector"])