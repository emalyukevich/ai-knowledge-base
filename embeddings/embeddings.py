from sentence_transformers import SentenceTransformer

class EmbeddingsModel:
    def __init__(self, model_name: str = "sentence-transformers/all-MiniLM-L6-v2"):
        self.model = SentenceTransformer(model_name)

    def encode(self, texts: list[str]) -> list[list[float]]:
        return self.model.encode(texts, batch_size=128, show_progress_bar=True ,convert_to_numpy=True).tolist()