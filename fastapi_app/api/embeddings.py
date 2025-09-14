from fastapi import APIRouter
from pydantic import BaseModel
from typing import Dict, List

from embeddings.service import get_clickhouse_client, store_embeddings
from embeddings.embeddings import EmbeddingsModel

router = APIRouter()
model = EmbeddingsModel()
client = get_clickhouse_client()

class EmbedRequest(BaseModel):
    text: List[str]
    metadata: Dict

@router.post("/embed")
async def embed(request: EmbedRequest):
    store_embeddings(client, model, request.text, request.metadata)
    return {'status': 'ok', 'inserted': len(request.text)}