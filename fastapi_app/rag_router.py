from fastapi import APIRouter
from pydantic import BaseModel
from typing import List
from fastapi_app.rag_pipeline import rag_pipeline

router = APIRouter()

class QueryRequest(BaseModel):
    query: str

class QueryResponse(BaseModel):
    answer: str
    sources: List[str]

@router.post('/query', response_model=QueryResponse)
async def rag_query(request: QueryRequest):
    result = rag_pipeline(request.query)
    return QueryResponse(**result)