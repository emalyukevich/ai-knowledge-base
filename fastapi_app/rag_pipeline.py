from typing import List, Union, Optional
import os
from dotenv import load_dotenv
import logging
import clickhouse_connect
import numpy as np
import torch
from transformers import AutoModel, AutoTokenizer
from huggingface_hub import InferenceClient

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

HF_TOKEN = os.getenv('HF_TOKEN')
if HF_TOKEN is None:
    raise ValueError("HF_TOKEN is not set. Add it to your .env file")

model_name = "mistralai/Mistral-7B-Instruct-v0.2"
client = InferenceClient(model=model_name, token=HF_TOKEN)

def get_query_embedding(
        texts: Union[str, List[str]],
        model_name_or_path: Optional[str] = None,
        device: Optional[str] = None,
        pooling: str = "mean",
        max_length: int = 512,
        normalize: bool = True) -> np.ndarray:
    """
    Преобразует текст или список текстов в вектор(ы).
    Возвращает np.ndarray: (dim,) для одного текста или (batch, dim) для списка.
    """
    if isinstance(texts, str):
        texts = [texts]

    device = device or ('cuda' if torch.cuda.is_available() else 'cpu')

    tokenizer = AutoTokenizer.from_pretrained(model_name_or_path)
    model = AutoModel.from_pretrained(model_name_or_path)
    model.to(device)
    model.eval()

    enc = tokenizer(texts, padding=True, truncation=True, max_length=max_length, return_tensors="pt")
    input_ids = enc['input_ids'].to(device)
    attention_mask = enc['attention_mask'].to(device)

    with torch.no_grad():
        outputs = model(input_ids=input_ids, attention_mask=attention_mask, return_dict=True)

        if pooling == 'cls':
            if hasattr(outputs, 'pooler_output') and outputs.pooler_output is not None:
                emb = outputs.pooler_output
            else:
                emb = outputs.last_hidden_state[:, 0, :]
        else:
            last_hidden = outputs.last_hidden_state # (batch, seq, dim)
            mask = attention_mask.unsqueeze(-1).expand(last_hidden.size()).float()
            summed = (last_hidden * mask).sum(dim=1)
            lengths = mask.sum(dim=1).clamp(min=1e-9)
            emb = summed / lengths # (batch, dim)

        if normalize:
            emb = emb / (emb.norm(p=2, dim=1, keepdim=True) + 1e-12)

        emb = emb.cpu().numpy().astype(np.float32)

    return emb[0] if len(emb) == 1 else emb

def search_in_clickhouse(query_vector: np.ndarray, top_k: int = 5):
    client = clickhouse_connect.get_client(
        host="clickhouse", port=8123, username="default", password="default_pass"
    )

    vector_str = '[' + ','.join(map(str, query_vector.tolist())) + ']'
    sql = f'''
        SELECT
            id, text, cosineDistance(vector, {vector_str}) AS distance
        FROM
            embeddings
        ORDER BY distance ASC
        LIMIT {top_k}
    '''
    result = client.query(sql)
    rows = result.result_rows
    columns = result.column_names

    return [dict(zip(columns, row)) for row in rows]

def build_prompt(query: str, results: list, max_context_len: int = 1000) -> str:
    """
       Формирует prompt для LLM в стиле RAG:
       - query: запрос пользователя
       - results: список документов из ClickHouse (top-k), где в каждом dict есть ключ 'text'
       - max_context_len: ограничение длины контекста (символы)
    """

    context_parts = []
    total_len = 0
    for idx, item in enumerate(results, start=1):
        fragment = item['text']
        fragment = fragment[:300]
        if total_len + len(fragment) > max_context_len:
            break

        context_parts.append(f'{idx}. {fragment}')
        total_len += len(fragment)

    context = "\n".join(context_parts)

    prompt = f"""Используй приведённый контекст, чтобы ответить на вопрос.
        Контекст:
        {context}
        
        Вопрос: {query}
        Ответ (будь максимально точным, опирайся только на контекст):"""
    return prompt

def generate_answer(prompt: str) -> str:
    try:
        response = client.chat_completion(
            messages=[
                {"role": "system", "content": "You are a helpful assistant for answering questions based on retrieved documents."},
                {"role": "user", "content": prompt},
            ],
            max_tokens=512,
            temperature=0.7
        )
        return response.choices[0].message["content"]
    except Exception as e:
        return f"Error during generation: {e}"

def rag_pipeline(query: str, embed_model: str = "sentence-transformers/all-MiniLM-L6-v2", top_k: int = 5) -> dict:
    """
        Полный RAG-пайплайн:
        1. Получает эмбеддинг запроса
        2. Делает поиск в ClickHouse
        3. Формирует промпт
        4. Генерирует ответ через LLM
        Возвращает dict: {"answer": str, "sources": list}
    """
    query_vec = get_query_embedding(query, model_name_or_path=embed_model)
    retrieved = search_in_clickhouse(query_vec, top_k=top_k)
    if not retrieved:
        return {"answer": "Sorry, I didn't find relevant documents.", "sources": []}
    prompt = build_prompt(query, retrieved)
    answer = generate_answer(prompt)
    sources = [str(r["id"]) for r in retrieved]

    return {"answer": answer, "sources": sources}