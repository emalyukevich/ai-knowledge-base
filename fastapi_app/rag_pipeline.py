from typing import List, Union, Optional
import os
import time
from dotenv import load_dotenv
import logging
import clickhouse_connect
import numpy as np
import torch
from sentence_transformers import SentenceTransformer
from transformers import AutoTokenizer, AutoModelForCausalLM, pipeline
from huggingface_hub import InferenceClient

from fastapi_app.errors.exceptions import LLMError, ValidationError

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

USE_LOCAL = os.getenv('USE_LOCAL_MODEL', 'true').lower() in ('1', 'true', 'yes')
LOCAL_LLM_MODEL = os.getenv("LOCAL_LLM_MODEL", "google/flan-t5-small")
HF_TOKEN = os.getenv("HF_TOKEN")
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2")

USE_MLFLOW = os.getenv('USE_MLFLOW', 'true').lower() in ('1', 'true', 'yes')

if torch.backends.mps.is_available():
    DEVICE = 'mps'
elif torch.cuda.is_available():
    DEVICE = 'cuda'
else:
    DEVICE = 'cpu'

logger.info('RAG pipeline init. USE_LOCAL=%s, device=%s', USE_LOCAL, DEVICE)

_embedding_model = None
_local_llm_pipeline = None
_hf_client = None
_clickhouse_client = None

def get_embedding_model():
    global _embedding_model
    if _embedding_model is None:
        logger.info('Loading embedding model: %s', EMBEDDING_MODEL)
        _embedding_model = SentenceTransformer(EMBEDDING_MODEL, device='cpu')
    return _embedding_model

def get_clickhouse_client():
    global _clickhouse_client
    if _clickhouse_client is None:
        _clickhouse_client = clickhouse_connect.get_client(
            host=os.getenv('CLICKHOUSE_HOST', 'clickhouse'),
            port=int(os.getenv('CLICKHOUSE_PORT', '8123')),
            username=os.getenv('CLICKHOUSE_USER', 'default'),
            password=os.getenv('CLICKHOUSE_PASSWORD', 'default_pass')
        )
    return _clickhouse_client

def get_hf_client():
    global _hf_client
    if _hf_client is None:
        if not HF_TOKEN:
            raise ValueError('HF_TOKEN is not set but remote HF inference was requested.')
        model_name = 'mistralai/Mistral-7B-Instruct-v0.2'
        _hf_client = InferenceClient(model=model_name, token=HF_TOKEN)
    return _hf_client

def get_local_llm_pipeline():
    """Return transformers pipeline for local LLM (lazy)."""
    global _local_llm_pipeline
    if _local_llm_pipeline is None:
        model_name = LOCAL_LLM_MODEL
        logger.info('Loading local LLM model: %s (device=%s). This may take time.', model_name, DEVICE)

        tokenizer = AutoTokenizer.from_pretrained(LOCAL_LLM_MODEL)
        model = AutoModelForCausalLM.from_pretrained(
            LOCAL_LLM_MODEL,                torch_dtype=torch.float16 if DEVICE != "cpu" else torch.float32,
            device_map="auto" if DEVICE != "cpu" else None
        )
        model.to(DEVICE)
        _local_llm_pipeline = pipeline("text-generation",
            model=model,
            tokenizer=tokenizer,
            device=0 if DEVICE != "cpu" else -1
        )
    return _local_llm_pipeline

#-----------------------------------------------------------------------------------

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
    model_name_or_path = model_name_or_path or EMBEDDING_MODEL
    if isinstance(texts, str):
        single = True
        texts = [texts]
    else:
        single = False

    if model_name_or_path.startswith("sentence-transformers/"):
        m = get_embedding_model()
        emb = m.encode(texts, convert_to_numpy=True, normalize_embeddings=True)
        return emb[0] if single else emb
    raise NotImplementedError('Only sentence-transformers embeddings are implemented in this pipeline.')

def search_in_clickhouse(query_vector: np.ndarray, top_k: int = 5):
    client = get_clickhouse_client()
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

    prompt = f"""
    You are an AI assistant. Read the CONTEXT below and answer the QUESTION strictly using this information.
    If you cannot find the answer, say "I don't know based on the given context."

    CONTEXT:
    {context}

    QUESTION: {query}
    ANSWER:
    """
    return prompt

def generate_answer(prompt: str, max_tokens: int = 300, temperature: float = 0.7) -> str:
    try:
        if USE_LOCAL:
            try:
                pipe = get_local_llm_pipeline()
                formatted_prompt = f"""You are a helpful financial assistant.
            Answer the QUESTION based only on the CONTEXT below.

            CONTEXT:
            {prompt}
            If the context does not contain enough information, reply: "I don’t know based on the given documents."
            Answer:
            """
                output = pipe(
                    formatted_prompt,
                    max_new_tokens=max_tokens,
                    do_sample=True,
                    temperature=temperature,
                    top_p=0.9,
                    repetition_penalty=1.1,
                )
                answer = output[0]["generated_text"].split("Answer:")[-1].strip()
                answer = answer.split("\n")[0].strip()

                return answer or "I don’t know based on the given documents."
            except Exception as e:
                logging.error(f"LLM generation failed: {e}", exc_info=True)
                return f"Error during generation: {e}"
        else:
            client = get_hf_client()

            response = client.chat_completion(
                messages=[
                    {"role": "system",
                     "content": "You are a helpful assistant for answering questions based on retrieved documents."},
                    {"role": "user", "content": prompt},
                ],
                max_tokens=max_tokens,
                temperature=temperature
            )
            try:
                return response.choices[0].message["content"].strip()
            except Exception:
                return str(response)
    except Exception as e:
        logger.exception("LLM generation failed: %s", e)
        raise LLMError(str(e))

def rag_pipeline(query: str, embed_model: str = None, top_k: int = 5) -> dict:
    """
        Полный RAG-пайплайн:
        1. Получает эмбеддинг запроса
        2. Делает поиск в ClickHouse
        3. Формирует промпт
        4. Генерирует ответ через LLM
        Возвращает dict: {"answer": str, "sources": list}
    """
    embed_model = embed_model or EMBEDDING_MODEL
    if not query or not query.strip():
        raise ValidationError("Empty query")

    start = time.perf_counter()
    q_vec = get_query_embedding(query, model_name_or_path=embed_model)
    retrieved = search_in_clickhouse(q_vec, top_k=top_k)

    if not retrieved:
        latency = round(time.perf_counter() - start, 3)
        return {"answer": "Sorry, I didn't find relevant documents.", "sources": [], "latency": latency,
                "status": "success"}

    prompt = build_prompt(query, retrieved)
    answer = generate_answer(prompt)

    latency = round(time.perf_counter() - start, 3)
    sources = [str(r["id"]) for r in retrieved]
    return {"answer": answer, "sources": sources, "latency": latency, "status": "success"}
