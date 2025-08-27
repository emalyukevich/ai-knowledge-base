from fastapi import FastAPI

app = FastAPI(
    title = "AI Knowledge Base API",
    description="Минимальный сервис для старта проекта",
    version="0.1.0"
)

@app.get("/")
def read_root():
    return {"message": "AI Knowledge Base is running 🚀"}
