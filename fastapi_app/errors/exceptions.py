class ValidationError(Exception):
    """Ошибка валидации входных данных"""
    pass

class LLMError(Exception):
    """Ошибка при обращении к LLM (API HuggingFace, OpenAI и т.п.)"""
    pass

class DBError(Exception):
    """Ошибка при обращении к базе данных"""
    pass