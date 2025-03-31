from pydantic import BaseModel

class Item(BaseModel):
    text: str = None
    is_done: bool = False

class RecommendationsRequest(BaseModel):
    postal_code: int = 238801