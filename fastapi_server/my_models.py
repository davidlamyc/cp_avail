from pydantic import BaseModel

class Item(BaseModel):
    text: str = None
    is_done: bool = False