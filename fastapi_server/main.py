from fastapi import FastAPI, HTTPException

from my_models import Item

app = FastAPI()

items = []

@app.get("/")
def root():
    return {"Hello": "World"}

@app.post("/items", response_model=list[Item])
def create_items(item: Item):
    items.append(item)
    return items

@app.get("/items")
def list_items(limit: int = 10):
    return items[0:limit]

@app.get("/items/{item_id}", response_model=Item)
def get_item(item_id: int) -> Item:
    if item_id < len(items):
        return items[item_id]
    else:
        raise HTTPException(status_code=404, detail=f'Item {item_id} not found')