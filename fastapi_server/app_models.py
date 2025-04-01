from pydantic import BaseModel

class RecommendationsRequest(BaseModel):
    postal_code: int = 238801
    prediction_timestamp: str