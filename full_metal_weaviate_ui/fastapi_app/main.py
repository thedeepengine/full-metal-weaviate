from fastapi import FastAPI
from utils import get_weaviate_client
app = FastAPI()

client=get_weaviate_client('')

@app.post("/v1/api/query")
async def root():
    return {"message": "Hello World"}