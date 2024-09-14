from fastapi import FastAPI
from utils import get_weaviate_client

from full_metal_weaviate import get_metal_client

app = FastAPI()

client=get_weaviate_client('')

opposite_refs = ['JeopardyQuestion.hasCategory<>JeopardyCategory.hasQuestion',
                'JeopardyQuestion.hasAssociatedQuestion<>JeopardyQuestion.associatedQuestionOf']

client_weaviate=get_weaviate_client('localhost')
client=get_metal_client(client_weaviate, opposite_refs)
JeopardyQuestion=client.get_metal_collection('JeopardyQuestion')

@app.post("/v1/api/query")
async def root(bundle: dict):
    print(bundle)
    res=JeopardyQuestion.metal_query()
    return res