from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from full_metal_weaviate.main import get_weaviate_client
from full_metal_weaviate import get_metal_client

app = FastAPI()

# client=get_weaviate_client('')

app.add_middleware(CORSMiddleware,allow_origins=["*"]
,allow_credentials=True,allow_methods=["*"],allow_headers=["*"])

opposite_refs = ['JeopardyQuestion.hasCategory<>JeopardyCategory.hasQuestion',
                'JeopardyQuestion.hasAssociatedQuestion<>JeopardyQuestion.associatedQuestionOf']

client_weaviate=get_weaviate_client('weaviate')
client=get_metal_client(client_weaviate, opposite_refs)
JeopardyQuestion=client.get_metal_collection('JeopardyQuestion')

@app.post("/v1/api/query/")
async def root(bundle: dict):
    print(bundle)
    res=JeopardyQuestion.metal_query()
    return res