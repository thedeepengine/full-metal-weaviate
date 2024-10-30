---
sidebar_position: 4
---

# Data Sample

Run this in your terminal to get two collections created in your Weaviate env and a couple sample data loaded.

```
import weaviate
client = weaviate.connect_to_local()
#client.collections.delete('JeopardyQuestion')
#client.collections.delete('JeopardyCategory')

JeopardyQuestion = client.collections.create(
    "JeopardyQuestion",
    properties=[
        Property(name="question", data_type=DataType.TEXT),
        Property(name="response", data_type=DataType.TEXT),
        Property(name="points", data_type=DataType.NUMBER),
    ]
)

JeopardyCategory = client.collections.create(
    "JeopardyCategory",
    properties=[
        Property(name="name", data_type=DataType.TEXT),
        Property(name="desc", data_type=DataType.TEXT),
    ]
)

# create two-way references
JeopardyQuestion.config.add_reference(ReferenceProperty(name="hasCategory",target_collection="JeopardyCategory"))
JeopardyCategory.config.add_reference(ReferenceProperty(name="categoryOf",target_collection="JeopardyQuestion"))



# load some data

metal_client=get_metal_client(client)
JeopardyQuestion=metal_client.get_metal_collection('JeopardyQuestion')
JeopardyCategory=metal_client.get_metal_collection('JeopardyCategory')

JeopardyQuestion.metal.register_opposite('hasCategory', 'categoryOf')

JeopardyCategory.l({'name': 'Politics', 'desc': 'politics related questions'}, False)
JeopardyCategory.l({'name': 'Ontology', 'desc': 'Ontology related questions'}, False)

JeopardyQuestion.l({'question': 'why?', 'hasCategory': 'Politics'})
JeopardyQuestion.l({'question': 'who?', 'hasCategory': 'Ontology'})
JeopardyQuestion.l({'question': 'what?', 'hasCategory': 'Ontology'})
JeopardyQuestion.l({'question': 'what's the meaning of life', 
'response': '42', 'hasCategory': 'Politics'})


```