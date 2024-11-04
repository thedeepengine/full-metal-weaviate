---
sidebar_position: 5
---

# Data Sample

This is a sample dataset to test against the possibilities of Full Metal Weaviate.

First get your classic Weaviate client

```python
import weaviate
weaviate_client = weaviate.connect_to_local()
```

Running the following will create three collections: Technology, TechnologyProperties and Contributor

```python
from full_metal_weaviate import get_metal_client

client_metal.collections.delete('Technology')
client_metal.collections.delete('Contributor')
client_metal.collections.delete('TechnologyProperties')

Technology = client_metal.collections.create(
    name="Technology",
    properties=[
        Property(name="name", data_type=DataType.TEXT, tokenization= Tokenization.FIELD),
        Property(name="description", data_type=DataType.TEXT),
        Property(name="github", data_type=DataType.TEXT),
        Property(name="nb_stars", data_type=DataType.INT),
        Property(name="release_date", data_type=DataType.DATE),
        Property(name="number_field", data_type=DataType.NUMBER),
    ],
    vectorizer_config=[Configure.NamedVectors.none(name="vect_field")]
)

TechnologyProperties = client_metal.collections.create(
    name="TechnologyProperties",
    properties=[
        Property(name="name", data_type=DataType.TEXT),
        Property(name="description", data_type=DataType.TEXT)
    ]
)

Contributor = client_metal.collections.create(
    name="Contributor",
    properties=[
        Property(name="name", data_type=DataType.TEXT)
    ]
)

Technology.config.add_reference(ReferenceProperty(name="hasProperty",target_collection="TechnologyProperties"))
TechnologyProperties.config.add_reference(ReferenceProperty(name="propertyOf",target_collection="Technology"))

Technology.config.add_reference(ReferenceProperty(name="hasContributor",target_collection="Contributor"))
Contributor.config.add_reference(ReferenceProperty(name="contributorOf",target_collection="Technology"))
```

