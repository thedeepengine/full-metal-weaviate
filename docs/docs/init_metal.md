---
sidebar_position: 1
---

# Intro

Full Metal Weaviate has been developed for my own work and research. Here is the thing: sometimes you just want to quickly spin up a db, load and query data for testing purposes. And this is the main usage of Full Metal Weaviate; this library is meant to be used to quickly poke around with Weaviate.

:::warning
Full Metal Weaviate is in alpha and under active development. It is absolutely not recommended for production use as it may contain bugs, incomplete features, and can undergo significant changes. Use at your own risk.
:::

## Installation

```
pip install git+https://github.com/thedeepengine/full-metal-weaviate.git
```

## Initialize your metal client

```python
import weaviate
from full_metal_weaviate import get_metal_client

weaviate_client = weaviate.connect_to_local()
metal_client = get_metal_client(weaviate_client)
```

## Get a metal collection

From a metal client, you can get a metal collection.

```python
collectionName = metal_client.get_metal_collection('<your_collection_name>')
```

From now on you can **[query](query_data.md)** and **[load](load_data.md)** data using `.metal_query` and `.metal_load` methods.

## Register opposite relationships

:::info
If you want to use two-way references loading metal syntax, you need to register the opposite relationships first.
:::

Register opposite two-way references using the `register_opposite` method.

For example:
if `hasCategory` is the opposite of `questionOf` and `hasAssociatedQuestion` the opposite of `associatedQuestionOf`

```python
JeopardyQuestion.metal.register_opposite('hasCategory', 'questionOf')
JeopardyQuestion.metal.register_opposite('hasAssociatedQuestion', 'associatedQuestionOf')
```

You only have to register the opposite relationship for one collection only, the system associate the opposite with the collection using your weaviate schema.

Check out the page to run **[two-way loading](load_data.md)** 

## Quickstart Dataset

If you want to quickly test the possibilities of fmw, **[load a sample dataset](data_sample.md)** or run **[queries on your own data](query_data.md)**


