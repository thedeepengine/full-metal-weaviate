---
sidebar_position: 1
---

# Intro

Full Metal Weaviate has been developed for my own work and research. Here is the thing: sometimes you just want to quickly spin up a db, load and query data for testing purposes. And this is the main usage of Full Metal Weaviate; this library is meant to be used to quickly poke around with Weaviate.

:::warning
Full Metal Weaviate is currently in alpha and under active development. It is absolutely not recommended for production use as it may contain bugs, incomplete features, and can undergo significant changes. Use at your own risk.
:::


## Initialize your metal client

```
import weaviate
weaviate_client = weaviate.connect_to_local()
metal_client = get_metal_client(weaviate_client)
```

## Quickstart Dataset

If you want to quickly test the possibilities of fmw, **[get a sample dataset](data_sample.md)**

## Get a metal collection

From a metal client, you can get a metal collection.

```
JeopardyQuestion = metal_client.get_metal_collection('JeopardyQuestion')
```

That's it. From now on you can **[query](query_data.md)** and **[load](load_data.md)** data using `.metal_query` and `.metal_load` methods

## Register opposite relationships

If you want to load two-way references, you have to register opposite relationships. 

Register opposite two-way references using `register_opposite` method.

For example:
if `hasCategory` is the opposite of `hasQuestion` and `hasAssociatedQuestion` the opposite of `associatedQuestionOf`

```
JeopardyQuestion.metal.register_opposite('hasCategory', 'hasQuestion')
JeopardyQuestion.metal.register_opposite('hasAssociatedQuestion', 'associatedQuestionOf')
```

Check out the page to run **[two-way loading](load_data.md)** 


