---
sidebar_position: 4
---

# Load data

:::info
The loading part of this library is not the most useful one excepted maybe for the two-way reference loading. Use a simple operator to upload two-way references.
:::


Get your metal client and some collections:

```python
client_weaviate=<your weaviate client>

# if you haven't yet created the sample testing dataset run:
# sample_data(client_weaviate)

# get metal client and collections
client_metal=get_metal_client(client_weaviate)
technology=client_metal.get_metal_collection('Technology')
technology_property=client_metal.get_metal_collection('technology_property')
contributor=client_metal.get_metal_collection('Contributor')
```
## Load simple attribute

Provide as parameter a dict or a list of dict

```python
technology=client_metal.get_metal_collection('Technology')
uuid=technology.metal_load({'name': 'Chroma'}, False)
```

<details>
  <summary>Example response</summary>
```json
[UUID('b0b0f7b2-1a16-4a7f-8a1f-cb6c93db5137')]
```
</details>

Or provide multiple objects to load:

```python
to_load = [{'name': 'Chroma'},{'name': 'Qdrant'}]
technology=client_metal.get_metal_collection('Technology')
uuid=technology.metal_load(to_load, False)
```

## Load simple attributes and references

Provide a dict with the fields and values, again you can provide a list of objects to load multiples objects, you'd have a list of uuids returned:

```python
to_load=[{'question': 'who?', 'hasCategory': uuid}]

technology_property=client_metal.get_metal_collection('technology_property')
uuid_target=technology_property.metal_load({'name': 'Hybrid Search'}, False)

to_load={'question': 'who?', 'hasProperty': uuid_target}

technology.metal_load(to_load, False)
```

You can collapse this previous load into something more condensed if your target uuid already exist and you have a query that uniquely identify it. 
Rather than first fetching the uuid and then loading your ref, you can directly provide the search query within the load function:

## Load simple attributes and unresolved references

```python
to_load={'question': 'who?', 'hasProperty': 'name=Hybrid Search'}
technology.metal_load(to_load, False)
```

`'name=Hybrid Search'` is metal query syntax, look at **[query](query_data.md)** if you're not familiar yet with the query syntax.

The system resolves the query to a uuid, if there is a unique match, the object is loaded, if there is more than one match, an exception is raised, asking you to resolve the reference yourself.

I am using it a lot when I have normalised data or data with unique ids, most likely on tokenized field as it highly reduces the risks of multiples uuids matching.

## Load pure reference

if you want to load one or more references between objects:
You can use two syntax, array and object
- as an array provide each ref as an array of 3 values, from_uuid, from_property and to `[['<uuid>', '<hasRef>', '<uuid>'], ['<uuid>', '<hasRef>', '<uuid>'], ...]`
- as dict provide for each ref an object like: `[{from_uuid:<uuid>, from_property: '<hasRef>',to:<uuid>}, {from_uuid:<uuid>, from_property: '<hasRef>',to:<uuid>}, ...]`

In this example, we use an unresolved uuid for the source and we provide directly a uuid for the target.

```python
uuid_target=technology_property.metal_load({'name': 'Replica Consistency'}, False)
technology.metal_load(['name=weaviate', 'hasProperty', uuid_target[0]], False)
```

Check the results:

```python
technology.q('name=weaviate', 'hasProperty:name')
```

## Load two-way reference

:::info
Loading two-way ref requires to first register opposite relationships using
`register_opposite` method on your collection.  
Ex: Technology.metal.register_opposite('hasProperty', 'propertyOf')
More details: **[Register opposite relationships](init_metal.md#register-opposite-relationships)** 
:::


### When Loading an Object

Use the operator `<>` to load a two-way reference.

**Syntax**: `{'<>reference_name': <uuid>}`

```python
uuid_target=technology_property.metal_load({'name': 'Sorted Set'}, False)
to_load=[{'name': 'redis', '<>hasProperty': uuid_target}]
uuid=technology.metal_load(to_load, False)
```

This would load 2 references:
`redis hasProperty Sorted Set` and `Sorted Set propertyOf redis`

### When Loading Pure Reference

**Syntax**: `[<uuid>, '<>ref_name', <uuid>]` or `{from_uuid:<uuid>, from_property: '<>ref_name',to:<uuid>}`

```python
uuid_source=technology.metal_load({'name': 'LlamaIndex'}, False)
uuid_target=technology_property.metal_load({'name': 'Framework'}, False)
technology.metal_load([uuid_source[0], '<>hasProperty', uuid_target[0]], False)
```

## Update already existing object

Provide the uuid of the object to update along with the attributes and refs to load:

```python
milvus=technology.q('name=milvus')
uuid_milvus = milvus[0]['uuid']
technology.metal_load({'uuid': uuid_milvus, 'description': 'open-source vector database'}, False)
```

