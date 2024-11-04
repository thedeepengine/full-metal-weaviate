---
sidebar_position: 4
---

# Load data

The loading part of this library is not the most useful one execpted maybe for the two-way reference loading.

Get your metal client and some collections:

```
client_weaviate=<your weaviate client>

# if you havent yet created the sample testing dataset run:
# sample_data(client_weaviate)

# get metal client and collections
client_metal=get_metal_client(client_weaviate)
Technology=client_metal.get_metal_collection('Technology')
TechnologyProperty=client_metal.get_metal_collection('TechnologyProperty')
Contributor=client_metal.get_metal_collection('Contributor')
```
## Load simple attribute

Provide as parameter a dict or a list of dict

```python
to_load = {'name': 'Chroma'}
# provide a list of object to load
# to_load = [{'name': 'Chroma'},{'name': 'Qdrant'}]

Technology=client_metal.get_metal_collection('Technology')
uuid=Technology.metal_load(to_load, False)
```

<details>
  <summary>Example response</summary>
```json
[UUID('b0b0f7b2-1a16-4a7f-8a1f-cb6c93db5137')]
```
</details>

## Load simple attributes and references

Provide a dict with the fields and values, again you can provide a list of objects to load multiples objects, you'd have a list of uuids returned:

```python
to_load=[{'question': 'who?', 'hasCategory': uuid}]

TechnologyProperty=client_metal.get_metal_collection('TechnologyProperty')
uuid_target=TechnologyProperty.metal_load({'name': 'Hybrid Search'}, False)

to_load={'question': 'who?', 'hasProperty': uuid_target}

Technology.metal_load(to_load, False)
```

You can collapse this previous load into something more condensed if your target uuid already exist and you have a query that uniquely identify it. 
Rather than first fetching the uuid and then loading your ref, you can directly provide the search query within the load function:

## Load simple attributes and unresolved references

```python
to_load={'question': 'who?', 'hasProperty': 'name=Hybrid Search'}
Technology.metal_load(to_load, False)
```

`'name=Hybrid Search'` is metal query syntax, look at **[query](query_data.md)** if you're not familiar yet with the query syntax.

The system woudl resolve the query to a uuid, if there is a unique match, the object is loaded, if there is more than one match, an exception is raised, asking you to resolve the reference yourself.

I am using it a lot when I have normalised data, most likely on tokenized field as it highly reduces the risks of multiples uuids matching.

## Load pure reference

if you want to load one or more references between objects:
You can use two syntax, array and object
- as an array provide each ref as an araray of 3 values, from_uuid, from_property and to `[['<uuid>', '<hasRef>', '<uuid>'], ['<uuid>', '<hasRef>', '<uuid>'], ...]`
- as dict provide for each ref an object like: `[{from_uuid:<uuid>, from_property: '<hasRef>',to:<uuid>}, {from_uuid:<uuid>, from_property: '<hasRef>',to:<uuid>}, ...]`

In this example, we use an unresolved uuid for the source and we provide directly a uuid for the target.

```python
uuid_target=TechnologyProperty.metal_load({'name': 'Replica Consistency'}, False)
Technology.metal_load(['name=weaviate', 'hasProperty', uuid_target[0]], False)
```

Check the results:

```python
Technology.q('name=weaviate', 'hasProperty:name')
```

## Load two-way reference

### when loading an object

Use the operator `<>` to load a two-way reference.

```python
uuid_target=TechnologyProperty.metal_load({'name': 'Sorted Set'}, False)
to_load=[{'name': 'redis', '<>hasProperty': uuid_target}]
uuid=Technology.metal_load(to_load, False)
```

This would like 2 references:
`redis hasProperty Sorted Set` and `Sorted Set propertyOf redis`

### when loading pure reference

```python
uuid_source=Technology.metal_load({'name': 'LlamaIndex'}, False)
uuid_target=TechnologyProperty.metal_load({'name': 'Framework'}, False)
Technology.metal_load([uuid_source[0], '<>hasProperty', uuid_target[0]], False)
```

## Update already existing object

Provide the uuid of the objec to update along with the attributes and refs to load:

```python
milvus=Technology.q('name=milvus')
uuid_milvus = milvus[0]['uuid']
Technology.metal_load({'uuid': uuid_milvus, 'description': 'open-source vector databse'}, False)
```

