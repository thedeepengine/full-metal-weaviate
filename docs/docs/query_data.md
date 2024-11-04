---
sidebar_position: 3
---

# Query Data

:::info TL;DR

Query is a composition of four basic building blocks:

**attribute:** name=value_name  
**logical:** name=value_name&age=value_age|desc=desc_value  
**refs:** hasChildren.name=name_value  
**refs deeply nested:** hasChildren.hasProperty.name=name_value

With this set of four rules, more complex queries can be built
:::

To query data, first get the metal collection:



```python
import weaviate
from full_metal_weaviate import get_metal_client

metal_client = get_metal_client('<your_weaviate_client>')
Technology=metal_client.get_metal_collection('Technology')
#collection_name = metal_client.get_metal_collection('<your_collection_name>')
```

## Simple attribute filtering

```python
response = Technology.metal_query('name=weaviate')
```

## Logical filtering

This is your common & and | operations.

```python
response = Technology.metal_query('name=weaviate|name=pinecone')
```

## Reference filtering

This is where you want to filter on a reference field.

```python
response = Technology.metal_query('hasProperty.name=HNSW')
```

Any other query is built out of these three basic blocks given some eventual extentions, like in deep reference filtering.


## Deep Reference filtering

Deeply nested references can be accessed using `.` notation. Nesting can happen at any depth.

```python
response = Technology.metal_query('hasProperty.hasCategory.name=efficiency')
```

# Return fields

:::info TL;DR

Return fields is a composition of four basic building blocks:


**attribute:** name,desc,title  
**references:** hasProperty.hasChildren:name  
**deeply nested references w/o previous level attribute:** hasProperty.hasChildren:name,desc  
**deeply nested references with previous level attribute:** hasProperty:name,title>(hasChildren:name,desc)  
:::

## Example

```python
response = Technology.metal_query('name=Weaviate',
return_fields='name,github,nb_stars,hasProperty:name,description')
```

## Complex return field examples

```python
('complex nesting with priority parenthesis',
'hasChildren:name>(hasAttrUuid:name,hasChildren:name>(hasAttrUuid:name,hasChildren:name))'),

('complex nesting, syntax mix',
'hasChildren:name>(hasAttrUuid.hasChildren:name,hasChildren:name)')
```
