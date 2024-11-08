---
sidebar_position: 2
---

# Quickstart

Full Metal Weaviate allows you to use a minimalistic syntax to query and load data into Weaviate.

:::info
There is no much doc yet about functions so `help(function_name)` in your terminal is probably one of your best friend if any doc exist, or feel free to deep dive into the code.
:::

## Installation

```
pip install git+https://github.com/thedeepengine/full-metal-weaviate.git
```

You can either load some [sample data](#test-with-sample-data) and test on this or directly [query your own data](#query-on-your-collection):

## Test with sample data

Running the function `get_sample_data` will create 3 collections to poke around with:


```python
from full_metal_weaviate import get_metal_client
from full_metal_weaviate.sample_data import get_sample_data
weaviate_client = <your weaviate client>

# this will create collections and load sample data
get_sample_data(weaviate_client)

# get metal client and collections
client_metal = get_metal_client(weaviate_client)
technology = client_metal.get_metal_collection('Technology')
technology_property = client_metal.get_metal_collection('TechnologyProperty')
contributor = client_metal.get_metal_collection('Contributor')
```

Start querying with a simple syntax:

```
# Filter on tech with name weaviate
technology.metal_query('name = weaviate')
```

<details>
  <summary>Example response</summary>
```json
[{'uuid': '050fd8f7-86ae-43fb-8cb6-139bd2bcfbf8',
  'properties': {'description': None,
   'nb_stars': None,
   'github': None,
   'release_date': None,
   'number_field': None,
   'name': 'weaviate'},
  'vector': {}}]
```
</details>

# Return the properties and refs you want 

Filter on tech with name weaviate and returns only the name attribute and hasProperty name reference.

First parameter is the filtering, second parameter the return field.

```python
technology.metal_query('name = weaviate','name,hasProperty:name')
```

The equivalent in classic graphql syntax would be:

<details>
  <summary>Example response</summary>
```json
[{'uuid': '050fd8f7-86ae-43fb-8cb6-139bd2bcfbf8',
  'properties': {'name': 'weaviate'},
  'references': {'hasProperty': [{'uuid': 'c34945d3-af30-43b8-a59a-7235e60bbb62',
     'properties': {'name': 'HNSW'},
     'vector': {}},
    {'uuid': 'f3d42422-c481-41d3-b044-211fe6ec6338',
     'properties': {'name': 'Dynamic Index'},
     'vector': {}},
    {'uuid': '945cedd8-e997-4815-978a-b7a180f17ccb',
     'properties': {'name': 'PQ'},
     'vector': {}},
    {'uuid': '92530bc7-00df-41b2-9ec2-db93469ff4c6',
     'properties': {'name': 'Flat Index'},
     'vector': {}}]},
  'vector': {}}]
```
</details>

## Filter on deeply nested refs

Here you filter on a deeply nested reference just using dot notation:

```
technology.q('hasProperty.hasCategory.name = adaptability', 'name,hasProperty.hasCategory:name')
```

Weaviate equivalent:

```
from weaviate.classes.query import Filter, QueryReference

technology.query.fetch_objects(
    filters = Filter.by_ref(link_on = "hasProperty").by_ref(link_on = "hasCategory").by_property("name").equal("adaptability"),
    return_properties = 'name',
    return_references = QueryReference(link_on = "hasProperty", return_properties = ["name"])
)
```

<details>
  <summary>Example response</summary>
```json
[{'uuid': '48cc9240-437d-436f-be3d-2b8ed6942eff',
  'properties': {'name': 'weaviate'},
  'references': {'hasProperty': [{'uuid': '35b3600b-19fc-4f27-8994-03c719b8fd0d',
     'properties': {},
     'references': {'hasCategory': [{'uuid': 'ac6d2e08-314c-4ac6-b4ce-436390fa41ba',
        'properties': {'name': 'performance'},
        'vector': {}}]},
     'vector': {}},
    {'uuid': '6625a75f-d84f-43c6-bbe8-d19b527f055f',
     'properties': {},
     'references': {'hasCategory': [{'uuid': 'd567e774-4dd1-4bd0-90f3-e199c088d761',
        'properties': {'name': 'adaptability'},
        'vector': {}}]},
     'vector': {}},
    {'uuid': '019fa50a-38fa-4352-a6c1-a0671fcc2709',
     'properties': {},
     'references': {'hasCategory': [{'uuid': 'e9351eaf-5e75-4cb3-94ba-1f2f012c030d',
        'properties': {'name': 'efficiency'},
        'vector': {}}]},
     'vector': {}},
    {'uuid': 'fe4f8e34-0b08-4c57-b22d-fdfd0cceabb3',
     'properties': {},
     'references': {'hasCategory': [{'uuid': 'a5bd962e-6a70-4188-9929-1e57c5cb4c5e',
        'properties': {'name': 'accuracy'},
        'vector': {}}]},
     'vector': {}}]},
  'vector': {}}]
```
</details>


## Query on your collection

```python
import weaviate
from full_metal_weaviate import get_metal_client

metal_client = get_metal_client('<your_weaviate_client>')
collection_name = metal_client.get_metal_collection('<your_collection_name>')

# simple attribute filtering
collection_name.metal_query('<field = value>')
```

Refer to **[query](query_data.md)** and **[load](load_data.md)** documentation for full syntax and loading options.

