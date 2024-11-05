# Full Metal Weaviate

[![Weaviate](https://img.shields.io/static/v1?label=Built%20with&message=Weaviate&color=green&style=flat-square)](https://weaviate.io/)

High level wrapper for Weaviate: iterate faster while reducing query and load boilerplate. 

# QUERY DATA WITH A SIMPLE SYNTAX

Query deeply nested data with `.` dot notation.

```python
collection.metal_query('hasChildren.hasProperty.name=name_value')
```

**attribute:** name=value_name  
**logical:** name=value_name&age=value_age|desc=desc_value  
**refs:** hasChildren.name=name_value  
**refs deeply nested:** hasChildren.hasProperty.name=name_value

# LOAD TWO-WAY REFS WITH A SINGLE LINER

Using `<>` operator:

```python
collection.metal_load([<uuid_source>,"<>yourRef",<uuid_target>])
```

`loads uuid_source->yourRef->uuid_target` and `uuid_target->oppositeOfYourRef->uuid_source`

# EXAMPLE

It turn this search query:

```
response = collection.query.fetch_objects(
    filters=Filter.by_ref(link_on="hasChildren").by_ref(link_on="hasProperty").by_property('name').equal(name_value),
    return_references=[
        QueryReference(
            link_on="hasChildren",
            return_references=QueryReference(link_on="hasProperty", 
            return_properties=["name"])
        ),
    ]
)
```

into this one-liner:

```
response = collection.query('hasChildren.hasProperty.name=name_value', 'hasChildren.hasProperty.name')
```

## Installation

```
pip install git+https://github.com/thedeepengine/full-metal-weaviate.git
```

## Documentation

https://thedeepengine.github.io/full-metal-weaviate/docs/quickstart/

### Limitation/next steps/things that needs to change

- Does not handle tenants
- Currently this library is monkey patching Weaviate, should be handled with proper class inheritance or something
- Not handling near_vector queries and other hybrid search, it might be nice to handle this as well

### Disclaimer

Full Metal Weaviate is currently in alpha and under active development. It is not recommended for production use as it may contain bugs, incomplete features, and can undergo significant changes. Use at your own risk.