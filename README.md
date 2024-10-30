# Full Metal Weaviate

High level wrapper for Weaviate: iterate faster while reducing boilerplate. 

Full Metal Weaviate propose two basic boilerplate-free intuitive functions:

`.query` and `.load`

`.query` makes it easy to query your data using a boilerplate-free syntax.

`.load` makes it easy to load your data.


It turn this search query:

```
response = jeopardy.query.fetch_objects(
    filters=Filter.by_property("round").equal("Double Jeopardy!"),
    return_references=[
        QueryReference(
            link_on="hasCategory",
            return_properties=["title"]
        ),
    ]
)
```

into this one-liner:

```
response = jeopardy.query('round=Double Jeopardy', 'hasCategory.title')
```

or this (2 way cross-references loading):

```
category = client.collections.get("JeopardyCategory")

category_obj_uuid = category.query.fetch_objects(
    filters=Filter.by_property("name").equal("category_name"),
    limit=3
)

questions = client.collections.get("JeopardyQuestion")
questions.data.reference_add(
    from_uuid=question_obj_uuid,
    from_property="hasCategory",
    to=category_obj_uuid
)

categories = client.collections.get("JeopardyCategory")
categories.data.reference_add({from_uuid=category_obj_uuid,from_property="hasQuestion",to=question_obj_uuid})
```

into this

```
questions = client.get_metal_collection("JeopardyQuestion")
questions.load([name=category_name,"<>hasQuestion",question_obj_uuid])
```

Use `<>` operator to create 2 way relationships. 
The library automatically picks up the registered opposite relationships and create the 2 way refs for you.

## Installation

```
pip install git+https://github.com/thedeepengine/full-metal-weaviate.git
```

## Usage

### Metal Queries

#### Attribute filtering

```
# equal
clt.query('name=Tensorflow')

# like
clt.query('name~mxnet')

# less than 
clt.query('github_stars>=10000')

# any
array_to_filter=['tensorflow', 'mxnet']
clt.query('name any array_to_filter', array_to_filter)
```

#### logical filtering


```
# and 
client.query('github_stars>=10000 & primary_language=python')

# or
client.query('github_stars>=10000 | primary_language=python')
```

#### reference filtering (a.k.a nested queries)

- pure nested query: `hasProperty.name ~ flexible`
- logical and on simple attribute and nested: `name=value & hasProperty.name=value`
- Deeply nested query: `hasProperty.hasCategory.instanceOf.name~*New*`

### Metal Load

Simple ref loading

```

```

two-way ref loading

```

```



### Also featured in Full Metal Weaviate

#### Simplified return_references

Specifiying fields to return is also less verbose and reduced to a minimal syntax:

For example:

- return field attributes: `name,desc`
- return field and references: `name,desc,hasRef1:name,hasRef2:name,desc`
- return field vector along with classic fields: `name,desc,vector`
- return named vector along: `name,desc,vector:vectorname`
- return field along with deeply nested ref: `name,hasRef1.hasRef3:name)` or `name,hasRef1>(hasRef3:name)`
- return named vector of reference: `name,hasRef1:name,vector:vector_name`

#### Integrated matching

When loading references, you can provide a query rather than a uuid. If the query returns a single item, the reference will be loaded, if not it raises an exception:

```
questions = client.get_metal_collection("JeopardyQuestion")
questions.load({from_uuid='name=myquestion',from_property="hasCategory",to='name=category'})
```

#### Rollback on failure

Sometimes you just want to quickly roll back the last load operation.
If you load your data using metal_load, if an exception is raised during the loading process and set the parameter rollback to `true`. whats been loaded so far will be deleted.

Also, Full Metal registers N (default to 10) last load operations, so you can see what has been loaded.

### Disclaimer

Full Metal Weaviate is currently in alpha and under active development. It is not recommended for production use as it may contain bugs, incomplete features, and can undergo significant changes. Use at your own risk.