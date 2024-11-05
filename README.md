# Full Metal Weaviate

High level wrapper for Weaviate: iterate faster while reducing boilerplate. 

Full Metal Weaviate propose two basic boilerplate-free intuitive functions:

`.metal_query` and `.metal_load` (aliased as `.q` and `.l`)

`.q` makes it easy to query your data using a boilerplate-free syntax.

`.l` makes it easy to load your data.


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

Use `<>` operator to create two-way relationships. 
The library automatically picks up the registered opposite relationships and create the two-way refs for you.

## Installation

```
pip install git+https://github.com/thedeepengine/full-metal-weaviate.git
```

Along with querying and loading, fmw provide a simplified way to get your returned fields, as its less verbose and reduced to a minimal syntax:

For example:

- return field attributes: `name,desc`
- return field and references: `name,desc,hasRef1:name,hasRef2:name,desc`
- return field vector along with classic fields: `name,desc,vector`
- return named vector along: `name,desc,vector:vectorname`
- return field along with deeply nested ref: `name,hasRef1.hasRef3:name)` or `name,hasRef1>(hasRef3:name)`
- return named vector of reference: `name,hasRef1:name,vector:vector_name`

#### Unresolved reference

When loading references, you can provide a query rather than a uuid. If the query returns a single item, the reference will be loaded, if not it raises an exception:

### Limitation/next steps/things that needs to change

- Does not handle tenants yet
- Currently this library is monkey patching Weaviate, should be handled with proper class inheritance or something
- Not handling near_vector queries and other hybrid search, it might be nice to handle this as well

### Disclaimer

Full Metal Weaviate is currently in alpha and under active development. It is not recommended for production use as it may contain bugs, incomplete features, and can undergo significant changes. Use at your own risk.