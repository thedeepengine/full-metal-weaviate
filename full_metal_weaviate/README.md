# Full Metal Weaviate

High level wrapper for Weaviate meant to reduce boilerplates and iterate faster. We propose essentially two basic high-sugary syntax functions:


metal_query and metal_load (aliased as .q and .l)


It turn this:
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

into:

```
response = jeopardy.q('round=Double Jeopardy', 'hasCategory.title')
```

or this (2 way cross-references loading):

```
category = client.collections.get("JeopardyCategory")

category_obj_id = category.query.fetch_objects(
    filters=Filter.by_property("name").equal("category_name"),
    limit=3
)

questions = client.collections.get("JeopardyQuestion")
questions.data.reference_add(
    from_uuid=question_obj_id,
    from_property="hasCategory",
    to=category_obj_id
)

categories = client.collections.get("JeopardyCategory")
categories.data.reference_add({from_uuid=category_obj_id,from_property="hasQuestion",to=question_obj_id})
```

into this

```
questions = client.get_metal_collection("JeopardyQuestion")
questions.metal_load({from_uuid=category_obj_id,from_property="hasQuestion",to=question_obj_id})
???

questions = client.get_metal_collection("JeopardyQuestion")
questions.l([category_obj_id,"hasQuestion",question_obj_id])
```

or event shorter: 

```
questions = client.get_metal_collection("JeopardyQuestion")
questions.l({from_uuid='name=myquestion',from_property="hasCategory",to='name=category'})
```

To balance with this seamingly too sugary syntax. Full Metal enforces constraints on what is parsed, raises explicit messages on properties or references typos and is verbose and dry run by default, so you can double check what was infered.

Its not meant to be used in production and just like for any other query system, especially if you use raw query syntax for your classic weaviate queries, you have to be cautious of injection risks.

## Metal query

```
response = jeopardy.query.fetch_objects(
    filters=Filter.by_property("round").equal("Double Jeopardy!"),
    limit=3
)
```

### Single condition filter

```
{
  Get {
    JeopardyQuestion(
      where: {
        path: ["round"],
        operator: Equal,
        valueText: "Double Jeopardy!"
      }
    ) {
      question
      answer
      round
    }
  }
}
```

```
JeopardyQuestion.q('round=Double Jeopardy!')

JeopardyQuestion.q('round=Double Jeopardy & name=ffjfuds | name = fjdifos=fjufdfd')


JeopardyQuestion.q('round=Double Jeopardy!;;hasChildren.value=3,hasProperty.name=|ontologyOf.name=fff')

JeopardyQuestion.q('round=Double Jeopardy!;;hasChildren.value=3,hasProperty.name=|ontologyOf.name=thats a name for ontology ;; double commas= as well')
```

### Nested conditions filter (or filter on references)

Filter on references

```
name=articleName;;hasCategory:name
```

```
{
  articles(where: {name: "articleName", hasCategory: {name: "sport"}}) {
    id
    name
    hasCategory {
      id
      name
    }
  }
}

```

### Logical conditions filter

```
jeopardy = client.collections.get("JeopardyQuestion")
response = jeopardy.query.fetch_objects(
    filters=Filter.by_property("answer").like("*bird*") &
            (Filter.by_property("points").greater_than(700) | Filter.by_property("points").less_than(300)),
    limit=3
)

```
#### and

#### or

## Metal load

metal loads proposes some easy syntax to load objects and references.
Especially 2 ways references.

### Loading an object with a two way cross-reference

Adding '<>' before the reference you want to load tells the system this should be
a cross-reference, and a 1 way cross-reference has no '<>'


```
table_to_load = {'name': 'Here is the question',
                '<>hasCategory': 'name=category_name_2_way',
                'hasCategory': 'name=category_name_1_way'}

uuid_table=questions.metal_load(table_to_load)
```

### Loading 2 way cross-references




## What is the cost of such sugar syntax

Full Metal Weaviate enforces a lot of constraints.


Here we name things, assign properties, zoom-in and zoom-out, move up and down the abstraction ladder, etc.. collectively we call that disambiguation and the tool to disambiguate: a disambiguation engine.

## Register 2 way references

### When you get your weaviate client
```
opposite_refs = ['JeopardyQuestion.hasCategory<>JeopardyCategory.hasQuestion',
                 'JeopardyQuestion.hasOwner<>JeopardyUsers.ownerOf']

client=get_metal_client(weaviate_client, opposite_refs)
# then load your data using <>:
JeopardyQuestion=client.get_metal_collection('JeopardyQuestion')
JeopardyQuestion.metal_load({'question': 'my question', '<>hasCategory': category_uuid})
```

### When your client is already loaded

```
opposite_refs = ['JeopardyQuestion.hasCategory<>JeopardyCategory.hasQuestion',
                 'JeopardyQuestion.hasOwner<>JeopardyUsers.ownerOf']

client=get_metal_client(weaviate_client)
client.register_opposite_ref(opposite_refs)
# then load your data using <>:
JeopardyQuestion=client.get_metal_collection('JeopardyQuestion')
JeopardyQuestion.metal_load({'question': 'my question', '<>hasCategory': category_uuid})
```