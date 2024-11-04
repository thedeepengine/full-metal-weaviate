---
sidebar_position: 3
---

# Query Data

Querying data can be reduced down to 3 basic operations, other queries are derived from these 3 building blocks.

Let's first get the metal collection:

```
JeopardyQuestion = metal_client.get_metal_collection('JeopardyQuestion')
```

## Simple attribute filtering

```
response = JeopardyQuestion.metal_query('question=Double Jeopardy')
```

## Logical filtering

This is your common & and | operations.

```
response = jeopardy.metal_query('question=Double Jeopardy|question=Simple Jeopardy')
```

## Reference filtering

This is where you want to filter on a reference field.

```
response = jeopardy.metal_query('hasCategory.name=Politics')
```

Any other query is built out of these three basic blocks given some eventual extentions, like in deep reference filtering


## Deep Reference filtering

This is where you want to filter on a reference field.

```
response = jeopardy.metal_query('hasProperty.hasCategory.name=Politics')
```

Nesting can happen at any depth.
