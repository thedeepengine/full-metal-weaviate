---
sidebar_position: 3
---

# Load data

The loading part of this library is not the most exiting one.
As it currently only allows basic loading options. 
Most likely the most interesting part here is the two-way reference loading.

## Load attribute

```
JeopardyQuestion = metal_client.get_metal_collection('JeopardyQuestion')
JeopardyQuestion.metal_load({'question': 'why?'}, False)
```

## Load mix of attribute and reference

Provide a dict with the fields and values:

to_load={'question': 'who?', 'hasCategory': uuid}

```
JeopardyQuestion = metal_client.get_metal_collection('JeopardyQuestion')
uuid=JeopardyCategory.metal_load({'question': 'who?'}, False)

to_load={'question': 'who?', 'hasCategory': uuid}

JeopardyQuestion.metal_load(to_load, False)
```

## Load pure reference

```
JeopardyQuestion.metal_load({'hasCategory': 'why?'}, False)
```


### Load two-way reference

## Load mix

## Load with integrated query

This one is interesting if you work a lot with normalized data and you know some filterings returns a unique value. 

Most likely, you work within a subspace of values.

Rather than first searching for the reference uuid, you can have it integrated within your loading function call.

```

```

This one-liner is a wrapper for the following:

```

```



