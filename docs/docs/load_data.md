---
sidebar_position: 3
---

# Load data

## Load attribute

```
JeopardyQuestion = metal_client.get_metal_collection('JeopardyQuestion')
JeopardyQuestion.metal_load({'question': 'why?'}, False)
```

## Load mix of attribute and reference

```
JeopardyQuestion = metal_client.get_metal_collection('JeopardyQuestion')
uuid=JeopardyCategory.metal_load({'question': 'who?'}, False)

JeopardyQuestion.metal_load({'question': 'who?', 
                            'hasCategory': uuid}, False)
```

## Load pure reference

```
JeopardyQuestion.metal_load({'hasCategory': 'why?'}, False)
```


### Load two-way reference

## Load mix

## Load with integrated query

