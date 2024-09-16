import itertools
from full_metal_monad import __

reversedRelation = {'partOf': 'hasPart', 'childrenOf': 'hasChildren','instanceOf': 'hasInstance', 'dimensionOf': 'hasDimension', 'hasProperty': 'propertyOf', 'ontologyOf': 'hasOntology', 'hasParent': 'parentOf', 'propertyOf': 'hasProperty', 'llmConversationOf': 'hasLlmConversation'} 
upToDownRelation = {'hasPart': 'partOf', 'hasChildren': 'childrenOf', 'hasInstance': 'instanceOf', 'hasDimension': 'dimensionOf', 'propertyOf': 'hasProperty', 'hasOntology': 'ontologyOf', 'hasProperty': 'propertyOf', 'hasLlmConversation': 'llmConversationOf'}
allRelationsReversed = {**reversedRelation, **upToDownRelation}
allRelations = list(set(list(reversedRelation.keys()) + list(reversedRelation.values())))

def grapql_to_d3hierarchy_format(hierarchy):
    hierarchy.update({**hierarchy.pop('properties',{}), **hierarchy.pop('references', {})})
    keys=list(set(hierarchy.keys()) & set(allRelations))
    sub_level = __(hierarchy).get(keys).values()
    hierarchy = __(hierarchy).remove(keys)
    hierarchy['children'] = [grapql_to_d3hierarchy_format(child) for child in list(itertools.chain(*sub_level))]
    return hierarchy

def getUuid(someDict):
    uuid = someDict.get('uuid')
    if not uuid:
        uuid = someDict.get('id')
        if not uuid:
            uuid = someDict.get('_additional', {}).get('id')
    return uuid

def trimDictD3Hierarchy(nested_dict, withVector):
    title = nested_dict['name']
    trimmed = {'name': title, 
               'uuid': getUuid(nested_dict), 
               'content': nested_dict.get('content', ''), 
               'position': nested_dict.get('weight', ''),
               'comment': nested_dict.get('comment', '')}
    if withVector:
        trimmed['vector'] = nested_dict.get('vector', '')
    if 'children' in nested_dict:
        sorted = sort_by_weight(nested_dict['children'])
        trimmed['children'] = [trimDictD3Hierarchy(child, withVector) for child in sorted]
    return trimmed

def sort_by_weight(array):
    return sorted(array, key=lambda x: x.get('weight', 0))



def f():
    print('ddddd')