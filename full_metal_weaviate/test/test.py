import os
import unittest
import copy
import weaviate
import weaviate.classes as wvc
from weaviate.classes.config import Property, DataType, ReferenceProperty, Configure, Tokenization
from full_metal_weaviate import get_metal_client, get_weaviate_client
from weaviate import WeaviateClient
from weaviate.connect import ConnectionParams
from weaviate.auth import AuthApiKey
from dataclasses import dataclass

from full_metal_weaviate.weaviate_op import (get_filter_compiler, parse_filter, get_return_field_compiler, get_weaviate_return_fields, get_translate_filter, parse_return_field)
from full_metal_weaviate.utils import MetalClientException, console
from full_metal_weaviate.sample_data import create_sample_collection

TECH_CLT = 'Technology'
CONTRIB_CLT = 'Contributor'
TECH_PROP_CLT = 'technology_property'
PROP_CATEGORY_CLT = 'PropertyCategory'

##################################################
############ parse_filter
##################################################
@dataclass
class parse_filter_test:
    desc: str
    filters_str: str
    recorded: dict = None

parse_filter_instance = [
('filter on simple attribute on equal operator for string type',
'string_field=hhh'),
('filter on simple attribute on like operator for string type',
 'string_field~*hhh*'),
('filter on simple attribute on equal operator for int type',
 'integer_field=1'),
('filter on simple attribute on equal operator for float type',
 'float_field=1.0'),
('filter on simple attribute on equal operator for date type',
 'date_field=2024/01/01'),
('filter on ref',
 'hasCategory.title=value'),
('logical and filter',
 'name=nameValue&content=contentValue'),
('logical or filter',
 'name=nameValue|content=contentValue'),
('logical and and or filter',
 'name=nameValue|content=contentValue&description~me'),
('deeply nested',
 'hasChildren.hasOntology.hasProperty.name=property_value'),
('complex query mix of simple attribute, logical, ref',
 'question=question_value&answer=answer_value|hasCategory.title=category_value'),
# ('complex logical filter with priority parenthesis',
#  '((name = John) & (age > 30)) | ((department = Sales) & (salary >= 50000))'),
# ('complex logical filter without priority parenthesis',
#  'name = John & age > 30 | department = Sales & salary >= 50000'),
 ('wrong syntax just a word',
  'fieldcantbealone'),
   ('wrong syntax operator',
  'field:name')
]

to_test = [parse_filter_test(i[0], i[1]) for i in parse_filter_instance]

def record_parse_filter(to_test):
    compiler=get_filter_compiler()
    to_test=copy.deepcopy(to_test)
    for i in to_test:
            try:
                res=parse_filter(compiler, i.filters_str)
                print(i.filters_str)
                print(res)
                i.recorded=res
            except MetalClientException as e:
                i.recorded = e
                pass
    return to_test

weaviate_client = get_weaviate_client()
client=get_metal_client(weaviate_client)
create_sample_collection(client)

tested=record_parse_filter(to_test)

technology,technology_property,property_category,contributor=get_clt(weaviate_client)

get_translate_filter_instance =[
('filter on uuid with field name',
 'uuid=11111111-1111-1111-1111-111111111111'),
('filter on uuid without field name',
 '11111111-1111-1111-1111-111111111111'),
]

to_test = [parse_filter_test(i[0], i[1]) for i in get_translate_filter_instance]

def record_get_translate_filter(to_test):
    compiler=get_filter_compiler()
    to_test=copy.deepcopy(to_test)
    for i in to_test[:5]:
            try:
                res=get_translate_filter(technology, i.filters_str)
                i.recorded=res
            except MetalClientException as e:
                i.recorded = e
                pass
    return to_test

tested=record_get_translate_filter(to_test)

#############################################
############ return fields
#############################################

@dataclass
class return_field_data:
    desc: str
    return_field: str
    recorded: dict = None

return_field_instance=[
    ('simple attribute',
    'name'),
    ('multiple attribute', 
   'name,date,attr'),
   ('attribute and default vector',
   'name,date,attr,vector'),
   ('attribute and named vector',
   'name,date,attr,vector:content'),
   ('simple attribute with named vector and reference',
    'name,date,attr,vector:content,name,hasChildren:name'),
    ('vector for reference',
    'name,date,attr,vector:content,name,hasChildren:name,vector'),
    ('simple reference',
   'hasChildren:name'),
   ('simple attribute and reference',
    'name,hasChildren:name'),
    ('multiple simple attributes and mutilple reference attributes',
   'name,date,hasChildren:name,value,content'),
   ('multiple reference attributes',
    'hasChildren:name,value,content'),
    ('multiple references',
    'hasChildren:name,hasInstance:name'),
    ('deeply nested 2 level reference',
    'hasChildren.hasChildren:name'),
    ('multiple reference, multiple attributes',
    'hasChildren:name,value,hasChildren:name,desc'),
    ('deeply nested with first level requiring fields',
    'hasChildren:name>hasChildren:name'),
    ('deeply nested with > syntax',
    'hasChildren:name>hasChildren:name>hasChildren:name'),
    ('complex deeply nested with mix nesting syntax',
    'hasChildren.hasChildren.hasChildren:name>hasAttrUuid:name'),
    ('complex nesting with priority parenthesis',
    'hasChildren:name>(hasAttrUuid:name,hasChildren:name>(hasAttrUuid:name,hasChildren:name))'),
    ('complex nesting, syntax mix',
    'hasChildren:name>(hasAttrUuid.hasChildren:name,hasChildren:name)'),
    ('complex nesting',
    'hasChildren>(hasAttrUuid.hasChildren:name,hasChildren:name)'),
    ('complex nesting, multiple nested references, multiples attributes',
    'hasChildren:name>(hasAttrUuid.hasChildren:name,value,attr,hasChildren:name,hasAttr:name)'),
    ('complex nesting, deeply nested mix syntax',
    'hasOntology:name>(hasAttrUuid.hasChildren:name,hasOntology:name>hasChildren:name)'),
    ('complex nesting',
    'hasOntology:name>(hasAttrUuid.hasChildren:name>hasOntology:name>(hasChildren:name,hasAttr:name))'),
    ('complex nesting',
    'name,date,hasOntology:name,hasChildren>(name,date,hasChildren:name,hasAttrUuid:name>hasChildren:name),hasAttrUuid:name'),
    ('complex nesting',
    'name,date,hasOntology:name,hasChildren>(hasChildren:name,hasAttrUuid:name>hasChildren:name),hasAttrUuid:name'),
    ('complex nesting',
    'name,hasChildren>(name,date,hasChildren:name,hasAttrUuid:name>hasChildren:name),hasAttrUuid:name'),
    ('complex nesting',
    'fname,hasChildren:attrName,name,fname,value,content,date>(hasAttr:name>(hasAttrUuid:name),hasChildren:name,value)'),
    ('wrong syntax',
     'name=value')
    ]

to_test = [return_field_data(i[0], i[1]) for i in return_field_instance]

def record_parse_return_field(to_test):
    compiler=get_return_field_compiler()
    to_test=copy.deepcopy(to_test)
    for i in to_test:
        try:
            res=parse_return_field(compiler, i.return_field)
            i.recorded=res
        except MetalClientException as e:
            i.recorded=e
            pass
    return to_test

tested=record_parse_return_field(to_test)

def record_get_weaviate_return_fields(to_test):
    compiler=get_return_field_compiler()
    to_test=copy.deepcopy(to_test)
    for i in to_test:
            try:
                res=get_weaviate_return_fields(compiler, i.return_field)
                i.recorded=res
            except MetalClientException as e:
                i.recorded=MetalClientException(i)
                pass
    return to_test

tested=record_get_weaviate_return_fields(to_test)

#############################################
############ metal_load
#############################################

#! load simple attribute
to_load=[{'name': 'test'}]
uuid=technology.metal_load(to_load, False)
technology.metal_query(uuid[0])

#! load mix attributes and references
uuid_target=technology_property.metal_load({'text_field_clt2': 'test__0001'}, False)[0]
uuid1=Technology.metal_load({'text_field': 'w1','hasProperty': uuid_target}, False)

Technology.metal_query(uuid1[0])
Technology.metal_query(uuid1[0], 'hasProperty:text_field_clt2')


#! load mix attributes and multiple references
uuid_target1=technology_property.metal_load({'text_field_clt2': 'test__0001'}, False)[0]
uuid_target2=technology_property.metal_load({'text_field_clt2': 'test__0001'}, False)[0]
uuid1=Technology.metal_load({'text_field': 'w1','hasProperty': [uuid_target1, uuid_target2]}, False)

Technology.metal_query(uuid1[0])
Technology.metal_query(uuid1[0], 'hasProperty:text_field_clt2')

# load pure references

## using array format
uuid_source=str(Technology.metal_load({'text_field': 'test__0001'}, False)[0])
uuid_target=technology_property.metal_load({'text_field_clt2': 'test__0002'}, False)[0]

to_load=[[uuid_source,'hasProperty',uuid_target]]
Technology.l(to_load, False)

Technology.q(uuid_source, 'hasProperty:text_field_clt2')

## using dict format
uuid_source=str(Technology.metal_load({'text_field': 'test__0001'}, False)[0])
uuid_target=technology_property.metal_load({'text_field_clt2': 'test__0002'}, False)[0]

to_load=[{'from_uuid': uuid_source, 'from_property':'hasProperty', 'to':uuid_target}]
Technology.l(to_load, False)

Technology.q(uuid_source, 'hasProperty:text_field_clt2')

## load 2 way references
uuid_source=Technology.metal_load({'text_field': 'test__0001'}, False)[0]
uuid_target=technology_property.metal_load({'text_field_clt2': 'test__0002'}, False)[0]
to_load=[{'from_uuid': uuid_source, 'from_property':'<>hasProperty', 'to':uuid_target}]

Technology.l(to_load, False)

Technology.q(uuid_source, 'hasProperty:text_field')
technology_property.q(uuid_target, 'propertyOf:text_field')

# load unresolved refs

## for mix ref
uuid_target=technology_property.metal_load({'text_field_clt2': 'test__123456'}, False)[0]

uuid_source=Technology.metal_load({'text_field': 'w1',
                       'hasProperty': 'text_field_clt2=test__123456'}, False)

Technology.q(uuid_source, 'propertyOf:text_field')

## for mix ref with multiple unresolved refs
uuid_target=technology_property.metal_load({'text_field_clt2': 'test__123456'}, False)[0]
uuid_target=technology_property.metal_load({'text_field_clt2': 'test__891'}, False)[0]

uuid_source=Technology.metal_load({'text_field': 'w1',
                       'hasProperty': ['text_field_clt2=test__123456', 'text_field_clt2=test__891']}, False)

Technology.q(uuid_source, 'propertyOf:text_field')

## for pure refeference
uuid_source=Technology.metal_load({'text_field': 'test__0001'}, False)[0]
uuid_target=technology_property.metal_load({'text_field_clt2': 'test__pure_ref'}, False)[0]

q='text_field_clt2=test__pure_ref'

to_load=[{'from_uuid': uuid_source, 'from_property':'<>hasProperty', 'to': q}]

Technology.l(to_load, False)

Technology.q(uuid_source, 'hasProperty:text_field_clt2')
technology_property.q(uuid_target, 'propertyOf:text_field')


Technology.q(uuid_target, 'propertyOf:text_field')

## update edge case
# saw issues if value to load is UUID type for pure uuid
# if array or not search: if obj.get(key) is not None and len(obj.get(key))>0}

#############################################
############ Metal Administration
#############################################

# get metal client

## from client weaviate
import weaviate

weaviate_client.close()
weaviate_client=get_weaviate_client()
metal_client=get_metal_client(weaviate_client)
JeopardyQuestion = metal_client.get_metal_collection('JeopardyQuestion')

# get metal collection

# register opposite

JeopardyQuestion.metal.register_opposite('hasCategory', 'hasQuestion')
JeopardyQuestion.metal.register_opposite('hasAssociatedQuestion', 'associatedQuestionOf')

# get registered opposite

JeopardyQuestion.metal.get_opposite('hasCategory')


# to be loaded in the deep engine doc

# metal_query
## filtering
## return fields

# metal_load
## attribute
## reference
### two-way reference
## mix
## manage already exisiting ref
## resolution

bed= metal_client.get_metal_collection('Bed')
calendar= metal_client.get_metal_collection('Calendar')
bed.metal.register_opposite('hasCalendar', 'calendarOf')
self=bed
col=bed
bed.q(filters_str='hasCalendar.status=Available')

calendar.q(return_fields='status')

unit= metal_client.get_metal_collection('Unit')
unit.metal.register_opposite('hasBed', 'bedOf')

unit.l({'uuid': '8fdbec00-f37c-401a-8cd3-6717b1409013', 
        'hasBed': '02cd0356-5ccb-4a1c-8ebc-4a47204147ea'}, False)


def get_
col.metal.context[]
col.metal.context['ref_target']['Bed']
col.metal.get_opposite('hasCalendar')

a=get_opposite(col.metal, 'hasCalendar', True)



col.metal.context['fields'][last_clt]['properties']
get_opposite(col.metal, 'hasCalendar', True)

to_load={'roommate_id': 'B',
 'gender': 'Female',
 'school': 'SDSU, Campanile Dr, San Diego, CA, USA',
 'hasProfileAttributes': '126e8c04-d146-45cb-b797-3c6225360977'}


uuid=self.l(
{'roommate_id': 'B',
 'gender': 'Female',
 'school': 'SDSU, Campanile Dr, San Diego, CA, USA',
 'hasProfileAttributes': '126e8c04-d146-45cb-b797-3c6225360977'}, False)

self.q(uuid[0], 'roommate_id,hasProfileAttributes:category')


roommate= metal_client.get_metal_collection('Roommate')
self=roommate
col=roommate
roommate.metal.register_opposite('hasProfileAttribute', 'profileAttributeOf')



