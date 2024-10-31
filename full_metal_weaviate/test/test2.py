import os
import unittest
import copy
import weaviate.classes as wvc
from weaviate.classes.config import Property, DataType, ReferenceProperty, Configure, Tokenization
from main import get_metal_client
from weaviate import WeaviateClient
from weaviate.connect import ConnectionParams
from weaviate.auth import AuthApiKey
from dataclasses import dataclass

from weaviate_op import get_filter_compiler, parse_filter, get_return_field_compiler, get_weaviate_return_fields
from utils import FMWParseFilterException, MetalClientException
from pyparsing import ParseException

def get_weaviate_client():
    weaviate_host = os.getenv('WEAVIATE_HTTP_HOST', 'localhost')
    # client_weaviate=get_metal_client(weaviate_host, opposite_refs)

    api_key_weaviate = os.getenv('WEAVIATE_API_KEY')
    weaviate_client = WeaviateClient(
    connection_params=ConnectionParams.from_params(
        http_host=weaviate_host,
        http_port="8080",
        http_secure=False,
        grpc_host=weaviate_host,
        grpc_port="50051",
        grpc_secure=False,
    ),
    auth_client_secret=AuthApiKey(api_key_weaviate))

    weaviate_client.connect()
    return weaviate_client

client_weaviate=get_weaviate_client()

client_metal=get_metal_client(client_weaviate)
clt1=client_metal.get_metal_collection('Clt1')
clt2=client_metal.get_metal_collection('clt2')


########################################
## create collection
########################################

client_metal.collections.delete('clt1')
client_metal.collections.delete('clt2')

clt1 = client_metal.collections.create(
    name="Clt1",
    properties=[
        Property(name="text_field", data_type=DataType.TEXT),
        Property(name="int_field", data_type=DataType.INT),
        Property(name="date_field", data_type=DataType.DATE),
        Property(name="float_field", data_type=DataType.DATE),
        Property(name="field_tokenized_field", data_type=DataType.TEXT, tokenization= Tokenization.FIELD, description="field tokenized"),
    ],
    vectorizer_config=[Configure.NamedVectors.none(name="vect_field")]
)

clt2 = client_metal.collections.create(
    name="Clt2",
    properties=[
        Property(name="text_field_clt2", data_type=DataType.TEXT)
    ]
)

clt1.config.add_reference(ReferenceProperty(name="hasRef1",target_collection="clt2"))
clt2.config.add_reference(ReferenceProperty(name="refOf1",target_collection="clt1"))

clt1.config.add_reference(ReferenceProperty(name="hasRef2",target_collection="clt2"))
clt2.config.add_reference(ReferenceProperty(name="refOf2",target_collection="clt1"))



##################################################
############ get_collection 
##################################################

client.close()
client = weaviate.connect_to_local()
metal_client=get_metal_client(client)
clt1=metal_client.get_metal_collection('clt1')
clt2=metal_client.get_metal_collection('clt2')

clt1.metal.register_opposite('hasRef1', 'refOf1')
clt1.metal.register_opposite('hasRef2', 'refOf2')

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
('filter on uuid with field name',
 'uuid=11111111-1111-1111-1111-111111111111'),
('filter on uuid without field name',
 '11111111-1111-1111-1111-111111111111'),
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
('complex logical filter with priority parenthesis',
 '((name = John) & (age > 30)) | ((department = Sales) & (salary >= 50000))'),
('complex logical filter without priority parenthesis',
 'name = John & age > 30 | department = Sales & salary >= 50000'),

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
                i.recorded=res
            except MetalClientException as e:
                i.recorded = e
                pass
    return to_test

tested=record_parse_filter(to_test)

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

i=return_field_data('wrong syntax','name=value')

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

# load simple attribute

to_load=[{'text_field': 'test'}]
uuid=clt1.metal_load(to_load, False)
clt1.metal_query(uuid[0])


# load mix attributes and references

uuid_target=clt2.metal_load({'text_field_clt2': 'test__0001'}, False)[0]
uuid1=clt1.metal_load({'text_field': 'w1','hasRef1': uuid_target}, False)

clt1.metal_query(uuid1[0])
clt1.metal_query(uuid1[0], 'hasRef1:text_field_clt2')


# load pure references

## using array format
uuid_source=str(clt1.metal_load({'text_field': 'test__0001'}, False)[0])
uuid_target=clt2.metal_load({'text_field_clt2': 'test__0002'}, False)[0]

to_load=[[uuid_source,'hasRef1',uuid_target]]
clt1.l(to_load, False)

clt1.q(uuid_source, 'hasRef1:text_field_clt2')

## using dict format
uuid_source=str(clt1.metal_load({'text_field': 'test__0001'}, False)[0])
uuid_target=clt2.metal_load({'text_field_clt2': 'test__0002'}, False)[0]

to_load=[{'from_uuid': uuid_source, 'from_property':'hasRef1', 'to':uuid_target}]
clt1.l(to_load, False)

clt1.q(uuid_source, 'hasRef1:text_field_clt2')

## load 2 way references
uuid_source=clt1.metal_load({'text_field': 'test__0001'}, False)[0]
uuid_target=clt2.metal_load({'text_field_clt2': 'test__0002'}, False)[0]
to_load=[{'from_uuid': uuid_source, 'from_property':'<>hasRef1', 'to':uuid_target}]

clt1.l(to_load, False)

clt1.q(uuid_source, 'hasRef1:text_field')
clt2.q(uuid_target, 'refOf1:text_field')

# load unresolved refs
## for pure refeference
uuid_source=clt1.metal_load({'text_field': 'test__0001'}, False)[0]
uuid_target=clt2.metal_load({'text_field_clt2': 'test__pure_ref'}, False)[0]

q='text_field_clt2=test__pure_ref'

to_load=[{'from_uuid': uuid_source, 'from_property':'<>hasRef1', 'to': q}]

clt1.l(to_load, False)

clt1.q(uuid_source, 'hasRef1:text_field_clt2')
clt2.q(uuid_target, 'refOf1:text_field')


clt1.q(uuid_target, 'refOf1:text_field')

## for mix ref
uuid_target=clt2.metal_load({'text_field_clt2': 'test__123456'}, False)[0]

uuid_source=clt1.metal_load({'text_field': 'w1',
                       'hasRef1': 'text_field_clt2=test__123456'}, False)

clt1.q(uuid_source, 'refOf1:text_field')

get_translate_filter(clt1, 'text_field=test__123456')

clt1.q(uuid1[0])

metal_query(clt1,uuid1[0])

clt1.q(uuid1[0])

# prop with vector / named vector
# pure ref one way dict
# pure ref two way dict
# pure ref array
# mix prop and resolved refs
# mix prop and resolved 2 way refs
# already existing ref


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


# ----///////////
client_metal=get_metal_client(client_weaviate)
clt1=client_metal.get_metal_collection('Clt1')
clt2=client_metal.get_metal_collection('clt2')



self=clt1
filters_str='text_field~*'
return_fields='hasRef1'
return_fields='hasRef1:text_field_clt2'
clt1.metal_query(uuid2[0], 'hasRef1')
clt1.metal_query(filters_str, return_fields)


clt1.metal_load({'question': 'who?'})



uuid2=str(node_col.l({'fname': 'test__0002'}, False)[0])
uuid3=str(node_col.l({'fname': 'test__0003'}, False)[0])





client = weaviate.connect_to_local()
metal_client=get_metal_client(client)
JeopardyQuestion=metal_client.get_metal_collection('JeopardyQuestion')
JeopardyCategory=metal_client.get_metal_collection('JeopardyCategory')


search_unique_ref_uuid(JeopardyQuestion, 'Politics')

r=col.q('politics')




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
