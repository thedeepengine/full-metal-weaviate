import os
import unittest
import copy
import weaviate
import weaviate.classes as wvc
from weaviate.classes.config import Property, DataType, ReferenceProperty, Configure, Tokenization
from main import get_metal_client, get_weaviate_client
from weaviate import WeaviateClient
from weaviate.connect import ConnectionParams
from weaviate.auth import AuthApiKey
from dataclasses import dataclass

from weaviate_op import get_filter_compiler, parse_filter, get_return_field_compiler, get_weaviate_return_fields
from utils import FMWParseFilterException, MetalClientException
from pyparsing import ParseException

from utils import console

TECH_CLT = 'Technology'
CONTRIB_CLT = 'Contributor'
TECH_PROP_CLT = 'TechnologyProperty'
PROP_CATEGORY_CLT = 'PropertyCategory'

client_weaviate.close()
del client_weaviate
client_weaviate=get_weaviate_client(True)
client_metal=get_metal_client(client_weaviate)
Technology=client_metal.get_metal_collection(TECH_CLT)
TechnologyProperty=client_metal.get_metal_collection(TECH_PROP_CLT)
PropertyCategory=client_metal.get_metal_collection(PROP_CATEGORY_CLT)
Contributor=client_metal.get_metal_collection(CONTRIB_CLT)

########################################
## create collection
########################################

def delete_sample_data(client):
    client.collections.delete(TECH_CLT)
    client.collections.delete(CONTRIB_CLT)
    client.collections.delete(TECH_PROP_CLT)
    client.collections.delete(PROP_CATEGORY_CLT)

def create_sample_collection(client, 
                tech_clt=TECH_CLT, 
                contrib_clt=CONTRIB_CLT, 
                tech_prop_clt=TECH_PROP_CLT,
                prop_category_clt=PROP_CATEGORY_CLT):
    clt_names=[tech_clt, contrib_clt, tech_prop_clt, prop_category_clt]
    existing_clt = [i for i in clt_names 
                     if client_weaviate.collections.exists(i)]
    if existing_clt:
        print(f'Warning: Collections {existing_clt} already exist. Delete or change sample data collection name parameters name')
        return
         
    Technology = client.collections.create(
        name=tech_clt,
        properties=[
            Property(name="name", data_type=DataType.TEXT, tokenization= Tokenization.FIELD),
            Property(name="description", data_type=DataType.TEXT),
            Property(name="github", data_type=DataType.TEXT),
            Property(name="nb_stars", data_type=DataType.INT),
            Property(name="release_date", data_type=DataType.DATE),
            Property(name="number_field", data_type=DataType.NUMBER),
        ],
        vectorizer_config=[Configure.NamedVectors.none(name="vect_field")]
    )

    TechnologyProperty = client.collections.create(
        name=tech_prop_clt,
        properties=[
            Property(name="name", data_type=DataType.TEXT),
            Property(name="description", data_type=DataType.TEXT)
        ]
    )

    PropertyCategory = client.collections.create(
        name=prop_category_clt,
        properties=[
            Property(name="name", data_type=DataType.TEXT),
            Property(name="description", data_type=DataType.TEXT),
            Property(name="coolness", data_type=DataType.TEXT_ARRAY)
        ]
    )

    Contributor = client.collections.create(
        name=contrib_clt,
        properties=[
            Property(name="name", data_type=DataType.TEXT)
        ]
    )

    Technology.config.add_reference(ReferenceProperty(name="hasProperty",target_collection=tech_prop_clt))
    TechnologyProperty.config.add_reference(ReferenceProperty(name="propertyOf",target_collection=tech_clt))

    Technology.config.add_reference(ReferenceProperty(name="hasContributor",target_collection=contrib_clt))
    Contributor.config.add_reference(ReferenceProperty(name="contributorOf",target_collection=tech_clt))

    TechnologyProperty.config.add_reference(ReferenceProperty(name="hasCategory",target_collection=prop_category_clt))
    PropertyCategory.config.add_reference(ReferenceProperty(name="categoryOf",target_collection=tech_prop_clt))

    existing_clt = all([client_weaviate.collections.exists(i) for i in clt_names])
    if existing_clt:
        console.print(f'[info]Sample collections created')
        return True
    else:
        console.print(f'[error]Sample Collections not created')
        return False


def sample_data(client):
    created=create_sample_collection(client)
    if created:
        metal_client=get_metal_client(client)
        load_sample_data(metal_client)

##################################################
############ get_collection
##################################################

def load_sample_data(metal_client):
    Technology=metal_client.get_metal_collection(TECH_CLT)
    TechnologyProperty=metal_client.get_metal_collection(TECH_PROP_CLT)
    TechnologyCategory=metal_client.get_metal_collection(PROP_CATEGORY_CLT)
    Contributor=metal_client.get_metal_collection(CONTRIB_CLT)

    Technology.metal.register_opposite('hasProperty', 'propertyOf')
    Technology.metal.register_opposite('hasContributor', 'contributorOf')
    TechnologyCategory.metal.register_opposite('categoryOf', 'hasCategory')
    
    TechnologyProperty.l([{'name': 'PQ', 
                            'description': 'Product Quantization Reduces index footprint'},
                            {'name': 'Flat Index', 
                            'description': 'Lightweight index that is designed for small datasets'},
                            {'name': 'HNSW', 
                            'description': 'Index that scales well to large datasets'},
                            {'name': 'Dynamic Index', 
                            'description': 'Automatically switch from a flat index to an HNSW index'},
                            {'name': 'Annoy', 
                            'description': 'Approximate Nearest Neighbors Oh Yeah, uses random forest, ideal for large datasets'},
                            {'name': 'IVF', 
                            'description': 'partitions data into clusters and creates an inverted index for efficient nearest neighbor search'},
                            {'name': 'LSH', 
                            'description': 'Locality-Sensitive Hashing, hashes high-dimensional data points into buckets'}], False)

    Contributor.l([{'name': 'dirkkul'},{'name': 'tsmith023'}], False)


    TechnologyCategory.l([{'name': 'performance', '<>categoryOf':['name=HNSW']},
                          {'name': 'efficiency', '<>categoryOf':['name=PQ', 'name=Annoy', 'name=IVF']},
                          {'name': 'adaptability', '<>categoryOf': ['name=Dynamic Index']},
                          {'name': 'accuracy', '<>categoryOf': ['name=Flat Index']}], 
                          False)

    Technology.l([{'name':'weaviate',
                    '<>hasProperty': ['name=HNSW', 'name=Dynamic Index', 'name=PQ', 'name=Flat Index'],
                    'hasContributor': ['name=dirkkul', 'name=tsmith023']}, 
                    {'name': 'pinecone',
                    '<>hasProperty': ['name=HNSW', 'name=PQ', 'name=Annoy']}, 
                    {'name':'milvus',
                    '<>hasProperty': ['name=HNSW', 'name=PQ', 'name=Annoy']},
                    {'name':'faiss',
                    '<>hasProperty': ['name=HNSW', 'name=IVF', 'name=PQ', 'name=LSH']}], False)
    console.print(f'[info]Sample data loaded')

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

#! load simple attribute
to_load=[{'text_field': 'test'}]
uuid=Technology.metal_load(to_load, False)
Technology.metal_query(uuid[0])


#! load mix attributes and references
uuid_target=TechnologyProperty.metal_load({'text_field_clt2': 'test__0001'}, False)[0]
uuid1=Technology.metal_load({'text_field': 'w1','hasProperty': uuid_target}, False)

Technology.metal_query(uuid1[0])
Technology.metal_query(uuid1[0], 'hasProperty:text_field_clt2')


#! load mix attributes and multiple references
uuid_target1=TechnologyProperty.metal_load({'text_field_clt2': 'test__0001'}, False)[0]
uuid_target2=TechnologyProperty.metal_load({'text_field_clt2': 'test__0001'}, False)[0]
uuid1=Technology.metal_load({'text_field': 'w1','hasProperty': [uuid_target1, uuid_target2]}, False)

Technology.metal_query(uuid1[0])
Technology.metal_query(uuid1[0], 'hasProperty:text_field_clt2')

# load pure references

## using array format
uuid_source=str(Technology.metal_load({'text_field': 'test__0001'}, False)[0])
uuid_target=TechnologyProperty.metal_load({'text_field_clt2': 'test__0002'}, False)[0]

to_load=[[uuid_source,'hasProperty',uuid_target]]
Technology.l(to_load, False)

Technology.q(uuid_source, 'hasProperty:text_field_clt2')

## using dict format
uuid_source=str(Technology.metal_load({'text_field': 'test__0001'}, False)[0])
uuid_target=TechnologyProperty.metal_load({'text_field_clt2': 'test__0002'}, False)[0]

to_load=[{'from_uuid': uuid_source, 'from_property':'hasProperty', 'to':uuid_target}]
Technology.l(to_load, False)

Technology.q(uuid_source, 'hasProperty:text_field_clt2')

## load 2 way references
uuid_source=Technology.metal_load({'text_field': 'test__0001'}, False)[0]
uuid_target=TechnologyProperty.metal_load({'text_field_clt2': 'test__0002'}, False)[0]
to_load=[{'from_uuid': uuid_source, 'from_property':'<>hasProperty', 'to':uuid_target}]

Technology.l(to_load, False)

Technology.q(uuid_source, 'hasProperty:text_field')
TechnologyProperty.q(uuid_target, 'propertyOf:text_field')

# load unresolved refs

## for mix ref
uuid_target=TechnologyProperty.metal_load({'text_field_clt2': 'test__123456'}, False)[0]

uuid_source=Technology.metal_load({'text_field': 'w1',
                       'hasProperty': 'text_field_clt2=test__123456'}, False)

Technology.q(uuid_source, 'propertyOf:text_field')

## for mix ref with multiple unresolved refs
uuid_target=TechnologyProperty.metal_load({'text_field_clt2': 'test__123456'}, False)[0]
uuid_target=TechnologyProperty.metal_load({'text_field_clt2': 'test__891'}, False)[0]

uuid_source=Technology.metal_load({'text_field': 'w1',
                       'hasProperty': ['text_field_clt2=test__123456', 'text_field_clt2=test__891']}, False)

Technology.q(uuid_source, 'propertyOf:text_field')

## for pure refeference
uuid_source=Technology.metal_load({'text_field': 'test__0001'}, False)[0]
uuid_target=TechnologyProperty.metal_load({'text_field_clt2': 'test__pure_ref'}, False)[0]

q='text_field_clt2=test__pure_ref'

to_load=[{'from_uuid': uuid_source, 'from_property':'<>hasProperty', 'to': q}]

Technology.l(to_load, False)

Technology.q(uuid_source, 'hasProperty:text_field_clt2')
TechnologyProperty.q(uuid_target, 'propertyOf:text_field')


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




