import os
import unittest
import weaviate.classes as wvc
from weaviate.classes.config import Property, DataType, ReferenceProperty, Configure, Tokenization
from main import get_metal_client

client_weaviate=get_metal_client('localhost')


opposite_refs = ['NodeTest.childrenOf<>NodeTest.hasChildren','NodeTest.instanceOf<>NodeTest.hasInstance', 
                 'NodeTest.ontologyOf<>NodeTest.hasOntology', 'NodeTest.hasAttr<>NodeTest.attrOf']
client_weaviate=get_metal_client(client_weaviate, opposite_refs)
node_col=client_weaviate.get_metal_collection('NodeTest')
col=node_col
self=node_col
node_col.q()
compiler = node_col.metal.compiler
compiler_r = node_col.metal.compiler_return_f

node_col.metal.get_opp_clt('hasChildren')

self = node_col


raw=col.metal_query('name=Roommate', 'name,hasChildren:name')




raw=node_col.metal_query('name=Roommate', 'i want a room downtown with a swimming pool')


#########################################
##### test metal_load ###################
#########################################

children1_uuid = generate_uuid5('children1')
children2_uuid = generate_uuid5('children2')

uuid1=str(node_col.l({'fname': 'test__0001'}, False)[0])
uuid2=str(node_col.l({'fname': 'test__0002'}, False)[0])
uuid3=str(node_col.l({'fname': 'test__0003'}, False)[0])
uuid4=str(node_col.l({'fname': 'test__0004'}, False)[0])
uuid5=str(node_col.l({'fname': 'test__0005'}, False)[0])
uuid6=str(node_col.l({'fname': 'test__0006'}, False)[0])
uuid7=str(node_col.l({'fname': 'test__0007'}, False)[0])


# single prop
t = [[{'fname': 'name test'}],
    [{'fname': 'name test', 'value': 3}],
    [{'fname': 'name test', 'hasChildren': 'fname=test__0001'}],
    [{'hasChildren': 'fname=test__0001'}],
    [{'hasChildren': [uuid5,uuid6,uuid7]}],
    [{'<>hasChildren': 'fname=test__0001'}],
    [{'fname': 'name test', 'vector': {'content': [0]*384}}],
    [{'from_uuid': uuid1, 'from_property': 'hasChildren', 'to': uuid2}],
    [{'from_uuid': uuid3, 'from_property': '<>hasChildren', 'to': uuid4}],
    [[uuid4, 'hasChildren',  uuid5],[uuid4, '<>hasAttr',  uuid5]],
    [{'fname': 'name test', 'hasChildren': uuid6}],
    [{'fname': 'name test', 'value': 3, '<>hasChildren': uuid1}],
    [{'uuid': uuid6, 'name': 'updated name'}],
    [{'uuid':uuid3,'hasChildren': [uuid5,uuid6,uuid7]}], # update multiples refs
    ]

i = [{'from_uuid': uuid1, 'from_property': 'hasChildren', 'to': uuid2}]
res=[]
for i in t:
   print(i)
   r=metal_load(col, i, False)
   print(r, '\n')
   res.append(r)

def get_test_clt():
    opposite_refs = ['JeopardyQuestion.hasCategory<>JeopardyCategory.hasQuestion',
                    'JeopardyQuestion.hasAssociatedQuestion<>JeopardyQuestion.associatedQuestionOf']

    client_weaviate=get_weaviate_client('localhost')
    client=get_metal_client(client_weaviate, opposite_refs)
    JeopardyQuestion=client.get_metal_collection('JeopardyQuestion')
    JeopardyCategory=client.get_metal_collection('JeopardyCategory')
    return JeopardyQuestion,JeopardyCategory

JeopardyQuestion,JeopardyCategory = get_test_clt()



client=get_weaviate_client('localhost')

#######################
### Jeopardy
#######################

client.collections.delete('JeopardyQuestion')
client.collections.delete('JeopardyCategory')

JeopardyQuestion = client.collections.create(
    name="JeopardyQuestion",
    description="A Jeopardy! question",
    properties=[
        Property(name="question", data_type=DataType.TEXT),
        Property(name="answer", data_type=DataType.TEXT),
        Property(name="points", data_type=DataType.INT)
    ],
    vectorizer_config=[Configure.NamedVectors.none(name="question"),
                    Configure.NamedVectors.none(name="answer")]
)

# JeopardyQuestion.config.add_property(Property(name="points",data_type=DataType.INT))

JeopardyCategory = client.collections.create(
    name="JeopardyCategory",
    description="A Jeopardy! category",
    properties=[
        Property(name="title", data_type=DataType.TEXT),
        Property(name="description", data_type=DataType.TEXT)
    ]
)

JeopardyQuestion.config.add_reference(ReferenceProperty(name="hasCategory",target_collection="JeopardyCategory"))
JeopardyQuestion.config.add_reference(ReferenceProperty(name="hasAssociatedQuestion",target_collection="JeopardyQuestion"))
JeopardyQuestion.config.add_reference(ReferenceProperty(name="associatedQuestionOf",target_collection="JeopardyQuestion"))
JeopardyCategory.config.add_reference(ReferenceProperty(name="hasQuestion",target_collection="JeopardyQuestion"))


#######################
### Class Code
#######################

class_test_clt = client_weaviate.collections.create(
    name='ClassTest',
    properties=[
        Property(name="name", data_type=DataType.TEXT),
        Property(name="body", data_type=DataType.TEXT)
    ],
    vectorizer_config=[Configure.NamedVectors.none(name="name")])

method_test_clt = client_weaviate.collections.create(
    name='MethodTest',
    properties=[
        Property(name="name", data_type=DataType.TEXT),
        Property(name="signature", data_type=DataType.TEXT)
    ])

attribute_test_clt = client_weaviate.collections.create(
    name='AttributeTest',
    properties=[
        Property(name="name", data_type=DataType.TEXT),
        Property(name="value", data_type=DataType.TEXT)
    ])

class_test_clt.config.add_reference(ReferenceProperty(name="hasMethod",target_collection='MethodTest'))
class_test_clt.config.add_reference(ReferenceProperty(name="hasAttribute",target_collection='AttributeTest'))
method_test_clt.config.add_reference(ReferenceProperty(name="hasClass",target_collection='ClassTest'))
attribute_test_clt.config.add_reference(ReferenceProperty(name="hasClass",target_collection='ClassTest'))


###########################################################################
######### test weaviate client
###########################################################################

### Employee

opposite_refs = ['Employee.hasDepartment<>Department.hasEmployee',
                 'ClassTest.hasAttribute<>AttributeTest.hasClass']

client_weaviate=get_weaviate_client('localhost')
client=metal(client_weaviate)


### jeopardy collection


def get_test_clt():
    opposite_refs = ['JeopardyQuestion.hasCategory<>JeopardyCategory.hasQuestion',
                    'JeopardyQuestion.hasAssociatedQuestion<>JeopardyQuestion.associatedQuestionOf']

    client_weaviate=get_weaviate_client('localhost')
    client=get_metal_client(client_weaviate, opposite_refs)
    JeopardyQuestion=client.get_metal_collection('JeopardyQuestion')
    JeopardyCategory=client.get_metal_collection('JeopardyCategory')
    return JeopardyQuestion,JeopardyCategory

JeopardyQuestion,JeopardyCategory = get_test_clt()

###########################################################################
######### test get_metal_collection
###########################################################################


client_weaviate=get_weaviate_client('localhost')
client=get_metal_client(client_weaviate, opposite_refs)
JeopardyQuestion=client.get_metal_collection('JeopardyQuestion')

JeopardyQuestion.metal.context
JeopardyQuestion.metal.props
JeopardyQuestion.metal.refs
JeopardyQuestion.metal.compiler



###########################################################################
######### test get_weaviate_return_fields
###########################################################################


res=[]
for i in t:
    temp={}
    temp['return_str']=i
    temp['compiled']=compiler_r.parseString(i, parseAll=True).asList()
    temp['weaviate']=get_weaviate_return_fields(compiler_r,i)
    res.append(temp)

res2=[]
for i in t:
    temp={}
    temp['return_str']=i
    temp['compiled']=compiler_r.parseString(i, parseAll=True).asList()
    temp['weaviate']=get_weaviate_return_fields(compiler_r,i)
    res2.append(temp)


for i,i1,i2 in zip(range(len(res)),res,res2):
    # print(i1['return_str'] == i2['return_str'])
    print(i, i1['weaviate'] == i2['weaviate'])

res.append(get_weaviate_return_fields(compiler_r,i))





###########################################################################
######### test register_opposite
###########################################################################


opposite_refs = ['JeopardyQuestion.hasCategory<>JeopardyCategory.hasQuestion',
                 'JeopardyQuestion.hasAssociatedQuestion<>JeopardyQuestion.associatedQuestionOf']

client_weaviate=get_weaviate_client('localhost')
client=metal(client_weaviate)

register_opposite(client,opposite_refs)
col = client.get_metal_collection('JeopardyQuestion')
col.metal.get_opposite()




# capturing the fundamental incommensurables axis of complexities.
# disambiguation is a set of heuristics. it goes through

# related to the complexity map, some system shares commonalitiles.
# it allows to understand the main axis your system evolves in.
# differente dimensions of information
# create categories, put together objects with similar properties, so you can act on it.
# thats pretty much what statistical models are doing, neural networks
# information reduction like in 
# moving up and down the abstraction ladder
# create relationships, as a network
# mece, get spaces that are mece
# some models would fail in some odd ways.
# structural fail or data fail

# first thing, some concepts have common properties.
# even more, it shows an idea of fundamentality.
# can we have those funadmentalitles as entities and look at their properties,
# classify, categories them, etc...

# we'll still need observatlibity, check what's happening.



#########################################################
##### test get_atomic_weaviate_filter ###################
#########################################################


### just obj ##############
to_load = [
    {'question': 'question1', 'vector': {'question': [1,2,3]}},
    {'question': 'question2', 'vector': {'question': [1,2,3]}},
    {'question': 'question3', 'vector': {'question': [1,2,3]}}]

uuid_question=JeopardyQuestion.l(to_load, False)

category_to_load = [{'title': 'football', 'description': 'football questions'},
            {'title': 'politics', 'description': 'politics questions'}]
uuid_category = JeopardyCategory.l(category_to_load, False)

self= JeopardyCategory
filters_str ='uuid=9b24c6f0-5687-4fd7-966a-cd952cfee07c'
JeopardyCategory.q('uuid=9b24c6f0-5687-4fd7-966a-cd952cfee07c')

### mix obj without opposite refs ##############
to_load = [
    {'question': 'question1', 'vector': {'question': [1,2,3]}, 'hasCategory': str(uuid_category[0])},
    {'question': 'question2', 'vector': {'question': [1,2,3]}, 'hasCategory': str(uuid_category[1])}]
uuid_question=JeopardyQuestion.l(to_load, False)

### mix obj with opposite refs ##############
to_load = [
    {'question': 'question1', 'vector': {'question': [1,2,3]}, '<>hasCategory': str(uuid_category[0])},
    {'question': 'question2', 'vector': {'question': [1,2,3]}, '<>hasCategory': str(uuid_category[1])}]
uuid_question=JeopardyQuestion.l(to_load, True)


to_load = [
    {'from_uuid': uuid_question[0],'from_property': '<>hasCategory','to_uuid': str(uuid_category[0])},
    {'from_uuid': uuid_question[1],'from_property': '<>hasCategory','to_uuid': str(uuid_category[1])},
    {'from_uuid': uuid_question[2],'from_property': '<>hasCategory','to_uuid': str(uuid_category[1])}]

ref_to_load = [['93173e12-3909-4208-8d92-b641b0f06234','<>hasCategory','a9c1ac6b-369c-4f88-a0f5-0049f1e7993a'],
           ['ba58b3f0-4ab5-4ed5-afff-495c743b4c33','<>hasCategory','97e75959-95a0-44c4-bc88-eba27b1c61c6']]

JeopardyQuestion.l(ref_to_load,False)



##########################################
#### test on raise 
##########################################

def f():
    try:
        a = {'fff': 2}
        a['l']
        return a
    except Exception as e:
        console.print_exception(extra_lines=100,width=100,word_wrap=True)

f()

import sys

try:
    1/0
except Exception:
    exc_type, exc_value, exc_traceback = sys.exc_info()


import linecache

while exc_traceback:
    filename = exc_traceback.tb_frame.f_code.co_filename
    linecache.getlines(filename)  # Load/caches the file if not already loaded
    exc_traceback = exc_traceback.tb_next




def f():
    try:
        a = {'fff': 2}
        a['l']
        return a
    except Exception as e:
        print(e)

f()

def f():
    try:
        a = {'fff': 2}
        a['l']
        return a
    except Exception as e:
        raise(e)
    finally:
        raise Exception('ssss')
        # print('ssss')

f()


def f():
    a = {'fff': 2}
    a['l']
    return a

f()

def f():
    try:
        a = {'fff': 2}
        a['l']
        return a
    except Exception:
        traceback.print_exc(file=sys.stderr)
