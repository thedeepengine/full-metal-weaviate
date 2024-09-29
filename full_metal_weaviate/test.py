import os
import unittest
import weaviate.classes as wvc
from weaviate.classes.config import Property, DataType, ReferenceProperty, Configure, Tokenization
# from main import get_metal_client, get_weaviate_client

# from full_metal_weaviate.main import get_metal_client,get_weaviate_client

global weaviate_client, client

client_weaviate=get_weaviate_client('localhost')
opposite_refs = ['NodeTest.childrenOf<>NodeTest.hasChildren','NodeTest.instanceOf<>NodeTest.hasInstance', 
                 'NodeTest.ontologyOf<>NodeTest.hasOntology', 'NodeTest.hasAttr<>NodeTest.attrOf']
client_weaviate=get_metal_client(client_weaviate, opposite_refs)

node_col=client_weaviate.get_metal_collection('NodeTest')
col=node_col
self=node_col
node_col.q()

node_col.metal.get_opp_clt('hasChildren')

self = node_col

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


uuid5 = str(uuid5)

# node_col.q(uuid4[0])


# single prop
t = [[{'fname': 'name test'}],
    [{'fname': 'name test', 'value': 3}],
    [{'fname': 'name test', 'hasChildren': 'fname=test__0001'}],
    [{'hasChildren': 'fname=test__0001'}],
    [{'<>hasChildren': 'fname=test__0001'}],
    [{'fname': 'name test', 'vector': {'content': [0]*384}}],
    [{'from_uuid': uuid1, 'from_property': 'hasChildren', 'to': uuid2}],
    [{'from_uuid': uuid3, 'from_property': '<>hasChildren', 'to': uuid4}],
    [[uuid4, 'hasChildren',  uuid5],[uuid4, '<>hasAttr',  uuid5]],
    [{'fname': 'name test', 'hasChildren': uuid6}],
    [{'fname': 'name test', 'value': 3, '<>hasChildren': uuid1}],
    [{'uuid': uuid6, 'name': 'updated name'}]
    ]

i = [{'from_uuid': uuid1, 'from_property': 'hasChildren', 'to': uuid2}]
res=[]
for i in t:
   print(i)
   r=metal_load(col, i, False)
   print(r, '\n')
   res.append(r)

to_load = [{'uuid': uuid6, 'name': 'updated name'}]


node_col.q(uuid1, 'hasChildren:fname,childrenOf:fname')
node_col.l({'hasChildren': '3b945d87-29eb-488c-8ed3-678e92f1bce4'}, False)

node_col.l({'from_uuid': '2b869d86-723e-482c-b031-9052a6072a59',
            'from_property': 'hasChildren',
            'to': '3b945d87-29eb-488c-8ed3-678e92f1bce4'})

to_load = {'from_uuid': '2b869d86-723e-482c-b031-9052a6072a59',
            'from_property': 'hasChildren',
            'to': uuid4}


to_load = [{'uuid': uuid6, 'name': 'updated name333333'}]
metal_load(node_col, to_load, dry_run=False)
node_col.q('da208f95-c55d-4be1-ad8b-0ea19c0927a3', 'name')

to_load = [{'name': 'name test'}]
a=node_col.l(to_load, False)


node_col.q(uuid3, 'hasChildren:fname')
node_col.q(uuid4, 'childrenOf:fname')

node_col.q(uuid4, 'hasChildren:fname')
node_col.q(uuid4, 'hasAttr:fname')
node_col.q(uuid5, 'attrOf:fname')


node_col.q(r[0], 'hasChildren:fname')
node_col.q(uuid1, 'childrenOf:name')


# multiple props
# mix prop and refs
# mix prop and unresolved refs
# simple ref
# 2 way ref
# prop with vector / named vector
# pure ref one way dict
# pure ref two way dict
# pure ref array
# mix prop and resolved refs
# mix prop and resolved 2 way refs
# already existing ref

node_col.q('name=home')
to_load = [{'name': 'name test', 'value': 3, '<>hasChildren': }]

to_load={'name': 'test0000'}
uuid=node_col.l(to_load)


node_col.q('990058ea-b66e-4e1d-8f67-0f282591794d', 'childrenOf:name')


to_load = [{'name': 'name test', 'value': 3, '<>hasChildren': '990058ea-b66e-4e1d-8f67-0f282591794d'}]

clt_name, refs = list( grouped.items())[0]

def get_test_clt():
    opposite_refs = ['JeopardyQuestion.hasCategory<>JeopardyCategory.hasQuestion',
                    'JeopardyQuestion.hasAssociatedQuestion<>JeopardyQuestion.associatedQuestionOf']

    client_weaviate=get_weaviate_client('localhost')
    client=get_metal_client(client_weaviate, opposite_refs)
    JeopardyQuestion=client.get_metal_collection('JeopardyQuestion')
    JeopardyCategory=client.get_metal_collection('JeopardyCategory')
    return JeopardyQuestion,JeopardyCategory

JeopardyQuestion,JeopardyCategory = get_test_clt()

JeopardyQuestion.metal_query()


client=get_weaviate_client('localhost')

get_translate_filter(JeopardyQuestion)
metal_query(JeopardyQuestion)

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

# single simple properties
return_fields='name'

# multiple simple properties
return_fields='name,date,attr'

# single reference
return_fields='hasChildren:name'

# single reference, multiple properties
return_fields='hasChildren:name,value,content'

# multiple reference logical and
return_fields='hasChildren:name,hasInstance:name'

# single nested reference without parent properties, fetch only name of the children of children
return_fields='hasChildren.hasChildren:name'

# nested reference with parent properties, fetch name of children and name of children of children
return_fields='hasChildren:name>hasChildren:name'

# deep nesting
return_fields='hasChildren:name>hasChildren:name>hasChildren:name'

# deep nested reference, get name of children at 3 level deep and get the name of hasAttrUuid at the last nested children level
return_fields='hasChildren.hasChildren.hasChildren:name>hasAttrUuid:name'

# deep nesting with different refs for each level
return_fields='hasChildren:name>(hasAttrUuid:name,hasChildren.name>(hasAttrUuid:name,hasChildren.name))'

# distribution of nesting operation to operation, logical and
return_fields='hasChildren.name>(hasAttrUuid.hasChildren:name,hasChildren.name)'
# equivalent of 
return_fields='hasChildren.name>hasAttrUuid.hasChildren:name,hasChildren.name>hasChildren.name'



# deep nesting
return_fields='hasChildren:name>hasChildren:name,hasOntology:name>hasChildren:name'
return_fields='hasChildren:name,hasChildren:name>hasOntology:name,hasChildren:name'


t=['name',
   'name,date,attr',
   'name,date,attr,vector',
   'name,date,attr,vector:content',
   'hasChildren:name',
   'name,hasChildren:name',
   'name,date,hasChildren:name',
   'name,date,hasChildren:name,value,content',
   'hasChildren:name,value,content',
    'hasChildren:name,hasInstance:name',
    'hasChildren.hasChildren:name',
    'hasChildren:name,value,hasChildren:name',
    'hasChildren:name>hasChildren:name',
    'hasChildren:name>hasChildren:name>hasChildren:name',
    'hasChildren.hasChildren.hasChildren:name>hasAttrUuid:name',
    'hasChildren>(name,hasAttrUuid:name,hasChildren:name>(hasAttrUuid:name,hasChildren:name))',
    'hasChildren>(name,hasAttrUuid.hasChildren:name,hasChildren:name)',
    'hasChildren>(hasAttrUuid.hasChildren:name,hasChildren:name)',
    'hasChildren.hasChildren>(hasAttrUuid.hasChildren:name,hasChildren:name)',
    'hasChildren>(name,hasAttrUuid.hasChildren:name,value,attr,hasChildren:name)',
    'hasChildren>(name,hasAttrUuid.hasChildren:name,value,attr,hasChildren:name,hasAttr:name)',
    'hasOntology>(name,hasAttrUuid.hasChildren:name,hasOntology:name>hasChildren:name)',
    'hasOntology>(name,hasAttrUuid.hasChildren:name>hasOntology:name>(hasChildren:name,hasAttr:name))',
    'name,date,hasOntology:name,hasChildren>(hasChildren:name,hasAttrUuid:name>hasChildren:name),hasAttrUuid:name',
    'name,hasChildren>(name,date,hasChildren:name,hasAttrUuid:name>hasChildren:name),hasAttrUuid:name']

for i in t:
    print(i)
    print(nested_expr.parseString(i, parseAll=True).asList())


'hasChildren:name>hasChildren:name'


compiler=get_return_field_compiler()

res=[]
for i in t:
    temp = get_weaviate_return_fields(compiler,i)
    res.append(temp)

res[10]


from pyparsing import Regex, Combine, ZeroOrMore, Forward, Group, Suppress, delimitedList, FollowedBy, Optional, NotAny

# Define the basic identifier used for properties and reference names
identifier = Regex("[_A-Za-z][_0-9A-Za-z]{0,230}")

# A field can now include multiple properties separated by commas, stopping if it looks ahead and finds a ':' indicating another field or reference
properties = Group(identifier + ZeroOrMore(',' + identifier))
field = Combine(identifier + Optional(':' + properties + NotAny("," + identifier + ":")))

# Forward declaration for nested expressions to handle recursive patterns
nested_expr = Forward()

# Define how nested structures are formatted, using ">" to indicate deeper nesting
nested_structure = Group(field + ZeroOrMore(Suppress('>') + nested_expr))

# This is the recursive rule that allows nested structures or plain nested expressions
nested_expr <<= nested_structure | delimitedList(nested_expr, delim=',')

# The entire query expression consists of one or more nested expressions separated by commas
query_expr = delimitedList(nested_expr, delim=',')

return_fields='hasChildren.hasChildren:name,value,content>hasChildren:name'
# return_fields='hasChildren:name'
parsed_result=query_expr.parseString(return_fields, parseAll=True)
print(parsed_result.asList())


parser_return_field=get_return_field_compiler()
return_fields_str = "question,name,metadata:distance,hasCategory:title,description>hasChildren:name,description,hasAssociatedQuestion:question,name"
parsed_types = parser_return_field.searchString(return_fields_str)
ret_prop,ret_ref,ret_metadata,include_vector=get_weaviate_return_fields(parsed_types)





###########################################################################
######### test return fields
###########################################################################


return_fields='name,'+'>'.join(['hasChildren:name>hasAttrUuid:name']*4)
return_fields='name,'+'>'.join(['(hasChildren:name>hasAttrUuid:name)']*4)
return_fields='name,hasChildren:name>hasChildren:name>hasChildren:name>hasChildren:name'
return_fields='name,hasChildren:name>hasChildren:name,hasChildren:name>hasAttrUuid:name'
return_fields='name,hasAttrUuid:name,hasChildren:name>(hasAttrUuid:name,hasChildren:name>(hasAttrUuid:name,hasChildren:name))'
return_fields='name,hasChildren:name>hasAttrUuid:name'






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

###########################################################################
######### test weaviate client
###########################################################################

client_weaviate=get_weaviate_client('localhost')
client=metal(client_weaviate, opposite_refs)
JeopardyQuestion=client.get_metal_collection('JeopardyQuestion')



filter_str = [
    'uuid=11111111-1111-1111-1111-111111111111', # uuid with uuid field
    # '11111111-1111-1111-1111-111111111111', # just uuid
    'name=nameValue', # single prop
    'name=nameValue&content=contentValue', # logical and
    'name=nameValue|content=contentValue', # logical or
    'hasChildren.hasOntology.name=childrenValue', # chained refs
    'question=questionValue&answer=answerValue|hasCategory.title=childrenName', # mix logical and/or
    'points=3', # integer,
    'hasCategory.title=childrenName'
] 

i = filter_str[7]
# i = filter_str[5]
# for i in filter_str:
operations=JeopardyQuestion.metal.compiler.parseString(i,parse_all=True)
w_filter=get_composed_weaviate_filter(JeopardyQuestion,operations[0])

col=JeopardyQuestion
chain_split=['hasCategory', 'title']
ref_naming_checker(JeopardyQuestion, ['hasCatkegory', 'tikktle'])



modelVectorizer = SentenceTransformer('/Users/paulhechinger/05data_shokunin/vectorizer/sentence_transformers/sentence-transformers_multi-qa-MiniLM-L6-cos-v1')







JeopardyQuestion.q(uuid[0])



JeopardyQuestion.q('question ~ *', 'question;;hasCategory:title')
JeopardyQuestion.q(return_fields='hasCategory.title')


uuids = ['93173e12-3909-4208-8d92-b641b0f06234']*10000

is_uuid_valid('93173e12-3909-4208-8d92-b641b0f06234')

a=all([is_uuid_valid(i) for i in uuids])
get_valid_uuid(['93173e12-3909-4208-8d92-b641b0f06234', '93173e12-3909-4208-8d92-b641b0f06234'])

JeopardyQuestion.q(include_vector=True)
is_metal_collection(JeopardyQuestion)

JeopardyQuestion.query.fetch_objects(
                return_properties=['question'],
                # return_references=[],
                include_vector=['question', 'answer', 'ggg'],
                limit=100)


get_compiler(['name']).parseString('name=ddd')
allowed_fields = ['name']
expr.parseString('name=3&name=fff')






###################################################
##### test get_translate_filter ###################
###################################################


client_weaviate=get_weaviate_client('localhost')
client=get_metal_client(client_weaviate)
node_col=client.get_metal_collection('NodeTest')

return_fields='name,hasChildren:name'
node_col.q('name=Roommate', 'name,hasChildren:name>hasChildren:name')


return_fields_str = 'name,hasChildren:name'
metal_query(node_col,'name=Roommate', 'name,hasChildren:name')

col = node_col
self = col
node_col.metal.

a=node_col.q('name=Roommate', 'name;;hasChildren:name;hasChildren:name')

# no return_fields set
node_col.metal_query('name=Roommate')


node_col.metal_query('name=Roommate', return_fields='name')


node_col.metal_query('name=10022', return_fields='name,hasChildren:attrName,attrUuid>instanceOf:name,instanceOf:name,instanceOf:name')


all_ref = node_col


[i.name for i in a.references]
[QueryReference(link_on=i.name) for i in a.references]

uuid = '6ded3907-24ac-46de-84ab-c75fbbc7fe32'
uuid = '1d30586a-1e77-4a90-9237-32cfec4e87db'
s=node_col.query.fetch_objects(filters=Filter.by_id().equal(uuid),
                             return_references=[QueryReference(link_on=i.name) for i in a.references])

s.objects[0].references['hasChildren'].objects

 return_references=QueryReference(link_on="has_assembly")


element.query.fetch_objects(filters=Filter.by_id().equal(single_uuid), return_references=QueryReference(link_on="has_assembly"))
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

###################################################
##### test get_translate_filter ###################
###################################################


get_composed_weaviate_filter(col,[],{})

#########################################################
##### test get_atomic_weaviate_filter ###################
#########################################################

get_atomic_weaviate_filter()



#########################################################
##### test get_atomic_weaviate_filter ###################
#########################################################

allowed_fields=['name', 'age', 'gender', 'salary', 'hasChildren', 'ontologyOf']
compiler=get_compiler(fields)
r=compiler.parseString('name=john&age=22',parse_all=True)

list(r)


allowed_fields = ['name', 'age', 'salary', 'department', 'hasChildren']
compiler2=get_compiler(allowed_fields)
r=compiler2.parseString('name=John&age>30|department=Sales',parseAll=True)

filters_str='name = John | age > 30 | department = Sales'
filters_str='name = John & age > 30 | department = Sales & salary >= 50000'
filters_str='((name = John) & (age > 30)) | ((department = Sales) & (salary >= 50000))'
filters_str='hasChildren.hasOntology.name=childrenValue'
compiler=get_compiler(allowed_fields)
y=compiler.parseString(filters_str,parseAll=True)
y[0]



compiler=get_compiler(allowed_fields)
y=compiler.parseString('name=John&age>30&department=Sales|name=Sarah',parseAll=True)
y

y=compiler.parseString('name=John&age>30&department=Sales',parseAll=True)
y


filters_str='question=aaaa&answer=hhhh&points=4'
filters_str='question=aaaa'
filters_str='hasCategory.name=childrenValue'
JeopardyQuestion,JeopardyCategory = get_test_clt()
col=JeopardyQuestion
w_filter=get_translate_filter(JeopardyQuestion,filters_str)


compiler=get_compiler(['aa', 'bb', 'cc'])
compiler.parseString('aa=dddd',parse_all=True)

compiler.parseString('aa=dddd',parse_all=True)


d=get_composed_weaviate_filter(col,operations,context)






client_weaviate=get_weaviate_client('localhost')
client=get_metal_client(client_weaviate)
node_col=client.get_metal_collection('NodeTest')



client_weaviate.    

home_uuid = '34317173-4aeb-4452-b1cb-1924dba65b82'
obj_type = 'Home'

filters_str='name=roommates'
return_fields='name'
self=node_col
context={}
ont_uuid_map = node_col.q(filters_str,return_fields)


col = client_weaviate.collections.get('Node4')
col = client_weaviate.collections.get('TableMetadata2')
col.query.fetch_objects()

node_col.query.fetch_objects()
                filters=w_filter,
                return_properties=ret_prop,
                limit=limit)

attrToQuery='attrName = all_profile_vec'
namespaceRoommate = '&childrenOf.instanceOf.name = Roommate'
filterOnLivingRoommates = '&childrenOf.name any living_users_id'

filters_str = attrToQuery+namespaceRoommate+filterOnLivingRoommates
filters_str='childrenOf.name any living_users_id'
closest_profile=node_col.q(filters_str,
                        context={'living_users_id': ['10309', '20000']},
                        return_fields='name',
                        # return_fields='name,value,metadata:distance,childrenOf:name',
                        # query_vector=profile_vec,
                        target_vector='content',
                        limit = 1000)


get_atomic_weaviate_filter(node_col, 'childrenOf.name', 'any', 'living_users_id', context)
self = node_col

##############
## test get_weaviate_return
########

ret_compiler=get_return_field_compiler()
parsed_types=ret_compiler.searchString('name,vector:container')
parsed_types=ret_compiler.searchString('name,vector')
ret_prop,ret_ref,ret_metadata,include_vector=get_weaviate_return(parsed_types)

get_weaviate_return()


ident.parseString(filter)
condition.parseString(filter)
a=atom.parseString(filter)
expr.parseString(filter)

expr <<= infixNotation(atom, [
            ('&', 2, opAssoc.LEFT, lambda t: {'and': [t[0][0], t[0][2]]}),
            ('|', 2, opAssoc.LEFT, lambda t: {'or': [t[0][0], t[0][2]]})
        ])

expr <<= infixNotation(atom, [
            ('&', 2, opAssoc.LEFT, lambda x: x[0]),
            ('|', 2, opAssoc.LEFT)
        ])

expr.parseString('name=John&(age>30)')
expr.parseString('name=John&age>30&department=Sales')
expr.parseString('name=John&(age>30&department=Sales)')

.parseString(filter)

d=expr.parseString(filter)

expr.parseString(filter)


exp = {'and': [{'and': [{'field': 'name', 'operator': '=', 'value': 'John'},
    {'or': [{'field': 'age', 'operator': '>', 'value': '30'},
      {'field': 'department', 'operator': '=', 'value': 'Sales'}]}]},
  {'field': 'salary', 'operator': '>=', 'value': '50000'}]}

r[0]
r[0]['and'][0]

compiler2=get_expr2(allowed_fields)
r=compiler2.parseString('name=john&age=44',parse_all=True)

r=compiler2.parseString('name=john',parse_all=True)
list(r)

condition.parseString('name=john&age=22')
expr.parseString('name=john&age=22')

r[0]

from pyparsing import Word, nums, oneOf

# Define grammar
operand = Word(nums).setParseAction(lambda tokens: int(tokens[0]))
operator = oneOf("+ -")
expr = operand + operator + operand

def calculate(t):
    if t[1] == '+':
        return t[0] + t[2]
    elif t[1] == '-':
        return t[0] - t[2]
expr.setParseAction(calculate)
result = expr.parseString("5 + 3")
print(result)  # Output: [8]



operand = Word(nums)
operator = oneOf("+ -")
expr = operand + operator + operand

# Parse the expression
result = expr.parseString("5 + 3")
print(result)  # Output: ['5', '+', '3']



#########################################
##### test metal_load ###################
#########################################



JeopardyQuestion,JeopardyCategory=get_test_clt()

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






to_load = [
    {'question': 'question1', 'vector': {'question': [1,2,3]}, '<>hasCategory': '93173e12-3909-4208-8d92-b641b0f06234'},
    {'question': 'question2', 'vector': {'question': [1,2,3]}, '<>hasCategory': '93173e12-3909-4208-8d92-b641b0f06234'}]


######## full update





########################################################
######## test metal_query ##############################
########################################################

node_col=client_weaviate.get_metal_collection('NodeTest')
node_col.metal_query('78345914-5f87-40f9-9f77-30802a547c05')

f=get_metal_collection(client_weaviate, 'NodeTest', True)
'NodeTest' in getattr(client,'buffer_clt',[])
client.buffer_clt

col.metal_query('78345914-5f87-40f9-9f77-30802a547c05')
f.metal_query('78345914-5f87-40f9-9f77-30802a547c05')

col.metal_query('78345914-5f87-40f9-9f77-30802a547c05')

node_col.metal_query()

self = client_weaviate
clt = node_col
filters_str = '78345914-5f87-40f9-9f77-30802a547c05'



# no return_fields

node_col.q('name = test')


import importlib
import full_metal_weaviate
importlib.reload(full_metal_weaviate)
from full_metal_weaviate import get_metal_client






single_level_dict = {
    "id": 1,
    "name": "John Doe",
    "email": "johndoe@example.com",
    "status": "active"
}

list_of_dicts = [
    {"id": 1, "name": "John Doe", "email": "johndoe@example.com", "status": "active"},
    {"id": 2, "name": "Jane Smith", "email": "janesmith@example.com", "status": "inactive"},
    {"id": 3, "name": "Jim Brown", "email": "jimbrown@example.com", "status": "active"}
]



class TestRemoveFunction(unittest.TestCase):
    def test_remove_single_field_from_dict(self):
        obj = _({'name': 'John', 'age': 30})
        self.assertEqual(obj.remove('name'), {'age': 30})

    def test_remove_multiple_fields_from_dict(self):
        obj = _({'name': 'John', 'age': 30, 'gender': 'male'})
        self.assertEqual(obj.remove(['name', 'gender']), {'age': 30})

    def test_remove_single_field_from_list_of_dicts(self):
        obj = _([{'name': 'John', 'age': 30}, {'name': 'Jane', 'age': 25}])
        expected = [{'age': 30}, {'age': 25}]
        self.assertEqual(obj.remove('name'), expected)

    def test_remove_multiple_fields_from_list_of_dicts(self):
        obj = _([{'name': 'John', 'age': 30, 'gender': 'male'}, {'name': 'Jane', 'age': 25, 'gender': 'female'}])
        expected = [{'age': 30}, {'age': 25}]
        self.assertEqual(obj.remove(['name', 'gender']), expected)

# if __name__ == '__main__':
#     unittest.main()
# if __name__ == '__main__':
#     unittest.main(argv=[''], exit=False)



class TestFlatMethod(unittest.TestCase):
    def test_single_level_dict(self):
        instance = _({'data': single_level_dict})
        result = instance.nested_to_flat(keys_to_capture='*')
        self.assertEqual(result, single_level_dict)

    def test_list_of_dicts(self):
        instance = _({'data': list_of_dicts})
        result = instance.nested_to_flat(keys_to_capture='*')
        expected = [
            {"id": 1, "name": "John Doe", "email": "johndoe@example.com", "status": "active"},
            {"id": 2, "name": "Jane Smith", "email": "janesmith@example.com", "status": "inactive"},
            {"id": 3, "name": "Jim Brown", "email": "jimbrown@example.com", "status": "active"}
        ]
        self.assertEqual(result, expected)

# if __name__ == '__main__':
#     unittest.main(argv=[''], exit=False)



##########################################
#### test translate_return_fields
##########################################

return_fields=[
    'question,answer', # simple direct fields
    'question,answer;;hasCategory:title', # with second level
    'question,answer,vector:question,metadata:distance;;hasCategory:title',
    'vector:question,answer',
    'question,answer,vector;;hasCategory:title',
    'question,answer,vector:question;;hasCategory:title',
    'question,metadata:distance;;hasCategory:title,description|hasAssociatedQuestion:question', # same level
    'question,metadata:distance;;hasAssociatedQuestion:question;hasCategory:title' # nested
]


def split_into_tokens(input_string):
    tokens = re.split('([^,;|:]+)', input_string)
    tokens = [token.strip() for token in tokens if token.strip()]
    return tokens

input_string = "question,name,metadata:distance,hasCategory:title,description;hasChildren:name|hasAssociatedQuestion:question"
tokens = split_into_tokens(input_string)
print(tokens)


import re

def split_into_tokens(input_string):
    tokens = re.split('([^,;|:]+)', input_string)
    tokens = [token.strip() for token in tokens if token.strip()]
    return tokens

input_string = "question,name,metadata:distance,hasCategory:title,description;hasChildren:name|hasAssociatedQuestion:question"

def parse_tokens(tokens):
    elements = {}
    current_label = None
    subfields = []

    for i, token in enumerate(tokens):
        next_token = tokens[i + 1] if i + 1 < len(tokens) else None
        
        if next_token:
            if next_token == ':':
                current_label = token
            elif next_token in [',', ';', '|']:
                if current_label:
                    subfields.append(token)
                else:
                    elements[token] = None
            else:
                if current_label:
                    subfields.append(token)
                else:
                    elements[token] = None
        else:
            # Handling the last token
            if current_label:
                # If it's part of subfields collection
                subfields.append(token)
                elements[current_label] = subfields if len(subfields) > 1 else subfields[0]
            else:
                # It's a standalone field, add to elements
                elements[token] = None

    return elements

# Example string
input_string = "question,name,metadata:distance,hasCategory:title,description;hasChildren:name|hasAssociatedQuestion:question"
tokens = split_into_tokens(input_string)
parsed_data = parse_tokens(tokens)
print(parsed_data)









def parse_tokens(tokens):
    elements = {}
    current_label = None
    subfields = []

    for i, token in enumerate(tokens):
        next_token = tokens[i + 1] if i + 1 < len(tokens) else None

        if next_token == ':':
            current_label = token
        elif current_label:
            subfields.append(token)
            if next_token in [';', '|']:
                elements[current_label] = subfields if len(subfields) > 1 else subfields[0]
                current_label = None
                subfields = []
        elif next_token in [';', '|']:
            current_label = None
        elif next_token == ',':
            elements[token] = None

    return elements

# Example string and parsing
input_string = "question,name,metadata:distance,hasCategory:title,description>hasChildren:name|hasAssociatedQuestion:question"
tokens = split_into_tokens(input_string)
parsed_data = parse_tokens(tokens)
print(parsed_data)



"question,name,metadata:distance,hasCategory:title,description>hasChildren:name,description,hasAssociatedQuestion:question,name"
"question,name,metadata:distance,hasCategory:title,description>(hasChildren:name,description,hasAssociatedQuestion:question,name)"
"question,name,metadata:distance,hasCategory:title,description>(hasChildren:name,description,hasAssociatedQuestion:question,name)"
s = "question,name,metadata:distance,hasCategory:title,description>hasChildren:name,hasAssociatedQuestion:question,name"
s.split(',')



import re

def extract_fields(input_string):
    # Regex pattern to match fields and their possible values
    pattern = r'(\w+)(?::\s*([^:>]+))?'
    
    # Find all matches in the input string
    matches = re.findall(pattern, input_string)
    
    # Creating a dictionary to store the results
    result = {}
    for field, values in matches:
        if values:
            # Split values on comma and strip spaces
            result[field] = [v.strip() for v in values.split(',')]
        else:
            # No values, just the field
            result[field] = None

    return result

# Example usage
input_string = "question,name,metadata:distance,hasCategory:title,description>hasChildren:name,description,hasAssociatedQuestion:question,name"
print(extract_fields(input_string))





import re

def parse_field_values(text):
    # Regex to match field names and their associated values
    pattern = r"(?P<field>\w+)(?::(?P<values>(?:[^:]+?(?=\w+:|$))))?"
    matches = re.finditer(pattern, text)
    result = {}
    for match in matches:
        field = match.group('field')
        values = match.group('values')
        if values:
            values = [value.strip() for value in values.split(',')]
        result[field] = values
    return result

# Example usage
text = "question,name,metadata:distance,hasCategory:title,description>hasChildren:name,description,hasAssociatedQuestion:question,name"
result = parse_field_values(text)
print(result)


import re

def split_on_word_colon(text):
    # Regex pattern to identify a word followed by a colon as the separator
    pattern = r'\b\w+:\b'
    # Split the text using the defined pattern
    parts = re.split(pattern, text)
    # Return the split parts, filtering out any empty strings
    return [part.strip() for part in parts if part.strip()]

# Example usage
text = "field1:value1 field2:value2 field3:value3"
result = split_on_word_colon(text)
print(result)












from pyparsing import Word, alphas, alphanums, Optional, delimitedList, Group, Suppress, oneOf

fieldName = Word(alphas)
value = Word(alphanums)
fieldWithValue = Group(fieldName + Suppress(":") + delimitedList(value))

nestedField = Group(fieldWithValue + Optional(Suppress(">") + fieldWithValue))

expression = delimitedList(fieldName | nestedField)

# Sample input
sample = "question,name,metadata:distance,hasCategory:title,description>hasChildren:name,description,hasAssociatedQuestion:question,name"

sample='metadata:distance'
# Parse the input
parsed_result = expression.parseString(sample)

# Print parsed results
print(parsed_result.asList())


from pyparsing import Word, alphas, OneOrMore, Combine
field = Word(alphas)

value = delimitedList(field)

field_value = Combine(field + Suppress(":") + value)

parser = OneOrMore(field | field_value)


sample_string = "question,name,metadata:distance,hasCategory:title,description>hasChildren:name,description,hasAssociatedQuestion:question,name"
list(parser.searchString(sample_string))

for word in found_words:
    print(word[0])


### 555555
from pyparsing import Word, alphas, Combine, Suppress, delimitedList, Optional, OneOrMore, FollowedBy, NotAny

# Define a parser for a simple word made up of alphabet characters
field = Word(alphas)

# Define a parser for values, which can be one or more comma-separated words,
# but ensure that a value does not precede a word followed by a colon
value = delimitedList(Combine(field + ~FollowedBy(Suppress(":"))))

# Define a parser for "field:value" format
field_value = Combine(field + Suppress(":") + value)

# Define the main parser that prefers "field:value" over just "field"
parser = OneOrMore(field_value | field)

# Sample string
sample_string = "question,name,metadata:distance,hasCategory:title,description>hasChildren:name,description,hasAssociatedQuestion:question,name"

# Extract fields and field:value pairs
results = parser.searchString(sample_string)

# Print results
for result in results:
    print(result[0])

### 6666
from pyparsing import Word, alphas, Combine, Suppress, delimitedList, Optional, OneOrMore, FollowedBy, NotAny

field = Word(alphas)
value = delimitedList(Combine(field + ~FollowedBy(Suppress(":"))), combine= True)
field_value = Combine(field + ":" + value)
complex_field_value = Group(field_value + ">" + OneOrMore(field_value))
complex_field_value.setParseAction(lambda tokens: ("complex_field_value", "".join(str(t) for t in tokens)))
parser = OneOrMore(complex_field_value | field_value | field.setParseAction(lambda tokens: ("field", tokens[0])))
sample_string = "question,name,metadata:distance,hasCategory:title,description>hasChildren:name,description,hasAssociatedQuestion:question,name"

field_value.setParseAction(lambda tokens: ("field_value", " ".join(tokens)))
results = parser.searchString(sample_string)

for result in results:
    print(result)


### 7777
from pyparsing import Word, alphas, Combine, Suppress, delimitedList, Optional, OneOrMore, FollowedBy, NotAny






field.parseString('jjjj')

parse_atomic_return_ref(field_value[0])

value=['hasCategory:title,description', 'hasChildren:name,description']

field_value=parsed_fields[2][0][1]
value=parsed_fields[3][0][1]


for result in results:
    print(result)


node_col.q('name=Roommate', 'name,hasChildren:name')



field = Word(alphas).setParseAction(lambda tokens: ("field", tokens[0]))
value = delimitedList(Combine(field + ~FollowedBy(Suppress(":"))), combine= True)
field_value = Combine(field + ":" + value)
complex_field_value = Group(field_value + ">" + OneOrMore(field_value))
complex_field_value.setParseAction(lambda tokens: ("complex_field_value", "".join(str(t) for t in tokens)))
parser = OneOrMore(complex_field_value | field_value | field)
sample_string = "question,name,metadata:distance,hasCategory:title,description>hasChildren:name,description,hasAssociatedQuestion:question,name"

field_value.setParseAction(lambda tokens: ("field_value", " ".join(tokens)))
results = parser.searchString(sample_string)

for result in results:
    print(result)


results[2][0]
results[3]

res=[]
for i in return_fields:
    try:
        res.append({i: translate_return_fields(i)})
        pass
    except Exception:
        pass

res[4]

return_fields = list(res[2].keys())[0]


allowed_fields=['a', 'b']
regex_pattern = r'^(?:' + '|'.join(map(re.escape, allowed_fields)) + ')$'
bool(re.match(regex_pattern, 'a'))

compiler=get_compiler(['a', 'b'])

def gg():
    fff=compiler.parseString('af=44')
    print(fff)
    return fff


self = TestGetExpr()

self.compiler

list(result)

try:
    gg()
except StopProcessingException as e:
    console.print(str(e))
except Exception as e:
    # Handle other exceptions that might occur
    console.print('An unexpected error occurred.')


try:
    ff=compiler.parseString('af=44')
except StopProcessingException as e:
    console.print(e)
except Exception as e:
    # Handle other exceptions that might occur
    console.print('An unexpected error occurred.')




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

l=['aa', 'bb', 'vcc']

__(l).startswith('v').get(lambda x: x == True)
__(l).startswith('v').truth()



[i[1][2:] for i in to_load if i[1].startswith('<>')]







colon = Suppress(":")
comma = Suppress(",")
gt = Suppress(">")
# colon = ":"
# comma = ","
# gt = ">"
simple = Word(alphas)
compound = Group(simple + (colon | gt) + delimitedList(simple))

expr = OneOrMore(Group(compound | simple))

input_string = "aaaa,bbbb,gggg,jjjj:aaaa,kkkk,llll>gggg,pppp:qqqq"
parsed = expr.parseString(input_string, parseAll=True)
parsed

# une bequille comme enregistrer des examples minimaux, le pattern des example
# plus complexes etant des composition des examples elementaire

compound.parseString('aaaa>ggg')

compound = Group(Word(alphas) + (Literal(';') | Literal('>')) + delimitedList(Word(alphas)))
OneOrMore(compound).parseString('aaaa>ggg,ll jjj>llll,ppp,ll')