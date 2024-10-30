import os
import unittest
import weaviate.classes as wvc
from weaviate.classes.config import Property, DataType, ReferenceProperty, Configure, Tokenization
from main import get_metal_client
from weaviate import WeaviateClient
from weaviate.connect import ConnectionParams
from weaviate.auth import AuthApiKey

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



#### get collection 

clt1=client_metal.get_metal_collection('clt1')
clt2=client_metal.get_metal_collection('clt2')

############ parse_filter

from dataclasses import dataclass

@dataclass
class parse_filter_test:
    desc: str
    filters_str: str
    recorded: dict = None

parse_filter = [
parse_filter_test('filter on simple attribute on equal operator for string type',
                  'string_field=hhh'),
                  'filter on simple attribute on equal operator for string type']

test_that = [{'filters_str':'string_field=hhh',
              'desc': 'filter on simple attribute on equal operator for string type'}]

test_that = [{'to_test':'string_field~*hhh*',
              'desc': 'filter on simple attribute on like operator for string type'}]

test_that = [{'to_test':'integer_field=1',
              'desc': 'filter on simple attribute on equal operator for int type'}]

test_that = [{'to_test':'float_field=1.0',
              'desc': 'filter on simple attribute on equal operator for float type'}]

test_that = [{'to_test':'date_field=2024/01/01',
              'desc': 'filter on simple attribute on equal operator for date type'}]

test_that = [{'to_test':'date_field=2024/01/01',
              'desc': 'filter on simple attribute on equal operator for date type'}]

test_that = [{'to_test':'uuid=11111111-1111-1111-1111-111111111111',
              'desc': 'filter on uuid with field name'}]

test_that = [{'to_test':'11111111-1111-1111-1111-111111111111',
              'desc': 'filter on uuid without field name'}]

test_that = [{'to_test':'hasCategory.title=value',
              'desc': 'filter on ref'}]

test_that = [{'to_test':'name=nameValue&content=contentValue',
              'desc': 'logical and filter'}]

test_that = [{'to_test':'name=nameValue|content=contentValue',
              'desc': 'logical or filter'}]

test_that = [{'to_test':'name=nameValue|content=contentValue&description~me',
              'desc': 'logical and and or filter'}]

test_that = [{'to_test':'hasChildren.hasOntology.hasProperty.name=property_value',
              'desc': 'deeply nested'}]

test_that = [{'to_test':'question=question_value&answer=answer_value|hasCategory.title=category_value',
              'desc': 'complex query mix of simple attribute, logical, ref'}]

test_that = [{'to_test':'((name = John) & (age > 30)) | ((department = Sales) & (salary >= 50000))',
              'desc': 'complex logical filter with priority parenthesis'}]

test_that = [{'to_test':'name = John & age > 30 | department = Sales & salary >= 50000',
              'desc': 'complex logical filter with priority parenthesis'}]


compiler=get_filter_compiler(['name'])
parse_filter(compiler, 'namde=jjj')

try:
    parse_filter(compiler, 'namde')
except ParseBaseException as e:
    print('aaa')
    print(e)

for i in 
parse_filter(compiler, )





############ return fields

{'to_test': 'name',
    'desc': 'single attribute'},

t=[
    'name',
   'name,date,attr', # multiple attribute
   'name,date,attr,vector', # attribute and default vector
   'name,date,attr,vector:content', # attribute and named vector
    'name,date,attr,vector:content,name,hasChildren:name', # simple attr with named vector and ref
    'name,date,attr,vector:content,name,hasChildren:name,vector',
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
    'hasChildren6:name>(hasAttrUuid:name,hasChildren:name>(hasAttrUuid:name,hasChildren:name))',
    'hasChildren:name>(hasAttrUuid.hasChildren:name,hasChildren:name)',
    'hasChildren>(hasAttrUuid.hasChildren:name,hasChildren:name)',
    'hasChildren:name>(hasAttrUuid.hasChildren:name,value,attr,hasChildren:name)',
    'hasChildren:name>(hasAttrUuid.hasChildren:name,value,attr,hasChildren:name,hasAttr:name)',
    'hasOntology:name>(hasAttrUuid.hasChildren:name,hasOntology:name>hasChildren:name)',
    'hasOntology:name>(hasAttrUuid.hasChildren:name>hasOntology:name>(hasChildren:name,hasAttr:name))',
    'name,date,hasOntology:name,hasChildren>(name,date,hasChildren:name,hasAttrUuid:name>hasChildren:name),hasAttrUuid:name',
    'name,date,hasOntology:name,hasChildren>(hasChildren:name,hasAttrUuid:name>hasChildren:name),hasAttrUuid:name',
    'name,hasChildren>(name,date,hasChildren:name,hasAttrUuid:name>hasChildren:name),hasAttrUuid:name',
    'fname,hasChildren:attrName,name,fname,value,content,date>(hasAttr:name>(hasAttrUuid:name),hasChildren:name,value)'
    ]

compiler=get_return_field_compiler()

res_save=[]
for i in t:
    print(i)
    temp = get_weaviate_return_fields(compiler,i)
    res_save.append(temp)

c=get_return_field_compiler()
res_save=[]
for i in t:
    print(i)
    temp=c.parseString(i, parseAll=True).asList()
    res_save.append(temp)





def parse_return_field(compiler, return_field):
    try:
        return compiler.parseString(return_field, parseAll=True)
    # .asList()
    except InvalidFunctionException as e:
        pass
        # print(e)
        # print(e)
        # print(f"Parse error in {e.parserElement}: {e}")


allowed_refs=['hasRef1']
c=get_return_field_compiler()
return_field='hasRef:name'
return_field='abcf:ddd'
return_field='hhh,hasRef1:name,hasRef2>name,vector'
parse_return_field(c,return_field)



'hasChildren:name,value,hasChildren:name',
'hasChildren:name>hasChildren:name',
'hasChildren:name>hasChildren:name>hasChildren:name',
'hasChildren.hasChildren.hasChildren:name>hasAttrUuid:name',
'hasChildren6:name>(hasAttrUuid:name,hasChildren:name>(hasAttrUuid:name,hasChildren:name))',
'hasChildren:name>(hasAttrUuid.hasChildren:name,hasChildren:name)',
'hasChildren>(hasAttrUuid.hasChildren:name,hasChildren:name)',




ref_field.parseString('hasdRef1', parseAll=True).asList()


from pyparsing import alphanums, ParseBaseException


class InvalidArgumentException(ParseFatalException):
    def __init__(self, s, loc, msg):
        super(InvalidArgumentException, self).__init__(
                s, loc, "invalid argument '%s'" % msg)

class InvalidFunctionException(ParseFatalException): 
    def __init__(self, s, loc, msg):
        super(InvalidFunctionException, self).__init__(
                s, loc, "invalid function '%s'" % msg)


class InvalidFunctionException(ParseFatalException): 
    def __init__(self, s, loc, msg):
        super(InvalidFunctionException, self).__init__(
                s, loc)
        console.print(f'❗Ref field not found: [bold yellow]{s}[/bold yellow]')


def error(exceptionClass):
    def raise_exception(s,l,t):
        print('s', s)
        raise exceptionClass(s,l,t[0])
    return Word(alphas,alphanums).setParseAction(raise_exception)

LPAR,RPAR = map(Suppress, "()")
valid_arguments = ['abc', 'bcd', 'efg']
valid_functions = ['avg', 'min', 'max']
argument = oneOf(valid_arguments) | error(InvalidArgumentException)
function_name = oneOf(valid_functions)  | error(InvalidFunctionException)
function_call = Group(function_name('fname') + LPAR + argument('arg') + RPAR)


tests = """\
    avg(abc)
    sum(abc)
    avg(xyz)
    """.splitlines()
for test in tests:
    if not test.strip(): continue
    try:
        print(test.strip())
        result = function_call.parseString(test, parseAll=True)
    except ParseBaseException as pe:
        print(pe)
    else:
        print(result[0].dump())
    print
    
# loading attributes

clt1.

# loading mix attributes and references

# loading pure references


## loading 2 way references


# get metal client

## from client weaviate
import weaviate


weaviate_client.close()
weaviate_client=get_weaviate_client()
metal_client=get_metal_client(weaviate_client)
JeopardyQuestion = metal_client.get_metal_collection('JeopardyQuestion')

# get metal collection


self=JeopardyQuestion
to_load={'question': 'why?'}
JeopardyQuestion.metal_load({'question': 'why?'}, False)


JeopardyQuestion.metal_query()

JeopardyQuestion.metal.register_opposite('hasCategory', 'hasQuestion')
JeopardyQuestion.metal.register_opposite('hasAssociatedQuestion', 'associatedQuestionOf')



JeopardyQuestion.metal.get_opposite('hasCategory')

JeopardyQuestion.metal.context['ref_target']

self=JeopardyQuestion
ref_source='hasCategory'
ref_target='hasQuestion'



## Register two-way relationships


################




JeopardyQuestion.q()

JeopardyQuestion.l({'question': 'test__0001'}, False)
JeopardyQuestion.l({'question3': 'test__0001'}, True)

uuid1=str(JeopardyQuestion.l({'fname': 'test__0001'}, False)[0])
uuid2=str(node_col.l({'fname': 'test__0002'}, False)[0])
uuid3=str(node_col.l({'fname': 'test__0003'}, False)[0])
uuid4=str(node_col.l({'fname': 'test__0004'}, False)[0])
uuid5=str(node_col.l({'fname': 'test__0005'}, False)[0])
uuid6=str(node_col.l({'fname': 'test__0006'}, False)[0])
uuid7=str(node_col.l({'fname': 'test__0007'}, False)[0])


JeopardyCategory.q('name=home')

t=[]






#############

two_way_relationship = [
    two_way('JeopardyQuestion.hasCategory', 'JeopardyCategory.hasQuestion'),
    two_way('JeopardyQuestion.hasAssociatedQuestion', 'JeopardyQuestion.associatedQuestionOf')
    ]















clt1.metal_load({'text_field': 'who?'})


uuid1=str(clt1.metal_load({'text_field': 'test__0001'}, False)[0])
uuid1=str(clt2.metal_load({'text_field_clt2': 'test__0001'}, False)[0])

uuid2=clt1.metal_load({'text_field': 'w1','hasRef1': uuid1}, False)



clt1.metal_query(uuid2[0])
clt1.metal_query(uuid2[0], 'hasRef1:text_field_clt2')


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


# ---------------

# multiple props
# mix prop and refs
# mix prop and unresolved refs

self = JeopardyQuestion
col = JeopardyQuestion
to_load={'question': 'why?', 'hasCategory': 'Politics'}

uuid=JeopardyQuestion.l(to_load, False)

JeopardyQuestion.l(to_load, True)


r=col.q('Politics')
col.q('Politics')

JeopardyQuestion.q(uuid[0], 'hasCategory:name')


JeopardyQuestion.l({'question': 'who?', 'hasCategory': 'Ontology'})



# simple ref
# 2 way ref
# prop with vector / named vector
# pure ref one way dict
# pure ref two way dict
# pure ref array
# mix prop and resolved refs
# mix prop and resolved 2 way refs
# already existing ref





client = weaviate.connect_to_local()
metal_client=get_metal_client(client)
JeopardyQuestion=metal_client.get_metal_collection('JeopardyQuestion')
JeopardyCategory=metal_client.get_metal_collection('JeopardyCategory')


search_unique_ref_uuid(JeopardyQuestion, 'Politics')

r=col.q('politics')


