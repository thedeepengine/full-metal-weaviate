import os
import unittest
from weaviate import WeaviateClient
from weaviate.connect import ConnectionParams
from weaviate.auth import AuthApiKey
import weaviate.classes as wvc
from weaviate.classes.config import Property, DataType, ReferenceProperty, Configure, Tokenization
# from main import get_metal_client, get_weaviate_client

from full_metal_weaviate.main import get_metal_client,get_weaviate_client

global weaviate_client, client

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


parser_return_field=get_return_field_compiler()
return_fields_str = "question,name,metadata:distance,hasCategory:title,description>hasChildren:name,description,hasAssociatedQuestion:question,name"
parsed_types = parser_return_field.searchString(return_fields_str)
ret_prop,ret_ref,ret_metadata,include_vector=get_weaviate_return_fields(parsed_types)


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


# differente dimensions of information
# create categories, put together objects with similar properties, so you can act on it.
# thats pretty much what statistical models are doing, neural networks
# information reduction like in 
# moving up and down the abstraction ladder
# create relationships, as a network

# some models would fail in some odd ways.
# structural fail or data fail

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


filter='name=John&age>30&department=Sales'
filter='name=John&age>30'


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


d=get_composed_weaviate_filter(col,operations,context)

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

client=get_metal_client(client_weaviate)
node_col=client.get_metal_collection('NodeTest', force_reload = True)
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


a=__(['aa', 'bb', 'cc'])
a=__(['aa', 'bb', 'cc']).original
a=__(['aa', 'bb', 'cc']).val.startswith('a').__
a=__(['aa', 'bb', 'cc']).val.startswith('a')


# intuitively vectorize pretty much any function


a=__(['aa', 'bb', 'cc']).endswith('b').__



a=__(['aa', 'bb', 'cc']).startswith('a').__

data=['aa', 'bb', 'cc']

a=__(data).startswith('a').val.__
a=__(data).val.startswith('a').__


fields


all(isinstance(x, int) for x in fields).trueOrRaise()



monad = MetalMonad({"name": "Alice", "age": 30, "city": "New York"})
# Test
# test non nested dict
# test nested dict
Axis:
- nb_of_fields

assert monad.get(["name", 'age']) == "Alice"
assert monad.get("age") == 30
assert monad.get("city") == "New York"

monad.get("nonexistent")

# dict

## boolean function
## jmespath type
## list of keys

# list

## boolean function
## list of indexes
## list of strings/actual values

__([1,2,3,4]).get([1,2])

data=['dd', 'ggg', 'dd']
__().get(['dd'])

data={'key1': 'value1', 'key2': 'value2', 'key3': 'value3'}
__(data).get(['key1', 'key3']).__


data=['aa', 'bb', 'cc']

res=[x for x in data if x.startswith('a')]

(data).val.startswith('a')

res=__(data).val.startswith('a').__

a.data

def f(x,y):
    print(x)
    print(y)
    return x,y

f(2,3)

a = [3, 4]

def g(a):
    # h=f(*a)
    print(*a,)
    print(f(*a))
    # print(h)

g(a)


list(enumerate([*a]))

*a,
string_list = __(['aa', 'bb', 'cc'])



a=__(['aa', 'bb', 'cc']).startswith('a').filter(lambda x: x).__



__(string_list).filter(lambda x: __(x).startswith('a'))


data=['aa', 'bb', 'cc']
__(data).inplace.startswith('a')


data = MetalMonad(['aa', 'bb', 'cc'])
a=data.inplace.startswith('a').__
print(data)  #


# Example usage:
string_list = MyClass(['aa', 'bb', 'cc'])
string_list.startswith('c').data
print(result)  # Output: [True, False, False]


([i[1][2:] for i in to_load if i[1].startswith('<>')]

a=__(to_load).val.startswith('<>')

list(set([i[1][2:] for i in to_load if i[1].startswith('<>')]))

.map(lambda s: s.upper())
print(chained_result)  # Output of chained operations


list(filter(lambda x:x, [True, False, False, True]))


# function_object = locals()[function_name]
# function_object = globals()[function_name]
# function_object = getattr(builtins, function_name)



# select roommate and its child and 
# show test for return filter function

