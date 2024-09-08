import os
import unittest
from weaviate import WeaviateClient
from weaviate.connect import ConnectionParams
from weaviate.auth import AuthApiKey
import weaviate.classes as wvc
from weaviate.classes.config import Property, DataType, ReferenceProperty, Configure, Tokenization

import full_metal_weaviate


JeopardyQuestion = client.collections.create(
    name="JeopardyQuestion",
    description="A Jeopardy! question",
    properties=[
        Property(name="question", data_type=DataType.TEXT),
        Property(name="answer", data_type=DataType.TEXT),
    ]
)

testttt = client.collections.create(
    name="Testtt",
    properties=[
        Property(name="que_s-tion", data_type=DataType.TEXT),
    ]
)

client.collections.delete('Testtt')
a=client.collections.get('Testtt')

testttt


JeopardyCategory = client.collections.create(
    name="JeopardyCategory",
    description="A Jeopardy! category",
    properties=[
        Property(name="title", data_type=DataType.TEXT)
    ]
)

JeopardyQuestion.config.add_reference(
    ReferenceProperty(
        name="hasCategory",
        target_collection="JeopardyCategory"))

JeopardyCategory.config.add_reference(
    ReferenceProperty(
        name="hasQuestion",
        target_collection="JeopardyQuestion"))




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



######### test weaviate client

opposite_refs = ['Employee.hasDepartment<>Department.hasEmployee',
                 'ClassTest.hasAttribute<>AttributeTest.hasClass']

client_weaviate=get_weaviate_client('localhost')
client=metal(client_weaviate, opposite_refs)



######## test metal_load
class_clt=client.get_metal_collection('ClassTest')

# register opposite references

opposite_refs = ['ffdsfs']
# node_col.config.add_reference(ReferenceProperty(name="instanceOf",target_collection=classname))

register_opposite(client,opposite_refs)


######## full update

client_weaviate=get_weaviate_client('localhost')
client=metal(client_weaviate)




class_clt.metal_load({'name': 'testName'})

col=class_clt
self=class_clt
to_load={'name': 'testName'}
new_uuid,obj = list(zip(uuids,ready_obj_copy))[0]


class_clt.q('uuid=7286e319-7a09-43d3-997e-37ef43629fd2')
class_clt.q('name=testName')


def parent(value):
    try:
        v = f(value)
        return value
    except Exception as e:
        console.print(str(e))
        # raise e
    
def f(value):
    print('do stuff')
    if value == 2:
        raise Exception('[bold red]dsfdsfdfds:[/] mfs')

parent(2)


data = {
    'A': [10, 20, 10, 30],
    'B': [20, 30, 20, 40]
}
df = pd.DataFrame(data)

# Using .query() to filter data
filtered_df = df.query('A==10&B==20')
print(filtered_df)







######## test metal_query



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
if __name__ == '__main__':
    unittest.main(argv=[''], exit=False)




_(a).metadata.vector.q()



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

if __name__ == '__main__':
    unittest.main(argv=[''], exit=False)

#### test on raise 


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

f()





is_match = bool(re.match(regex_pattern, 'name=that is a value'))

#### test translate_filters

regex_pattern = r'^(?:' + '|'.join(allowed_fields) + r'|\w+)'
a=re.search(regex_pattern, 'nafdsmfed')

a.group()

client = get_weaviate_client('localhost')

m_client = metal(client)
node_col = m_client.get_metal_collection('NodeTest')


node_col.metal_props

jmespath.search(f'types.{self.name}.{prop_ref}', self.metal_context)

jmespath.search(f'types.{node_col.name}.hasChildren', node_col.metal_context)
a=jmespath.search(f'types.{node_col.name}.named', node_col.metal_context)




node_col.metal_context['fields']['NodeTest']['properties'].values()


def maybe(value) -> Maybe:
    if value is not None:
        return Some(value)
    return Nothing()


self = node_col


safe_jmes_search(f'typefdsfds', self.metal_context).unwrap()

maybe_value = maybe(5)
result = maybe_value.value_or(0)
print(result)

maybe_value_none = get_maybe_value(None)
result_none = maybe_value_none.value_or(0)  # Outputs 0
print(result_none)

node_col.q()

@patch('your_module.is_uuid_valid')
@patch('your_module.get_ref_filter')
@patch('your_module.Filter2')
def test_uuid_validity(self, mock_filter2, mock_get_ref_filter, mock_is_uuid_valid):
    mock_is_uuid_valid.return_value = True
    mock_filter = MagicMock()
    mock_filter2.by_property.return_value = mock_filter
    mock_filter.__getitem__.side_effect = lambda op: lambda x: f"filtered by {op} with {x}"

    result = self.instance.translate_filters('123e4567-e89b-12d3-a456-426614174000')

    mock_is_uuid_valid.assert_called_once_with('123e4567-e89b-12d3-a456-426614174000')
    self.assertTrue('uuid=' in result)


metal_query()


jmespath.search(f'types.{self.name}.{prop_ref}', self.metal_context)

filters_str = 'name=nameValue'

from unittest.mock import Mock

self = Mock()
self.name = 'TestCol'
self.metal_context = {'types': {self.name: {'name': 'TEXT'}},
                      'fields': {self.name: {'properties': 'name'}}}


filters_str = 'name=nameValue'
filters_str = 'name=nameValue&hasChildren.name=childrenValue'
filters_str='instanceOf.name=Roommate&name any user_ids'

filters_str='instanceOf.name=Rooffjidsofd=fdsfsdmmate&name any user_ids'
filters_str = 'name=nameVdisfdso=alue'

filters_str = "name=John Doe& profession=Software Developer"
filters_str = "description=This value >= that value & count=10"
filters_str = "description=This value >= that value"

filters_str = "name=John Doe& profession='Software Developer'"


filters_str = "description=This value >= that value & (count=10 | hasChildren.name=hhhh)"

pattern_and=r'([_A-Za-z][_0-9A-Za-z]{0,230})\s*(!=|=|<=|<|>=|>|~|any|all)\s*([^&]*)'
conditions = re.findall(pattern, filters_str)

conditions = re.findall(r'([a-zA-Z0-9_.]+)\s*(!=|=|<=|<|>=|>|~|any|all)\s*([^|]*)', filters_str)

conditions

filters_str = "name=John Doe && age>=30 || city=New York"

conditions = re.findall(r'([_A-Za-z][_0-9A-Za-z]{0,230})\s*(=|!=|<=|>=|<|>|~|any|all)\s*([^&|]*)(\s*(&|\|\|)?\s*)', filters_str)
conditions

translate_filters(self, 'name=nameValue&hasChildren.name=childrenValue')




import re
import ast

def parse_conditions(filters_str):
    print(f"Initial filter string: {filters_str}")
    result = evaluate_conditions_recursively(filters_str)
    return result

def evaluate_conditions_recursively(filters_str):
    print(f"Processing recursive input: {filters_str}")
    # Base case: No more parentheses to process
    if '(' not in filters_str:
        return evaluate_conditions(filters_str)

    # Recursive case: Process innermost parentheses first
    while '(' in filters_str:
        # Process and replace innermost expressions
        filters_str = re.sub(r'\(([^()]*?)\)', lambda m: repr(evaluate_conditions(m.group(1))), filters_str)
        print(f"Post-parentheses evaluation: {filters_str}")
    return evaluate_conditions(filters_str)

def evaluate_conditions(condition_str):
    print(f"Evaluating condition string: {condition_str}")
    if isinstance(condition_str, str) and condition_str.startswith('{'):
        condition_str = ast.literal_eval(condition_str)  # Safely convert string representation of dictionary back to dictionary

    if isinstance(condition_str, dict):
        return condition_str

    results = []
    and_blocks = condition_str.split('&&')
    for block in and_blocks:
        or_results = []
        or_conditions = block.split('||')
        for cond in or_conditions:
            if cond.strip().startswith('{'):
                cond = ast.literal_eval(cond.strip())  # Convert back to dict if it's a string representation of a dictionary
            if isinstance(cond, str):
                parsed = parse_condition(cond.strip())
            else:
                parsed = cond  # Already a dictionary
            if parsed:
                or_results.append(parsed)
        if len(or_results) > 1:
            results.append({'OR': or_results})
        elif or_results:
            results.extend(or_results)

    if len(results) > 1:
        return {'AND': results}
    elif results:
        return results[0]
    return None

def parse_condition(cond_str):
    print(f"Parsing individual condition: {cond_str}")
    pattern = r'([_A-Za-z][_0-9A-Za-z]{0,230})\s*(=|!=|<=|<|>=|>|~|any|all)\s*([^&|]*)'
    match = re.match(pattern, cond_str)
    if match:
        key, operator, value = match.groups()
        print(f"Matched condition - field: {key}, operator: {operator}, value: {value}")
        return {'field': key, 'operator': operator, 'value': value.strip()}
    return None

# Example usage
filters_str = "(name=John Doe && age>=30) || (city=New York && country=USA)"
result = parse_conditions(filters_str)
print(f"Final output: {result}")















from pyparsing import (
    Word, alphas, alphanums, Regex, Suppress, Group, Forward, infixNotation, opAssoc
)

# Elements
ident = Word(alphas, alphanums + "_")
operator = Regex("==|!=|<=|>=|<|>|=").setName("operator")
value = Regex(r'\"[^\"]*\"|[^\s\)\(&|]+')
condition = Group(ident + operator + value)

# Parentheses
lpar = Literal("(")
rpar = Literal(")")

# Define expression
expr = Forward()
atom = condition | Group(lpar + expr + rpar)

# Define operators
expr << infixNotation(atom,
    [
        ("&&", 2, opAssoc.LEFT, lambda t: {"and": [t[0][0], t[0][2]]}),
        ("||", 2, opAssoc.LEFT, lambda t: {"or": [t[0][0], t[0][2]]}),
    ])

expr.setDebug(True)  # Enable debugging for the whole expression
input_string = "(name==\"John Doe\" && age>=30) || (city==\"New York\" && country==\"USA\")"
input_string = "(name==\"John Doe\" && age>=30)"
result = expr.parseString(input_string, parseAll=True)[0]

print(result)






from pyparsing import Word, alphas, alphanums, Regex, Group, Suppress, Forward, infixNotation, opAssoc

# Elements
ident = Word(alphas, alphanums + "_")  # Identifiers can include letters, numbers, and underscores
operator = Regex("==|!=|<=|>=|<|>|=")  # Supported operators
value = Regex(r'\"[^\"]*\"|[^\s]+')    # Values can be quoted strings or continuous non-space characters

# Group the elements to form a condition
condition = Group(ident + operator + value)

# Define a placeholder for expressions using Forward to allow for recursive definitions
expr = Forward()

# Operators
and_operator = "&&"
or_operator = "||"

# Define expression using infixNotation to handle nested operations and precedence
expr <<= infixNotation(condition,
    [
        (and_operator, 2, opAssoc.LEFT, lambda t: {'and': [t[0], t[2]]}),
        (or_operator, 2, opAssoc.LEFT, lambda t: {'or': [t[0], t[2]]}),
    ])

# Test input with mixed operators
input_string = "(age >= 30 && name == \"John Doe\") || city == \"New York\""

# Parse the input
result = expr.parseString(input_string, parseAll=True)

# Print the result
print("Parsed conditions:", result.asList())






from pyparsing import infixNotation, opAssoc, Word, nums

# Define the basic element: integers
integer = Word(nums).setParseAction(lambda tokens: int(tokens[0]))
integer.setParseAction(lambda tokens: int(tokens[0]))  # Corrected the chaining of setParseAction

# Define operations with correct structuring for output
def multiply(tokens):
    # tokens will be a list like [operand1, '*', operand2]
    return tokens[0] * tokens[2]

def add(tokens):
    # tokens will be a list like [operand1, '+', operand2]
    return tokens[0] + tokens[2]

# Define the grammar using infixNotation with correct order of operations
expression = infixNotation(integer,
    [
        ('*', 2, opAssoc.LEFT, multiply),  # multiplication with left associativity
        ('+', 2, opAssoc.LEFT, add),       # addition with left associativity
    ])

# Test input
input_string = "3 + 4 * 2"

# Parse the input
result = expression.parseString(input_string)

# Print the result
print("Result of parsing:", result)



input_string = "3 + 4"

# Parse the input
try:
    result = expression.parseString(input_string, parseAll=True)
    print(result[0])  # Correctly outputs the evaluated result
except Exception as e:
    print("Error:", e)





from pyparsing import infixNotation, opAssoc, Word, nums, oneOf

# Define the basic element: integers
integer = Word(nums)
integer.setParseAction(lambda t: int(t[0]))

# Define operations
def multiply(t):
    return t[0] * t[2]

def add(t):
    return t[0] + t[2]

# Define the expression grammar
expression = infixNotation(integer,
    [
        ('*', 2, opAssoc.LEFT, multiply),
        ('+', 2, opAssoc.LEFT, add)
    ])

# Input string
input_string = "3 + 4 * 2"

# Parse and evaluate the expression
result = expression.parseString(input_string)

# Output the result
print(result)


from pyparsing import infix_notation,int_expr,one_of

expr = infix_notation(int_expr,
    [
        (one_of("+ -"), 2, opAssoc.LEFT),
    ],
    lpar="<",
    rpar=">"
    )
expr.parse_string("3 - <2 + 11>")

expr = infix_notation(int_expr,
    [
        ('*', 2, opAssoc.LEFT, multiply),
        ('+', 2, opAssoc.LEFT, add)
    ],
    )
expr.parse_string("3 - <2 + 11>")




from pyparsing import pyparsing_common, infix_notation, opAssoc

integer = pyparsing_common.integer


expr = infix_notation(integer, [
    ("+", 2, opAssoc.LEFT),
    ("-", 2, opAssoc.LEFT)
])

result = expr.parseString("1 + 2 - 3")
print(result.asList())





expr = infix_notation(integer, [
        ('&', 2, opAssoc.LEFT),
        ('|', 2, opAssoc.LEFT)
])

result = expr.parseString("1 | 2 & 3")
print(result.asList())


def f(t):
    print(t)
    print(t[0])
    return t[0]



################################################ 111111
################################################

from pyparsing import pyparsing_common, infix_notation, opAssoc, Literal, Regex, Group, Word, Forward


integer = pyparsing_common.integer
integer.set_debug(False)
expr = infix_notation(integer, [
        ('&', 2, opAssoc.LEFT,lambda t: {"&": [t[0][0], t[0][2]]}),
        ('|', 2, opAssoc.LEFT)
])

result = expr.parse_string("1 | 2 & 3")
expr.setDebug(False)
print(result.asList())


################################################ 22222
################################################

ident = Word(alphas, alphanums + "_"+".")
operator = Regex("==|!=|<=|>=|<|>|=").setName("operator")
# value = Regex(r'\"[^\"]*\"|[^\s\)\(&|]+')
value = Regex(r'aaa')
condition = Group(ident + operator + value)

lpar = Literal("(")
rpar = Literal(")")

expr = Forward()
atom = condition | Group(lpar + expr + rpar)

expr = infix_notation(atom, [
        ('&', 2, opAssoc.LEFT,lambda t: {"and": [t[0][0], t[0][2]]}),
        ('|', 2, opAssoc.LEFT,lambda t: {"or": [t[0][0], t[0][2]]})
])

expr.parse_string('ddd=aaa&ff=aaa|gg=aaa')

################################################ 33333
################################################

ident = Word(alphas, alphanums + "_"+".")

ident = Regex("[_A-Za-z][_0-9A-Za-z]{0,230}")
operator = Regex("!=|=|<=|<|>=|>|~|any|all").setName("operator")
value = Regex(r'\"[^\"]*\"|[^\s\)\(&|]+')
value = Regex(r'\"[^\"]*\"|[^=<>~!&|()\s]+')

condition = Group(ident + operator + value)
lpar, rpar = map(Literal, "()")

expr = Forward()
atom = condition | Group(lpar + expr + rpar)

expr = infix_notation(atom, [
        ('&', 2, opAssoc.LEFT,lambda t: {"and": [t[0][0], t[0][2]]}),
        ('|', 2, opAssoc.LEFT,lambda t: {"or": [t[0][0], t[0][2]]})
])
expr.parse_string('ddd=aaa&ff=aaa|gg=that is a value')

# ^\s*([_A-Za-z][_0-9A-Za-z]{0,230})\s*(!=|=|<=|<|>=|>|~|any|all)\s*(.*)$






################################################ 444444
################################################

from pyparsing import *

# Define specific allowed identifiers.
allowed_idents = ["age", "name", "status", "date", "location"]
ident = oneOf(allowed_idents)

# Define the operator using a regular expression.
operator = Regex("!=|=|<=|<|>=|>|~|any|all").setName("operator")

# Modify the value regex to capture groups of non-operator/non-parenthesis characters,
# including spaces, but not extending into an operator or a new condition.
value = Regex(r'(?:[^=<>~!&|()\s"]+|"[^"]*")(?:\s+[^=<>~!&|()\s"]+)*')

# Define the condition as a group of identifier, operator, and value.
condition = Group(ident + operator + value)

# Define literals for parentheses.
lpar, rpar = map(Literal, "()")

# Forward declaration for recursive patterns.
expr = Forward()

# Define an atom as either a condition or a parenthesized expression.
atom = condition | Group(lpar + expr + rpar)

# Use infixNotation to handle logical AND and OR operators.
expr <<= infixNotation(atom, [
    ('&', 2, opAssoc.LEFT, lambda t: {"and": [t[0][0], t[0][2]]}),
    ('|', 2, opAssoc.LEFT, lambda t: {"or": [t[0][0], t[0][2]]})
])

# Parsing the string with updated rules.
result = expr.parseString('name="that is a value" & status=activedsdsa fds fds | age= djjjjf ffff')
print(result.asDict())


expr.set_debug(True)
atom.set_debug(True)
print(expr.parse_string('ddd=aaa&ff=aaa|gg=aaa').asList())


print(expr.parse_string('ddd=aaa&ff=aaa|hasChildren.name=aaa').asList())



################################################ 5555555
################################################

def get_ident(allowed_fields): # sub optimal checking for authorised fields
    base_ident = oneOf(allowed_fields)
    subfield_ident = Regex("[_A-Za-z][_0-9A-Za-z]{0,230}")
    ident = Combine(base_ident + ZeroOrMore('.' + subfield_ident))
    return ident

def get_expr(allowed_fields):
    ident = get_ident(allowed_fields)
    operator = Regex("!=|=|<=|<|>=|>|~|any|all").setName("operator")
    value = Regex(r'(?:[^=<>~!&|()\s"]+|"[^"]*")(?:\s+[^=<>~!&|()\s"]+)*')
    condition = Group(ident + operator + value)
    lpar, rpar = map(Literal, "()")
    expr = Forward()
    atom = condition | Group(lpar + expr + rpar)
    expr <<= infixNotation(atom, [
        ('&', 2, opAssoc.LEFT, lambda t: {"and": [t[0][0], t[0][2]]}),
        ('|', 2, opAssoc.LEFT, lambda t: {"or": [t[0][0], t[0][2]]})
    ])
    return expr

allowed_fields = node_col.metal_props + node_col.metal_refs
expr = get_expr(allowed_fields)
expr.parseString('name="that is a value" & childrenOf=activedsdsa fds fds | name= djjjjf ffff')

expr.parseString('name=that is a value & childrenOf.name=activedsdsa fds fds | map= djjjjf ffff')




a=oneOf(allowed_fields)
base_ident.parse_string('naffme')



################################################ 666666
################################################
from pyparsing import *

d=Combine(one_of(['a', 'b']))
d.parse_string('gg')


def customOneOf(allowed_fields):
    regex_pattern = r'^(?:' + '|'.join(allowed_fields) + r'|\w+)'
    return Regex(regex_pattern)

def ff(x, allowed_fields):
    print('HHHH', x)
    regex_pattern = r'^(?:' + '|'.join(map(re.escape, allowed_fields)) + ')'
    is_match = bool(re.match(regex_pattern, x[0]))
    if not is_match:
        raise Exception('NNNOOOOT GOOD')

def get_ident(allowed_fields): # sub optimal checking for authorised fields
    base_ident=customOneOf(allowed_fields)
    base_ident.add_parse_action(lambda x: ff(x,allowed_fields))
    subfield_ident = Regex("[_A-Za-z][_0-9A-Za-z]{0,230}")
    ident = Combine(base_ident + ZeroOrMore('.' + subfield_ident))
    return ident

def get_expr(allowed_fields):
    ident = get_ident(allowed_fields)
    operator = Regex("!=|=|<=|<|>=|>|~|any|all").setName("operator")
    value = Regex(r'(?:[^=<>~!&|()\s"]+|"[^"]*")(?:\s+[^=<>~!&|()\s"]+)*')
    condition = Group(ident + operator + value)
    lpar, rpar = map(Literal, "()")
    expr = Forward()
    atom = condition | Group(lpar + expr + rpar)
    expr <<= infixNotation(atom, [
        ('&', 2, opAssoc.LEFT, lambda t: {"and": [t[0][0], t[0][2]]}),
        ('|', 2, opAssoc.LEFT, lambda t: {"or": [t[0][0], t[0][2]]})
    ])
    return expr

allowed_fields = node_col.metal_props + node_col.metal_refs
expr = get_expr(allowed_fields)

expr.parseString('name=that is a value',parse_all=True)

expr.parseString('name=that is a value&hasChildren=that other value',parse_all=True)

try:
    expr.parseString('name=that is a value&hasChldren.name=that other value',parse_all=True)
    print("Parsed successfully:", result)
except ParseException as pe:
    print("Error:", pe)

################################################ 777777777
################################################

allowed_fields = node_col.metal_props + node_col.metal_refs

expr = get_expr(allowed_fields)
r=expr.parseString('name="that is a value" & childrenOf=activedsdsa fds fds | name= djjjjf ffff',parse_all=True)

expr.parseString('name="that is a value" & childrenOf.name=activedsdsa fds fds | name= djjjjf ffff',parse_all=True)

expr.parseString('name="that is a value" & childrenOf.name=activedsdsa fds fds | nadme= djjjjf ffff',parse_all=True)


def translate_filter(self, filter_str):
    if filter_str is None:
        return None
    if is_uuid_valid(filter_str):
        filter_str=f'uuid={filter_str}'


r=r.as_list()
r[0]['or'][0]['and']


prop=r[0][0]

prop = 'name'
prop = 'hasChildren.name'
prop='childrenOf'
op_symbol='='
value='activedsdsa fds fds'

filter_str='hasChildren.hasOntology.name=childrenValue'
r=expr.parseString(filter_str,parse_all=True).as_list()

a=str_filter_to_weaviate_filter(r[0][0], r[0][1], r[0][2])

# single refs
filter_str = 'hasChildren.name=childrenValue'
filter_str='hasChildren.hasOntology.name=childrenValue'
filter_str='name=nameValue&content=contentValue|(hasChildren.name=childrenName)'
r=expr.parseString(filter_str,parse_all=True).as_list()



from functools import reduce
from operator import and_, or_

operations = [
    {'or': [
        {'and': [
            ['name', '=', 'that is a value'],
            ['childrenOf.name', '=', 'activedsdsa fds fds']
        ]},
        ['name', '=', 'djjjjf ffff']
    ]}
]
a=process_operations(operations)[0]


get_weaviate_filter('childrenOf', '=', 'activedsdsa fds fds')
# Process the operations
result = process_operations(operations)
print(result)


reduce(and_, [a,a])


# chained refs
'hasChildren.hasOntology.name=childrenValue'

# uuid
'uuid=validUuid'

# prop
'name=nameValue'

# logical and
'name=nameValue&content=contentValue'
# logical or
'name=nameValue|content=contentValue'

# mix logical and/or
'name=nameValue&content=contentValue|(hasChildren.name=childrenName)'


from pyparsing import *

# Set of allowed base identifiers.
allowed_idents = ["name", "age", "status"]
base_ident = oneOf(allowed_idents)

# Additional identifier pattern that follows a dot.
subfield_ident = Regex("[_A-Za-z][_0-9A-Za-z]{0,230}")

# Full identifier starting with a base identifier, optionally followed by dot-separated subfields.
ident = Combine(base_ident + ZeroOrMore('.' + subfield_ident))

# Test the ident parser with various examples.
examples = [
    "name",
    "nafjme.subfield",
    "age.other3subfield",
    "name.subfield1.sub_field2",
    "name.subfield1.sub_field2.subfield3"
]

# Parsing and printing the outputs for each example.
for example in examples:
    result = ident.parseString(example)
    print(f"'{example}' -> {result[0]}")















from pyparsing import nestedExpr
nested_parser = nestedExpr('(', ')')
expression = "((3 + (4*5)) / 7)"
parsed_expression = nested_parser.parseString(expression)
print(parsed_expression.asList())





# Elements
ident = Word(alphas, alphanums + "_")
operator = Regex("==|!=|<=|>=|<|>|=").setName("operator")
value = Regex(r'\"[^\"]*\"|[^\s\)\(&|]+')
condition = Group(ident + operator + value)

# Define expression
expr = Forward()
atom = condition | Group(lpar + expr + rpar)

# Define operators
expr << infixNotation(atom,
    [
        ("&&", 2, opAssoc.LEFT, lambda t: {"and": [t[0], t[2]]}),
        ("||", 2, opAssoc.LEFT, lambda t: {"or": [t[0], t[2]]}),
    ])

# Parsing the input
input_string = "(name==\"John Doe\" && age>=30) || (city==\"New York\" && country==\"USA\")"
input_string = "(name==\"John Doe\" && age>=30)"
result = expr.parseString(input_string, parseAll=True)[0]






from pyparsing import (
    infixNotation, opAssoc, Keyword, Word, alphas, alphanums, nums, oneOf
)

# Define the grammar
name = Word(alphas)
integer = Word(nums)
value = Word(alphanums + ' ')
condition_op = oneOf(">= <= == != < >")
logic_op = oneOf("& |")

# Define expressions
expr = Word(alphas) + condition_op + (value | integer)
condition = Keyword("(").suppress() + expr + Keyword(")").suppress()

# Define how to parse the logic
def parse_action(t):
    return {"field": t[0], "op": t[1], "value": t[2]}

expr.setParseAction(parse_action)

# Define logic operations parsing
def and_action(t):
    print('and', t)
    return {"&": [t[0][i] for i in range(0, len(t), 2)]}

def or_action(t):
    print('or', t)
    return {"|": [t[0][i] for i in range(0, len(t), 2)]}

# Use infix notation to handle binary operators
parsed_expr = infixNotation(condition, [
    (logic_op, 2, opAssoc.LEFT, and_action),
    (Keyword("|"), 2, opAssoc.LEFT, or_action),
])

# Example usage
data = "(name=John Doe & age>=30) | (city=New York & country=USA)"
result = parsed_expr.parseString(data, parseAll=True)
print(result)



















from pyparsing import infixNotation, opAssoc, Word, nums, oneOf

# Define the basic element: integers
integer = Word(nums)
integer.setParseAction(lambda t: int(t[0]))
integer.setDebug(True)  # Enable debugging for integers

# Define operations
def multiply(t):
    result = t[0] * t[2]
    print(f"Multiplying: {t[0]} * {t[2]} = {result}")  # Enhanced debug print
    return result

def add(t):
    result = t[0] + t[2]
    print(f"Adding: {t[0]} + {t[2]} = {result}")  # Enhanced debug print
    return result

# Define the expression grammar with debugging
expression = infixNotation(integer,
    [
        ('*', 2, opAssoc.LEFT, multiply),
        ('+', 2, opAssoc.LEFT, add)
    ])
expression.setDebug(True)  # Enable debugging for the whole expression

# Input string
input_string = "3 + 4 * 2"

# Parse and evaluate the expression
try:
    result = expression.parseString(input_string, parseAll=True)[0]
    print("Final Result:", result)
except Exception as e:
    print("Error during parsing:", e)
