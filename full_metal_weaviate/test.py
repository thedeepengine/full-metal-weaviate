import os
import unittest
from weaviate import WeaviateClient
from weaviate.connect import ConnectionParams
from weaviate.auth import AuthApiKey
import weaviate.classes as wvc
from weaviate.classes.config import Property, DataType, ReferenceProperty, Configure, Tokenization


JeopardyQuestion = client.collections.create(
    name="JeopardyQuestion",
    description="A Jeopardy! question",
    properties=[
        Property(name="question", data_type=DataType.TEXT),
        Property(name="answer", data_type=DataType.TEXT),
        Property(name="points", data_type=DataType.INT)
    ]
)

# JeopardyQuestion.config.add_property(Property(name="points",data_type=DataType.INT))

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


###########################################################################
######### test weaviate client
###########################################################################

### Employee

opposite_refs = ['Employee.hasDepartment<>Department.hasEmployee',
                 'ClassTest.hasAttribute<>AttributeTest.hasClass']

client_weaviate=get_weaviate_client('localhost')
client=metal(client_weaviate, opposite_refs)

### jeopardy collection

opposite_refs = ['JeopardyQuestion.hasCategory<>JeopardyCategory.hasQuestion']

client_weaviate=get_weaviate_client('localhost')
client=metal(client_weaviate, opposite_refs)


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
operations=JeopardyQuestion.metal_compiler.parseString(i,parse_all=True)
w_filter=get_composed_weaviate_filter(JeopardyQuestion,operations[0])

col=JeopardyQuestion
chain_split=['hasCategory', 'title']
ref_naming_checker(JeopardyQuestion, ['hasCatkegory', 'tikktle'])

operations.tolist()

######## test metal_load
class_clt=client.get_metal_collection('ClassTest')

# register opposite references

opposite_refs = ['ffdsfs']
# node_col.config.add_reference(ReferenceProperty(name="instanceOf",target_collection=classname))

register_opposite(client,opposite_refs)


######## full update






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

