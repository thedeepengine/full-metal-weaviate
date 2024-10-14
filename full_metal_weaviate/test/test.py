import unittest
from pyparsing import *
from pyparsing import ParseException
from full_metal_weaviate.utils import StopProcessingException
from full_metal_weaviate.weaviate_op import get_ident, get_compiler


class TestGetExpr(unittest.TestCase):
    def setUp(self):
        self.allowed_fields = ["name", "age", "salary", "department"]
        self.compiler = get_compiler(self.allowed_fields)

    def test_single_conditions(self):
        operators = ["=", "!=", "<", "<=", ">", ">=", "~", "any", "all"]
        field = "fieldName"
        value = "valueName"
        for op in operators:
            test_input = f"{field} {op} {value}"
            try:
                result = self.compiler.parseString(test_input, parseAll=True)
                self.assertTrue(True)
            except ParseException:
                self.fail(f"Failed to parse valid expression: {test_input}")

    def test_conditions_with_quoted_values(self):
        test_inputs = [
            'name = "John Doe"',
            'department = "Sales and Marketing"',
            'name ~ "Doe, John"',
            'department any "Sales" "Marketing"',
            'department all "Sales" "Marketing"',
        ]
        for test_input in test_inputs:
            try:
                result = self.compiler.parseString(test_input, parseAll=True)
                self.assertTrue(True)
            except ParseException:
                self.fail(f"Failed to parse valid expression: {test_input}")

    def test_conditions_with_unquoted_values(self):
        test_inputs = [
            "name = John",
            "department = Sales",
            "name ~ Doe",
            "department any Sales Marketing",
            "department all Sales Marketing",
        ]
        for test_input in test_inputs:
            try:
                result = self.compiler.parseString(test_input, parseAll=True)
                self.assertTrue(True)
            except ParseException:
                self.fail(f"Failed to parse valid expression: {test_input}")

    def test_invalid_operators(self):
        invalid_ops = ["==", "<>", "!==", "=>", "=<", "!!", "not"]
        field = "age"
        value = "30"
        for op in invalid_ops:
            test_input = f"{field} {op} {value}"
            with self.assertRaises(ParseException):
                self.compiler.parseString(test_input, parseAll=True)

    def test_invalid_identifiers(self):
        invalid_fields = ["unknown", "123name", "$salary", "name!"]
        op = "="
        value = "value"
        for field in invalid_fields:
            test_input = f"{field} {op} {value}"
            with self.assertRaises(ParseException):
                self.compiler.parseString(test_input, parseAll=True)

    def test_logical_expressions(self):
        test_inputs = [
            "name = John & age > 30",
            "department = Sales | department = Marketing",
            "(name = John & age > 30) | department = Sales",
            "name = John & (age > 30 | salary >= 50000)",
            "((name = John & age > 30) | department = Sales) & salary >= 50000",
        ]
        for test_input in test_inputs:
            try:
                result = self.compiler.parseString(test_input, parseAll=True)
                self.assertTrue(True)
            except ParseException:
                self.fail(f"Failed to parse valid logical expression: {test_input}")

    def test_invalid_syntax(self):
        test_inputs = [
            "name =",  # Missing value
            "= John",  # Missing field
            "name John",  # Missing operator
            "name = John &",  # Incomplete expression
            "name = John |",  # Incomplete expression
            "name = John && age > 30",  # Invalid logical operator
            "name = John || age > 30",  # Invalid logical operator
            "(name = John",  # Unmatched parenthesis
            "name = John)",  # Unmatched parenthesis
            "name = (John",  # Unmatched parenthesis
        ]
        for test_input in test_inputs:
            with self.assertRaises(ParseException):
                self.compiler.parseString(test_input, parseAll=True)

    def test_nested_conditions(self):
        print('')

    def test_complex_expressions(self):
        test_inputs = {
            "(name = John & (age > 30 | department = Sales)) & salary >= 50000": [
                {
                    "and": [
                        {
                            "and": [
                                {"field": "name", "operator": "=", "value": "John"},
                                {
                                    "or": [
                                        {
                                            "field": "age",
                                            "operator": ">",
                                            "value": "30",
                                        },
                                        {
                                            "field": "department",
                                            "operator": "=",
                                            "value": "Sales",
                                        },
                                    ]
                                },
                            ]
                        },
                        {"field": "salary", "operator": ">=", "value": "50000"},
                    ]
                }
            ],
            "name = John & age > 30 & department = Sales": {
                "and": [
                    {"field": "name", "operator": "=", "value": "John"},
                    {"field": "age", "operator": ">", "value": "30"},
                    {"field": "department", "operator": "=", "value": "Sales"},
                ]
            },
            "name = John | age > 30 | department = Sales": {
                "or": [
                    {"field": "name", "operator": "=", "value": "John"},
                    {"field": "age", "operator": ">", "value": "30"},
                    {"field": "department", "operator": "=", "value": "Sales"},
                ]
            },
            "name = John & age > 30 | department = Sales & salary >= 50000": {
                "or": [
                    {
                        "and": [
                            {"field": "name", "operator": "=", "value": "John"},
                            {"field": "age", "operator": ">", "value": "30"},
                        ]
                    },
                    {
                        "and": [
                            {"field": "department", "operator": "=", "value": "Sales"},
                            {"field": "salary", "operator": ">=", "value": "50000"},
                        ]
                    },
                ]
            },
            "((name = John) & (age > 30)) | ((department = Sales) & (salary >= 50000))": {
                "or": [
                    {
                        "and": [
                            {"field": "name", "operator": "=", "value": "John"},
                            {"field": "age", "operator": ">", "value": "30"},
                        ]
                    },
                    {
                        "and": [
                            {"field": "department", "operator": "=", "value": "Sales"},
                            {"field": "salary", "operator": ">=", "value": "50000"},
                        ]
                    },
                ]
            },
        }
        for test_input in test_inputs:
            try:
                result = self.compiler.parseString(test_input, parseAll=True)
                self.assertTrue(True)
            except ParseException:
                self.fail(f"Failed to parse valid complex expression: {test_input}")





t=['name',
   'name,date,attr',
   'name,date,attr,vector',
   'name,date,attr,vector:content',
    'name,date,attr,vector:content,name,hasChildren:name',
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
    'name,date,hasOntology:name,hasChildren>(hasChildren:name,hasAttrUuid:name>hasChildren:name),hasAttrUuid:name'
    ]

for i in t:
    print(query_expr.parseString(i, parseAll=True).asList())

compiler_r = get_return_field_compiler()

t = [
    # all vector first level
    'name,date,attr,vector', 
    # single named vector first level
    'name,date,attr,vector:content', 
    # mutiple named vector first level
    'name,date,attr,vector:content,name',
    # all vectors for a reference,
    'name,date,attr:content,name,hasChildren:name,vector',
    # named vectors for a reference (has to be nested)
    'name,date,attr:content,name,hasChildren:name>(vector:name,content)',
    # vector with refs as well
    'fname,vector:name,hasAttr:name,fname,hasChildren:name>(vector:name,hasChildren:name)'
    ]


return_fields= 'name,date,attr,vector,name,hasChildren:name,vector'
return_fields='fname,vector:name,hasChildren:name>(vector:name)'
return_fields='fname,vector:name,hasAttr:name,fname,hasChildren:name>(vector:name,hasChildren:name)'
return_fields='fname,vector:name,hasAttr:name,fname,hasChildren:name>(vector:name)'

for i in t:
    r=get_weaviate_return_fields(compiler_r, i)
    print(r)


from full_metal_weaviate.main import get_weaviate_client, get_metal_client
weaviate_client = os.getenv('WEAVIATE_HTTP_HOST', 'localhost')
client_weaviate=get_weaviate_client(weaviate_client)
client=get_metal_client(client_weaviate, opposite_refs)
JeopardyQuestion=client.get_metal_collection('JeopardyQuestion')
node_col=client.get_metal_collection('NodeTest')


if __name__ == "__main__":
    unittest.main()


from full_metal_weaviate.main import get_weaviate_client, get_metal_client

weaviate_client_url = "localhost"
del client_weaviate
del node_col
client_weaviate = get_weaviate_client(weaviate_client_url)
client_weaviate = get_metal_client(client_weaviate)
node_col=client_weaviate.get_metal_collection('NodeTest')
col=node_col
self=node_col
filters=f'instanceOf.name=Home&fname any ids'
instances=col.q(f'instanceOf.name=Home&fname any ids', 
        'fname,hasChildren:attrName,name,fname,value,content,date>(hasAttr:name>hasAttrUuid:name,hasChildren:name,value)',
        context={'ids': ['10037A1'] })

instances=col.q(f'instanceOf.name=Home', 'fname,hasChildren:attrName,name,fname,value,content,date>(hasAttr:name>hasAttrUuid:name,hasChildren:name,value)',)

filters_str=f'instanceOf.name=Home'
return_fields='fname'
instances=col.q(f'instanceOf.name=Home', 'fname')


node_col.query.fetch_objects()

res = self.query.fetch_objects()
res = self.query.fetch_objects(filters=w_filter,return_properties=ret_prop)


return_fields='fname,hasChildren:attrName,name,fname,value,content,date>(hasAttr:name>hasAttrUuid:name,hasChildren:name,value)'












res=[{'name': jmespath.search('properties.fname', i),
    'vector': [i for j in jmespath.search('references.hasChildren[*].vector.content', i) for i in j]} 
    for i in a]



h=jmespath.search('references.hasChildren[*].vector.content', a[0])

res[0]['name']
res[0]['vector'][0]

a[0]['references']['hasChildren'][2]['vector'].keys()



a[0]

a=node_col.q(filter_fields,return_fields)
len(a)

refs_fields=process_query_references(node_col.metal.ret_ref)

for i in refs_fields:
    print(i in filter_fields)


return_fields='hasChildren:name,vector>(hasAttr:name)'






def process_query_references(query_refs):
    result = []

    def recursive_concat(query_ref, path=""):
        current_path = path + ('.' if path else '') + query_ref.link_on
        if query_ref.return_properties:
            for prop in query_ref.return_properties:
                result.append(current_path + '.' + prop)
        if query_ref.return_references:
            if isinstance(query_ref.return_references, list):
                for sub_ref in query_ref.return_references:
                    recursive_concat(sub_ref, current_path)
            else:
                recursive_concat(query_ref.return_references, current_path)

    for query_ref in query_refs:
        recursive_concat(query_ref)

    return result

def filter_dict_by_keys(data, keys):
    def get_subdict(path, source):
        subdict = source
        for part in path.split('.'):
            if isinstance(subdict, dict) and part in subdict:
                subdict = subdict[part]
            else:
                return None
        return subdict

    result = {}
    for key in keys:
        subpath_parts = key.split('.')
        current_dict = result
        for part in subpath_parts[:-1]:
            if part not in current_dict:
                current_dict[part] = {}
            current_dict = current_dict[part]
        final_key = subpath_parts[-1]
        # Fetch the value from the original data based on the path
        value = get_subdict(key, data)
        if value is not None:
            current_dict[final_key] = value

    return result

# Example usage
data = {
    'key1': {
        'key2': {
            'key3': 'value1'
        },
        'key4': 'value2'
    },
    'keya': {
        'keyb': {
            'keyc': 'value3'
        },
        'keyd': 'value4'
    }
}

paths = ['key1.key2', 'keya.keyb.keyc']
filtered_dict = filter_dict_by_keys(data, paths)
print(filtered_dict)
