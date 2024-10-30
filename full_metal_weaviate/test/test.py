import unittest
from pyparsing import *
from pyparsing import ParseException
from full_metal_weaviate.utils import StopProcessingException
from full_metal_weaviate.weaviate_op import get_ident, get_compiler



allowed_fields = ["name", "age", "salary", "department"]
a=get_compiler(allowed_fields)
a.parseString('name=aaaa', parseAll=True)

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





############ return

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



compiler=get_return_field_compiler()

compiler.parseString('name=aaa,', parseAll=True).asList()



from pyparsing import Word, alphas, ParseException


a = 'name=aaa,'
a = 'name=aaa'


try:
    result = compiler.parseString('name=aaa', parseAll=True).asList()
except ParseException as pe:
    print(f"Parsing stopped at character position: {pe.loc}")
    raise

a[4:]


compiler.parseString('name:aaa', parseAll=True).asList()







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




node_col.query.fetch_data


node_col.query.fetch_objects()


client_weaviate=get_metal_client('weaviate')

node_col=client_weaviate.get_metal_collection('Node')



"action":"hnsw_commit_log_maintenance","error":"stat /var/lib/weaviate/jeopardycategory/nLhSPSUPPrF6/main.hnsw.commitlog.d/1725891952: no such file or directory","level":"error","msg":"hnsw commit log maintenance failed","time":"2024-10-24T00:30:52Z"}











##################################################################################




# def custom_one_of(allowed_fields):
#     regex_pattern = r'\b(?:' + '|'.join(allowed_fields) + r')\b|\w+'
#     return Regex(regex_pattern)

# def one_of_checker(x, allowed_fields):
#         regex_pattern = r'^(?:' + '|'.join(map(re.escape, allowed_fields)) + ')$'
#         is_match = bool(re.match(regex_pattern, x[0]))
#         if not is_match:
#             raise FieldNotFoundException(x[0])

# def get_ident(allowed_fields=None):

#     def field_check(token):
#         print(token)
#         # context[token]
#         # return token
    
#     base_ident=Regex("[_A-Za-z][_0-9A-Za-z]{0,230}").setParseAction(field_check)
#     middle_ident=Regex("[_A-Za-z][_0-9A-Za-z]{0,230}")
#     middle_ident.setParseAction(lambda x: print(x))
#     final_ident=Regex("[_A-Za-z][_0-9A-Za-z]{0,230}").setParseAction(lambda x: print('final:', x))
#     ident=Combine(base_ident + ZeroOrMore('.' + middle_ident) + final_ident)
#     return ident








# from pyparsing import Regex, ZeroOrMore, Optional, ParseException, ParserElement, ParseResults

# # Enable packrat parsing for potential performance gain
# ParserElement.enablePackrat()


# def base_field_check(token, context):
#     print("field_check:", token)
#     matching_clt=[k for k,v in context['fields'].items() if token in v['all']]
#     if len(matching_clt) == 1:
#         clt_name=token[0]
#         return clt_name
#     elif len(matching_clt) > 1:
#         raise MoreThanOneCollectionException(matching_clt)
#     elif len(matching_clt) == 0:
#         raise NoCollectionException()


# def get_ident(context):

#     # def field_check(token, clt_name):
#     #     print("field_check:", token)
#     #     if allowed_fields and token[0] not in allowed_fields:
#     #         raise ParseException(f"Token '{token[0]}' not allowed.")

#     pattern = "[_A-Za-z][_0-9A-Za-z]*"
#     base_ident = Regex(pattern).setParseAction(lambda token: base_field_check(token, context))
#     middle_ident = Regex(pattern).setParseAction(lambda tokens: print("middle:", tokens[0]))
#     final_ident = Regex(pattern).setParseAction(lambda tokens: print("final:", tokens[0]))

#     ident = Combine(base_ident + ZeroOrMore('.' + middle_ident) + Optional('.' + final_ident))
#     return ident

# def get_filter_compiler(allowed_fields):
#     try:
#         ident = get_ident(allowed_fields)
#         operator = Regex("!=|=|<=|<|>=|>|~|any|all").setName("operator")
#         value = Regex(r'(?:[^=<>~!&|()\s"]+|"[^"]*")(?:\s+[^=<>~!&|()\s"]+)*')
#         condition = Group(ident + operator + value)
#         condition.setParseAction(lambda t: {'field': t[0][0], 'operator': t[0][1], 'value': t[0][2]})
#         lpar, rpar = map(Literal, "()")
#         expr = Forward()
#         g = Group(lpar + expr + rpar)
#         atom = condition | Group(lpar + expr + rpar).setParseAction(lambda t: t[1])
#         expr <<= infixNotation(atom, [
#             ('&', 2, opAssoc.LEFT, lambda t: {'and': t[0][::2]}),
#             ('|', 2, opAssoc.LEFT, lambda t: {'or': t[0][::2]})
#         ])
#         return expr
#     except StopProcessingException as e:
#         console.print(e)





# def get_return_field_compiler():
#     basic_prop=Regex("[_A-Za-z][_0-9A-Za-z]{0,230}")
#     property = Combine(basic_prop + ~FollowedBy(oneOf("> :")))
#     value=Regex("\\s*(\\*|[_A-Za-z][_0-9A-Za-z]{0,230}(\\.[_0-9A-Za-z]{1,230})*|[_A-Za-z][_0-9A-Za-z]{0,230})\\s*")
#     values = delimitedList(Combine(value + ~FollowedBy(oneOf(":"))), combine= True)
#     property.setParseAction(lambda t: {'property': t[0]})
#     nested_expr = Forward()
#     reference = Combine(value+':'+values+Suppress(Optional('>'))) | value+Suppress('>')

#     def get_ref(s,l,t):
#         print(s)
#         print('t', t)
#         ref_name = s.split(':')[0]
#         if ref_name not in allowed_refs:
#             raise InvalidFunctionException(s,l,ref_name)
#         return {'reference': t[0]}
        
#     reference.setParseAction(get_ref)
#     parenthesized = Group(Suppress('(') + nested_expr + Suppress(')'))

#     nested = reference + OneOrMore(Group(nested_expr) | Group(reference) | values | parenthesized)
#     # nested = reference + OneOrMore(nested_expr | Group(reference) | values)
#     nested.setParseAction(lambda t: {'nested':t.asList()})

#     # nested.setParseAction(lambda t: {'nested': [item for item in t.asList() if item]})

#     expression = delimitedList(nested|reference|property)
#     nested_expr <<= expression
#     return nested_expr




# def get_return_field_compiler():
#     basic_prop = Regex("[_A-Za-z][_0-9A-Za-z]{0,230}")
#     property = Combine(basic_prop + ~FollowedBy(oneOf("> :")))
#     value = Regex("\\s*(\\*|[_A-Za-z][_0-9A-Za-z]{0,230}(\\.[_0-9A-Za-z]{1,230})*|[_A-Za-z][_0-9A-Za-z]{0,230})\\s*")
#     values = delimitedList(Combine(value + ~FollowedBy(oneOf(":"))), combine=True)
#     property.setParseAction(lambda t: {'property': t[0]})
#     nested_expr = Forward()
#     reference = Combine(value + ':' + values + Suppress(Optional('>'))) | value + Suppress('>')

#     def get_ref(s, l, t):
#         print('ttt', t)
#         ref_name = t[0].split(':')[0]
#         if ref_name not in allowed_refs:
#             raise InvalidFunctionException(s, l, ref_name)
#         return {'reference': t[0]}

#     reference.setParseAction(get_ref)
    
#     parenthesized = Group(Suppress('(') + nested_expr + Suppress(')'))
#     nested = reference + OneOrMore(Group(nested_expr) | Group(reference) | values | parenthesized)
#     nested.setParseAction(lambda t: {'nested': t.asList()})

#     expression = delimitedList(nested | reference | property)
#     nested_expr <<= expression
#     return nested_expr

# def get_return_field_compiler():
#     basic_prop = Regex("[_A-Za-z][_0-9A-Za-z]{0,230}")
#     property = Combine(basic_prop + ~FollowedBy(oneOf("> :")))
#     value = Regex(r"\s*(\*|[_A-Za-z][_0-9A-Za-z]{0,230}(\.[_0-9A-Za-z]{1,230})*|[_A-Za-z][_0-9A-Za-z]{0,230})\s*")
#     values = delimitedList(Combine(value + ~FollowedBy(oneOf(":"))), combine=True)
#     property.setParseAction(lambda t: {'property': t[0]})
#     nested_expr = Forward()
#     reference = Combine(value + ':' + values + Suppress(Optional('>'))) | value + Suppress('>')
    
#     def get_ref(t):
#         return {'reference': t[0]}
    
#     reference.setParseAction(get_ref)    
#     nested = reference + OneOrMore(nested_expr | Group(reference) | values)
#     nested.setParseAction(lambda t: {'nested': t.asList()})

#     expression = delimitedList(nested | reference | property)
#     nested_expr <<= expression
#     return nested_expr


# def get_return_field_compiler(allowed_refs):
#     basic_prop=Regex("[_A-Za-z][_0-9A-Za-z]{0,230}")
#     property = Combine(basic_prop + ~FollowedBy(oneOf("> :")))
#     value=Regex("\\s*(\\*|[_A-Za-z][_0-9A-Za-z]{0,230}(\\.[_0-9A-Za-z]{1,230})*|[_A-Za-z][_0-9A-Za-z]{0,230})\\s*")
#     values = delimitedList(Combine(value + ~FollowedBy(oneOf(":"))), combine= True)
#     property.setParseAction(lambda t: {'property': t[0]})
#     nested_expr = Forward()
#     reference = Combine(value+':'+values+Suppress(Optional('>'))) | value+Suppress('>')

#     def get_ref(s,l,t):
#         print(s)
#         ref_name = s.split(':')[0]
#         if ref_name not in allowed_refs:
#             raise InvalidFunctionException(s,l,ref_name)
#         return {'reference': t[0]}
        
#     reference.setParseAction(get_ref)
#     nested = reference + OneOrMore(nestedExpr(content=nested_expr) | Group(reference) | values)
#     nested.setParseAction(lambda t: {'nested':t.asList()})

#     expression = delimitedList(nested|reference|property)
#     nested_expr <<= expression
#     return nested_expr
