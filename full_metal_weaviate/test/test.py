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
    'name,date,hasOntology:name,hasChildren>(hasChildren:name,hasAttrUuid:name>hasChildren:name),hasAttrUuid:name']

for i in t:
    print(query_expr.parseString(i, parseAll=True).asList())




if __name__ == "__main__":
    unittest.main()
