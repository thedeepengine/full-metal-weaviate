import unittest

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

