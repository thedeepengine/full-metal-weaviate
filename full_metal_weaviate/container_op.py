import jmespath
from functools import reduce
from types import FunctionType
import pandas as pd
from rich.console import Console
from returns.maybe import Maybe, Some, Nothing

console = Console()

class Metal:
    def __init__(self, data):
        self.data = data
        self.mode = 'full'
        self.nestedtemp = False

    @property
    def inplace(self): # inplace not working
        self.mode = 'inplace'
        return self

    @property
    def full(self):
        self.mode = 'full'
        return self

    @property
    def modified(self):
        self.mode = 'modified'
        return self

    @property
    def nested(self):
        self.nestedtemp = True
        return self
    
    def get(self, fields, names = {}, pattern =''):
        if isinstance(self.data, dict):
            if isinstance(fields, FunctionType):
                return dict(filter(fields, self.data.items()))
            if isinstance(fields, str):
                return jmespath.search(fields, self.data)
            if isinstance(fields, list):
                if type(fields) == str:
                    fields = [fields]
                return {k:v for k,v in self.data.items() if k in fields}
        if isinstance(self.data, list):
            if isinstance(fields, FunctionType):
                if isinstance(self.data[0], dict):
                    return [dict(filter(fields, i.items())) for i in self.data]
            if type(fields) == str:
                fields = [fields]
            if self.nestedtemp == True:
                return [__(i).nested_to_flat(fields, pattern=pattern, names=names) for i in self.data]
            res = [{k:v for k,v in i.items() if k in fields} for i in self.data]
            if len(fields) == 1:
                res = [i[fields[0]] for i in res]
            return res
    
    def filter(self, func):
        if isinstance(self.data, list):
            return [i for i in self.data if func(i)]

    def remove(self, fields):
        if type(fields) == str:
            fields = [fields]
        if isinstance(self.data, dict):
            return {k:v for k,v in self.data.items() if k not in fields}
        if isinstance(self.data, list):
            res = [{k:v for k,v in i.items() if k not in fields} for i in self.data]
            return res

    def apply(self, func, fields=None):
        # case apply recursively to a nested property
        if type(fields) == str:
            fields = [fields]
        if isinstance(self.data, dict):
            console.print(f'data type: {type(self.data)}. Object should be an array to apply function on.')
            # raise Exception('object should be an array to apply function on')
            return
        
        if isinstance(self.data[0], dict) and fields !=None: # TODO: to be improved as it just check if first item is a dict
            # if '.' in fields:
            #     nesting = fields.split('.')
            res = [{k: (func(v) if k in fields else v) for k, v in i.items()} for i in self.data]

            if self.mode == 'full':
                return res
            elif self.mode == 'modified':
                return [{k: v for k, v in row.items() if k in fields} for row in res]
            elif self.mode == 'inplace':
                self.data = res
                return self.data
            return
        else: # for any other type than a dict
            res = [func(i) for i in self.data]
            return res

    def nested_to_flat(self, keys_to_capture,pattern='references.hasChildren',acc_id=None,delete_pattern=False,names={},temp_keep_format=False):
        """# pythonOntology.flat(keys_to_capture) WISHED SYNTAX
        I fucking almost did it
        a = _(newHierarchy).nested_to_flat(keys_to_capture='*',pattern='children')   
        """

        pattern_split = pattern.split('.')
        if keys_to_capture == '*':
            keys_to_capture = list(set(list(self.data.keys())) - set([pattern[0]]))
        if isinstance(keys_to_capture, str):
            keys_to_capture = [keys_to_capture]

        def rget(data, keys):
            def safe_get(acc, key):
                if isinstance(acc, list):
                    return [safe_get(item, key) for item in acc]
                return acc.get(key, None) if isinstance(acc, dict) else None
            return reduce(safe_get, keys, data)

        def get_level(data, keys_to_capture):
            level_data = [rget(data, key.split('.')) for key in keys_to_capture]
            zipped = [(x, y) for x, y in zip(keys_to_show, level_data) if y is not None]
            return dict(zipped)

        def all_levels(data, keys_to_capture, acc_id,acc_id_value):
            if len(data) == 0:
                return []
            # keys_to_capture = [pattern+'.'+i for i in keys_to_capture]
            r = get_level(data, keys_to_capture)
            results = [r] if len(r) > 0 else []
            if acc_id:
                acc_id_value='.'.join([acc_id_value, results[0][acc_id]])
                results[0]['acc_id']=acc_id_value
            children = rget(data, pattern_split)
            if isinstance(children, list):
                for child in children:
                    res = all_levels(child,keys_to_capture,acc_id,acc_id_value)
                    if len(res) > 0 and len(res[0]) > 0:
                        results.extend(res)
            return results

        def keys_to_show_strategy(keys_to_capture, names):
            keys_to_capture = [names.get(item, item) for item in keys_to_capture]
            keys_to_capture_end = [i.split('.')[-1] for i in keys_to_capture]
            all_distinct = len(keys_to_capture_end) == len(set(keys_to_capture_end))
            if all_distinct:
                keys_to_show = keys_to_capture_end
            else:
                keys_to_show = keys_to_capture
            return keys_to_show

        keys_to_show = keys_to_show_strategy(keys_to_capture, names)

        res = all_levels(self.data, keys_to_capture, acc_id, '')
        if temp_keep_format == False and len(res) == 1:
            return res[0]
        return res
    
    def extract_nested(self, path):

        def extract(data, path):
            keys = path.split('.')
            print(keys)
            for key in keys:
                if isinstance(data, list):
                    print(data)
                    data = [extract(item, keys[0]) for item in data if keys[0] in item]
                elif isinstance(data, dict):
                    if key == '*':
                        data = extract(jmespath.search('*', data)[0], '.'.join(keys[1:]))
                    else:
                        print('key', key)
                        data = data.get(key, None)
                else:
                    return data
            return data

        if isinstance(self.data, list):
            return [extract(item, path) for item in self.data]
        return extract(self.data, path)

    @staticmethod
    def aggregate_and_rename(key, value):
        new_key = 'children'
        aggregated_value = []
        for sub_key, sub_value in value.items():
            if isinstance(sub_value, list):
                aggregated_value.extend(sub_value)
        return new_key, aggregated_value

    def apply_to_nested(self, target_key, transform_func=None):
        if transform_func is None:
            transform_func = self.aggregate_and_rename

        def recurse(data, target_key):
            if isinstance(data, dict):
                new_data = {}
                for key, value in data.items():
                    if key == target_key:
                        new_key, new_value = transform_func(key, value)
                        new_data[new_key] = new_value
                    else:
                        new_data[key] = recurse(value, target_key)
                return new_data
            elif isinstance(data, list):
                return [recurse(item, target_key) for item in data]
            else:
                return data

        self.data = recurse(self.data, target_key)
        return self.data

def safe_jmes_search(query, data) -> Maybe:
    result = jmespath.search(query, data)
    if result is not None:
        return Some(result)
    return Nothing


# alias

__ = Metal