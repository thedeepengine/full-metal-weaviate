import re
import jmespath
from functools import reduce
from weaviate.classes.query import MetadataQuery, Filter, QueryReference
from weaviate.exceptions import WeaviateBaseError
from types import FunctionType

from app.app_instance import get_weaviate_context

WEAVIATE_CONTEXT = get_weaviate_context()

OPERATORS = {
    '!=': 'not_equal',
    '=': 'equal',
    '<': 'less_than',
    '>': 'greater_than',
    '<=': 'less_or_equal',
    '>=': 'greater_or_equal',
    '~': 'like',
    'any': 'any',
    'all': 'all',
}

def q(self,filters_str=None,return_fields=None,context={},limit=100,include_vector=False,return_raw=False,with_metadata=False):
    filters=translate_filters(self.name,filters_str,context)
    return_properties, return_references=translate_return_fields(return_fields)

    res = self.query.fetch_objects(
        filters=filters,
        return_properties=return_properties,
        return_references=return_references,
        include_vector=include_vector,
        limit=limit)
    if not return_raw:
        res = extract_object(res, include_vector, with_metadata)
        if len(res) == 1 and hasattr(filters, 'model_dump') and filters.model_dump()['operator'] == 'Equal':
            res = res[0]
    return res

def extract_object(res, include_vector=False, with_metadata=False):
    # input_string = "hasChildren:name,surname;hasChildren:name"
    def recursive_extract(item):
        result = {
            'uuid': str(item.uuid),
            'properties': item.properties            
        }
        if item.references:
            result['references'] = {}
            for k, v in item.references.items():
                result['references'][k] = [recursive_extract(j) for j in v.objects]
        
        if with_metadata and hasattr(item, 'metadata'):
            if hasattr(item.metadata, 'distance'):
                if item.metadata.distance != None:
                    result['metadata'] = {'distance': item.metadata.distance}
            if hasattr(item.metadata, 'score'):
                if item.metadata.score != None:
                    result['metadata'] = {'score': item.metadata.score}

        if include_vector and hasattr(item, 'vector'):
            result['vector'] = item.vector

        return result

    return [recursive_extract(i) for i in res.objects]

def translate_return_fields(return_fields):
    if return_fields == None:
        return None, None
    top_split = return_fields.split(';;')
    if len(top_split) == 2:
        top_properties, nested_segments = top_split
        top_properties = [prop.strip() for prop in top_properties.split(',')]
    else:
        if ':' not in top_split[0]:
            top_properties = [prop.strip() for prop in top_split[0].split(',')]
            nested_segments = None
            all_references = None
        else:
            top_properties = None
            nested_segments = top_split[0]
    
    if nested_segments != None:
        all_references = []
        all_paths = nested_segments.split('|')
        for path in all_paths:
            levels = path.split(';')
            levels.reverse()
            current_reference = None
            for level in levels:
                if ':' in level:
                    link_on, properties = level.split(':')
                    properties_list = [p.strip() for p in properties.split(',')]
                else:
                    link_on = level.strip()
                    properties_list = []
                current_reference = QueryReference(link_on=link_on, return_properties=properties_list, return_references=current_reference)
            all_references.append(current_reference)
        
    return top_properties, all_references

# def get_query_reference():

def get_ref_prop(obj, ref_name, spec_prop = None):
    objs = obj.references[ref_name].objects if ref_name in obj.references else []
    if spec_prop != None:
        prop = [i.properties[spec_prop] for i in objs]
    else:
        prop = [i.properties for i in objs]
    return prop

def get_ref_prop2(obj, ref_name, spec_prop = None):
    objs = obj.objects[0].references[ref_name].objects if ref_name in obj.objects[0].references else []
    if spec_prop != None:
        prop = [i.properties[spec_prop] for i in objs]
    else:
        prop = [i.properties for i in objs]
    return prop

def get_ontology_uuid(name):
    ontology_obj = ontology.query.fetch_objects(filters=Filter.by_property("name").equal(name))
    if len(ontology_obj.objects) == 1:
        uuid = str(ontology_obj.objects[0].uuid)
    else:
        raise Exception("ontology matching issue")

    return uuid


class Filter2:
    @staticmethod
    def by_property(prop):
        class PropertyFilter:
            def equal(self, value):
                if prop == 'uuid':
                    return Filter.by_id().equal(value)
                return Filter.by_property(prop).equal(value)
            def less_than(self, value):
                return Filter.by_property(prop).less_than(value)
            def less_or_equal(self, value):
                return Filter.by_property(prop).less_or_equal(value)
            def greater_than(self, value):
                return Filter.by_property(prop).greater_than(value)
            def greater_or_equal(self, value):
                return Filter.by_property(prop).greater_or_equal(value)
            def like(self, pattern):
                return Filter.by_property(prop).like(pattern)
            def any(self, values):
                if prop == 'uuid':
                    return Filter.by_id().contains_any(values)
                return Filter.by_property(prop).contains_any(values)
            def all(self, values):
                return Filter.by_property(prop).contains_all(values)
        return PropertyFilter()

def get_ref_filter(col_name,prop, op, value):
    prop_names, ref_names = WEAVIATE_CONTEXT['var_names'][col_name].values()
    temp_filter = Filter
    refs = prop.split('.')[:-1]
    prop = prop.split('.')[-1]
    check_refs = all([i in ref_names for i in refs])
    check_prop = prop in prop_names+['uuid']
    if check_refs and check_prop:
        for i in refs:
            temp_filter = temp_filter.by_ref(link_on=i)
        if prop == 'uuid':
            temp_filter = temp_filter.by_id()
        else:
            temp_filter = temp_filter.by_property(prop)
        temp_filter = getattr(temp_filter, OPERATORS[op])
        temp_filter = temp_filter(value)
    else:
        raise Exception('columns wrong')
    return temp_filter

def translate_filters(col_name,filters_str,context = {}):
    if filters_str is None:
        return None
    conditions = re.findall(r'([a-zA-Z0-9_.]+)\s*(!=|=|<=|<|>=|>|~|any|all)\s*([^&]*)', filters_str)

    filter_objects = []
    for prop, op_symbol, value_placeholder in conditions:
        if value_placeholder in context:
            value = context[value_placeholder]
        else:
            prop_ref = prop.split('.')[-1]
            if WEAVIATE_CONTEXT['types'][col_name].get(prop_ref, '') == 'NUMBER':
                value = float(value_placeholder)
            else:
                value = value_placeholder

        if '.' in prop:
            ref_filter = get_ref_filter(col_name, prop, op_symbol, value)
            filter_objects.append(ref_filter)
        else:
            property_filter = Filter2.by_property(prop)
            operation = getattr(property_filter, OPERATORS[op_symbol])
            filter_objects.append(operation(value))

    if len(filter_objects) > 1:
        return reduce(lambda x, y: x & y, filter_objects)  # Combining using & operator
    elif filter_objects:
        return filter_objects[0]
    else:
        return None

####################################################################################
####################################################################################
############################ WEAVIATE HEAVY COLLECTION MODIFS 

## Add propety to collection
def add_property_to_collection(collection, prop_name):
    echart_col = client_weaviate.collections.get(ECHART_CLASSNAME)
    echart_col.config.add_property(Property(name="lir",data_type=DataType.TEXT))


####################################################################################
####################################################################################
############################ DICT OPERATIONS #######################################

class _:
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
        if type(fields) == str:
            fields = [fields]
        if isinstance(self.data, dict):
            if isinstance(fields, FunctionType):
                return dict(filter(fields, self.data.items()))
            return {k:v for k,v in self.data.items() if k in fields}
        if isinstance(self.data, list):
            if self.nestedtemp == True:
                return [_(i).nested_to_flat(fields, pattern=pattern, names=names) for i in self.data]
            res = [{k:v for k,v in i.items() if k in fields} for i in self.data]
            if len(fields) == 1:
                res = [i[fields[0]] for i in res]
            return res

    def remove(self, fields):
        if type(fields) == str:
            fields = [fields]
        if isinstance(self.data, dict):
            return {k:v for k,v in self.data.items() if k not in fields}
        if isinstance(self.data, list):
            res = [{k:v for k,v in i.items() if k not in fields} for i in self.data]
            return res

    def apply(self, func, fields):
        # case apply recursively to a nested property
        if type(fields) == str:
            fields = [fields]
        if isinstance(self.data, dict):
            raise Exception('object should be an array to apply function on')
        
        if isinstance(self.data[0], dict): # TODO: to be improved as it just check if first item is a dict
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

    def nested_to_flat(self,keys_to_capture,pattern='references.hasChildren',acc_id=None,delete_pattern=False,names={},temp_keep_format=False):
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
