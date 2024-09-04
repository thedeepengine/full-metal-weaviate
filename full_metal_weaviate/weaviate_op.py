import re
from functools import reduce,wraps
from types import FunctionType
import pandas as pd
import copy
import random
from collections import defaultdict
from rich.console import Console
from rich.traceback import install
from weaviate.classes.query import MetadataQuery, Filter, QueryReference
from weaviate.exceptions import WeaviateBaseError
from weaviate.util import generate_uuid5,get_valid_uuid
from rich.table import Table


from full_metal_weaviate.container_op import __

console = Console()


def metal_load(self,to_load,dry_run=True):
    try:
        self.client_parent().init_metal_batch()
        resolved_load=resolve_load(self,to_load)
        if dry_run:
            return resolved_load
        else:
            uuids=resolve_ref_uuid_and_metal_load(self,resolved_load)
            # a = {'fff': 2}
            # a['l']
        return uuids
    except Exception as e:
        console.print_exception(extra_lines=5,show_locals=True)
        console.print('\n----------[bold blue] Exception Raised Triggered Metal Rollback[/] --------------\n')
        if (objs:=self.client_parent().current_transaction_object):
            uuids=__(objs).get(['uuid'])
            self.data.delete_many(where=Filter.by_id().contains_any(uuids))
            console.print(f'[magenta]Deleting {len(uuids)} object(s)')

        if (refs:=self.client_parent().current_transaction_reference):
            grouped_ref = defaultdict(list)
            for item in refs:
                grouped_ref[item.get("clt_name")].append(item.get("ref"))
            for clt_name,ref in grouped_ref:
                clt=self.client_parent().get_metal_collection(clt_name)
                delete_many_refs(clt,ref)

            console.print(f'[magenta]Deleting {len(refs)} reference(s)')

def delete_many_refs(clt, refs):
    # try:
    for i in refs:
        clt.data.reference_delete(from_uuid=i['from_uuid'],from_property=i['from_property'],to=i['to'])
    # except Exception as e:
    #     print(e)

def metal_query(self,filters_str=None,return_fields=None,context={},limit=100,include_vector=False,return_raw=False,with_metadata=False,query_vector=None,target_vector=None,simplify_unique=True,auto_limit=None):
    try:
        # assert(isinstance(include_vector, (list, bool)), 'include_vector should be either boolean or list')
        filters=translate_filters(self,filters_str,context)
        return_properties, return_references=translate_return_fields(return_fields)

        if with_metadata:
            return_metadata=MetadataQuery(distance=True)
        else:
            return_metadata = None

        if query_vector != None:
            res = self.query.near_vector(
                near_vector=query_vector,
                target_vector=target_vector,
                filters=filters,
                return_properties=return_properties,
                return_references=return_references,
                return_metadata=return_metadata,
                include_vector=include_vector,
                limit=limit,
                auto_limit=auto_limit)
        else:
            res = self.query.fetch_objects(
                filters=filters,
                return_properties=return_properties,
                return_references=return_references,
                include_vector=include_vector,
                limit=limit)
            
        if not return_raw:
            res = extract_object(res, include_vector, with_metadata)
            if simplify_unique and len(res) == 1 and hasattr(filters, 'model_dump') and filters.model_dump()['operator'] == 'Equal':
                res = res[0]
        return res
    except Exception as e:
        console.print_exception(show_locals=True)
        console.print(str(e))
        # raise
        # console.print_exception(show_locals=True)
        
def get_ref_filter(self,prop, op, value):
    prop_names, ref_names = self.metal_context['fields'][self.name].values()
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
        return temp_filter
    else:
        missing_ref = [i for i in refs if i not in ref_names]
        missing_prop = [i for i in prop_names+['uuid'] if i not in prop_names+['uuid']]
        raise Exception('[bold red]Fields missing or typoed[/]: [underline]'+ ','.join(missing_ref+missing_prop)+'[/]'+
                         '\n[bold red]Existing fields[/]: [underline]'+ ', '.join(prop_names+ref_names))

def translate_filters(self,filters_str,context = {}):
    if filters_str is None:
        return None
    if is_uuid_valid(filters_str):
        filters_str=f'uuid={filters_str}'

    conditions = re.findall(r'([a-zA-Z0-9_.]+)\s*(!=|=|<=|<|>=|>|~|any|all)\s*([^&]*)', filters_str)

    filter_objects = []
    for prop, op_symbol, value_placeholder in conditions:
        if value_placeholder in context:
            value = context[value_placeholder]
        else:
            prop_ref = prop.split('.')[-1]
            if self.metal_context['types'][self.name].get(prop_ref, '') == 'NUMBER':
                value = float(value_placeholder)
            else:
                value = value_placeholder

        if '.' in prop:
            ref_filter = get_ref_filter(self, prop, op_symbol, value)
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
    
def resolve_load(col,to_load):
    ready_obj = []
    buffered_query = {}
    if isinstance(to_load, dict):
        to_load=[to_load]
    for obj in to_load:
        prop_names, ref_names = col.metal_context['fields'][col.name].values()
        keys = list(obj.keys())
        
        is_ref_array=__(keys).apply(lambda x: x in ['from_uuid', 'from_property', 'to'])
        is_ref=all(is_ref_array)
        if is_ref:
            if not is_uuid_valid(obj['from_uuid']):
                col.q(obj['from_uuid'])
        keys_cleaned=[i.replace('<>', '') if i.startswith('<>') else i for i in keys] 
        is_prop_or_ref_array = __(keys_cleaned).apply(lambda x: x in prop_names+ref_names+['vector'])
        is_prop_or_ref = all(is_prop_or_ref_array)
        if is_prop_or_ref:
            refs=__(keys_cleaned).filter(lambda x: x in ref_names)            
            r=resolve_refs(col,obj,refs,buffered_query)
            ready_obj.append(r)
    return ready_obj

def resolve_ref_uuid_and_metal_load(col,ready_objs):
    """
    will load props and refs with 2 ways.
    This should happen in two steps as the reversed way ref needs uuid to be loaded.
    So first create all obj props, once uuids are available, associate with reversed way
    ref and then load pure refs.
    """
    prop_names, ref_names = col.metal_context['fields'][col.name].values()
    to_load_ready=__(ready_objs).apply(lambda x: ({'prop':__(x['obj']).get(prop_names),
                                                'ref': __(x['obj']).get(ref_names),
                                                'vector': __(x['obj']).get('vector')}))

    uuids=batch_load_object(col,to_load_ready)

    assert len(uuids) == len(ready_objs)
    ready_objs_copy = copy.deepcopy(ready_objs)
    for new_uuid,obj in zip(uuids,ready_objs_copy):
        for opp_ref in obj['opp_refs']:
            opp_ref['ref']['to'] = new_uuid
    to_load_opp_refs=__(ready_objs_copy).apply(lambda x:x['opp_refs'])
    to_load_opp_refs=[j for i in to_load_opp_refs for j in i]
    if len(to_load_opp_refs) > 0:
        group_opp_ref_and_load_ref(col,to_load_opp_refs)
    return uuids


def resolve_refs(col,obj,refs,buffered_query={}):
    obj_copy = copy.deepcopy(obj)
    opp_refs=[]
    for ref in refs:
        if ref in obj:
            ref_value=obj[ref]
            is2way=False
        elif '<>'+ref in obj:
            ref_value=obj['<>'+ref]
            is2way=True

        if is_uuid_valid(ref_value):
            return obj
        opposite_clt_name=col.metal_context['ref_target'][col.name][ref]['target_clt']
        opposite_clt = col.client_parent().get_metal_collection(opposite_clt_name)
        
        if ref_value not in buffered_query:
            r=opposite_clt.q(ref_value,simplify_unique=False)
            nb_obj = len(r)
            if nb_obj==1:
                obj_copy[ref] = r[0]['uuid']
                buffered_query[ref_value] = r[0]['uuid']
            elif nb_obj==0:
                console.print("[bold red]no object found:[/] [underline]{}".format(ref_value))
                return
            elif nb_obj > 1:
                console.print("[bold red]more than one matching object:[/] [underline]{}".format(ref_value))
                return
        else:
            obj_copy[ref] = buffered_query[ref_value]

        if is2way:
            opposite_relation=col.get_opposite(ref)
            opp_ref={'ref': {'from_uuid': buffered_query[ref_value], 'from_property': opposite_relation, 'to': generate_uuid5(str(obj)+str(random.random()))},
                     '$metal_meta$': {'from_clt': opposite_clt_name, 'to_clt': col.name}}
            opp_refs.append(opp_ref)
    return {'obj': obj_copy, 'opp_refs': opp_refs}


def group_by_meta(data):
    groups = defaultdict(list)
    for item in data:
        meta_key = item['$metal_meta$']['from_clt']
        groups[meta_key].append(item)
    return list(groups.values())

def check_naming(keys,prop_names,ref_names):
    is_pure_ref='$metal_meta$' in keys or (len(keys) == 3 and all(__(keys).apply(lambda x: x in ['from_uuid', 'from_property', 'to'])))
    is_prop=__(keys).apply(lambda x: x in prop_names+ref_names+['vector'])
    if is_pure_ref:
        return 'ref'
    if is_prop:
        return 'prop'

def batch_load_object(clt, objs):
    if isinstance(objs, dict):
        objs=[objs]
    uuids = []
    with clt.batch.dynamic() as batch:
        for obj in objs:
            if all(obj.get(key) is None for key in ['prop', 'ref','vector']):
                raise Exception('all keys are None for batch load object')
            temp_uuid=batch.add_object(properties=obj.get('prop'),references=obj.get('ref'),vector=obj.get('vector'))
            clt.client_parent().append_transaction(clt.name,temp_uuid,'object')
            uuids.append(temp_uuid)
    show_batch_error(clt,batch)
    return uuids

def batch_load_references(clt, refs):
    if isinstance(refs, dict):
        refs=[refs]
    with clt.batch.dynamic() as batch:
        for ref in refs:
            batch.add_reference(from_uuid=ref['from_uuid'],from_property=ref['from_property'],to=ref['to'])
            clt.client_parent().append_transaction(clt.name,ref,'reference')
    show_batch_error(clt,batch)
    
def show_batch_error(clt,batch):
    if batch.number_errors == 0:
        console.print('[bold green]BATCH OK[/]')
    else:
        console.print(f"""
    [bold magenta]number_errors:[/]{batch.number_errors}
    [bold magenta]failed_objects:[/]{len(clt.batch.failed_objects)}
    [bold magenta]failed_references:[/]{len(clt.batch.failed_references)}
    """)
        
        if len(clt.batch.failed_objects) > 0:
            console.print('Showing first x<10 objects errors:')
            table = Table(show_header=True, header_style="bold magenta")
            table.add_column("property", style="dim", width=12)
            table.add_column("error")

            for error in clt.batch.failed_objects[:10]:
                table.add_row(str(error.object_.properties),error.message)
            console.print(table)

        if len(clt.batch.failed_references) > 0:
            console.print('Showing first x<10 references errors:')
            table = Table(show_header=True, header_style="bold magenta")
            table.add_column("property", style="dim", width=12)
            table.add_column("error")

            for error in clt.batch.failed_references[:10]:
                table.add_row(str(error.object_.properties),error.message)
            console.print(table)

def group_opp_ref_and_load_ref(col,refs):
    buffer_clt={}
    group_by_clt=group_by_meta(refs)
    for group in group_by_clt:
        group_refs=[i['ref'] for i in group]
        clt_name=group[0]['$metal_meta$']['from_clt']
        from_clt=buffer_clt.get(clt_name, col.client_parent().get_metal_collection(clt_name))
        if clt_name not in buffer_clt:
            buffer_clt[clt_name]=from_clt
        batch_load_references(from_clt,group_refs)


# uuids_mapping = pd.DataFrame()
# r=__(r).nested.get(['uuid', 'properties.name'])
# r=pd.json_normalize(r)
# new_uuids=pd.concat([uuids_mapping, r], ignore_index=True, sort=False)
# is_duplicated=new_uuids.duplicated().any()
# if not is_duplicated:
#     uuids_mapping=new_uuids


def is_uuid_valid(uuid):
    try:
        if uuid == None: return False
        return get_valid_uuid(uuid)
    except ValueError:
        return False
    except TypeError:
        return False


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
            def contains_any(self, values):
                if prop == 'uuid':
                    return Filter.by_id().contains_any(values)
                return Filter.by_property(prop).contains_any(values)
            def contains_all(self, values):
                return Filter.by_property(prop).contains_all(values)
        return PropertyFilter()


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


class WeaviateTransaction:
    def __init__(self, client):
        self.client = client
        self.uuids = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.rollback()
        else:
            self.commit()

    def add_data(self, data):
        uuid = self.client.add(data)
        self.uuids.append(uuid)
        return uuid

    def rollback(self):
        for uuid in self.uuids:
            self.client.delete(uuid)
        console.print("Rolled back all changes.")

    def commit(self):
        console.print("All data committed successfully.")


OPERATORS = {
    '!=': 'not_equal',
    '=': 'equal',
    '<': 'less_than',
    '>': 'greater_than',
    '<=': 'less_or_equal',
    '>=': 'greater_or_equal',
    '~': 'like',
    'any': 'contains_any',
    'all': 'contains_all',
}
