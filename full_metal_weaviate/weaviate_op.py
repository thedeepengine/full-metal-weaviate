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
from pyparsing import ZeroOrMore, Literal, Combine, Regex, Group, Forward, infixNotation, opAssoc
from rich.table import Table

from full_metal_weaviate.container_op import __
from full_metal_weaviate.utils import StopProcessingException

console = Console()


def metal_load(self,to_load,dry_run=True):
    """
    
    """
    try:
        self.metal.original_client().metal.init_metal_batch()
        ref_format=check_format(self,to_load)
        if ref_format in ['ref_array_with_valid_uuid', 'ref_dict_with_valid_uuid']:
            resolved_load=resolve_ref_to_load(self,to_load,ref_format)
        elif ref_format == 'mix_dict':
            resolved_load=resolve_mix_to_load(self,to_load)

        if dry_run:
            return resolved_load
        else:
            if ref_format in ['ref_array_with_valid_uuid', 'ref_dict_with_valid_uuid']:
                resolved_load=[j for i in resolved_load for j in i]
                group_opp_ref_and_load_ref(self,resolved_load)
            elif ref_format == 'mix_dict':
                uuids=resolve_ref_uuid_and_metal_load(self,resolved_load)
        return uuids
    except Exception as e:
        objs=self.metal.original_client().metal.current_transaction_object
        refs=self.metal.original_client().metal.current_transaction_reference
        console.print_exception(extra_lines=5,show_locals=True)
        if (objs or refs):
            console.print('\n----------[bold blue] Exception Raised Triggered Metal Rollback[/] --------------\n')
            rollback_transactions(objs,refs)

def rollback_transactions(col,objs,refs):
    if objs:
        uuids=__(objs).get(['uuid'])
        col.data.delete_many(where=Filter.by_id().contains_any(uuids))
        console.print(f'[magenta]Deleting {len(uuids)} object(s)')

    if refs:
        grouped_ref = defaultdict(list)
        for item in refs:
            grouped_ref[item.get("clt_name")].append(item.get("ref"))
        for clt_name,ref in grouped_ref:
            clt=col.metal.original_client().get_metal_collection(clt_name)
            delete_many_refs(clt,ref)
        console.print(f'[magenta]Deleting {len(refs)} reference(s)')
        
def delete_many_refs(clt, refs):
    # try:
    for i in refs:
        clt.data.reference_delete(from_uuid=i['from_uuid'],from_property=i['from_property'],to=i['to'])
    # except Exception as e:
    #     print(e)

def metal_query(self,filters_str=None,return_fields=None,context={},limit=100,return_raw=False,query_vector=None,target_vector=None,simplify_unique=True,auto_limit=None):
    try:
        operations=self.metal.compiler.parseString(filters_str,parse_all=True)
        w_filter=get_composed_weaviate_filter(self,operations[0],context)
        return_properties,return_references,return_metadata,include_vector=translate_return_fields(return_fields)

        if query_vector != None:
            res = self.query.near_vector(
                near_vector=query_vector,
                target_vector=target_vector,
                filters=w_filter,
                return_properties=return_properties,
                return_references=return_references,
                return_metadata=return_metadata,
                include_vector=include_vector,
                limit=limit,
                auto_limit=auto_limit)
        else:
            res = self.query.fetch_objects(
                filters=w_filter,
                return_properties=return_properties,
                return_references=return_references,
                include_vector=include_vector,
                return_metadata=return_metadata,
                limit=limit)
            
        if not return_raw:
            res = extract_object(res, include_vector)
            if simplify_unique and len(res) == 1 and hasattr(w_filter, 'model_dump') and w_filter.model_dump()['operator'] == 'Equal':
                res = res[0]
        return res
    except Exception as e:
        console.print_exception(show_locals=True)
        console.print(str(e))
        # raise
        # console.print_exception(show_locals=True)

def check_format(col,to_load):
    if isinstance(to_load, dict):
        to_load = [to_load]
    if isinstance(to_load, list) and isinstance(to_load[0], str):
        to_load = [to_load]

    allowed_fields = col.metal.props+col.metal.refs+['vector']

    def is_ref_array_with_valid_uuid(to_load):
        try:
            if (isinstance(to_load[0], list) 
            & all(len(obj) == 3 for obj in to_load) 
            & all([is_uuid_valid(i[0], True)&is_uuid_valid(i[2], True) for i in to_load])):
                return True
            return False
        except Exception:
            pass
    
    def is_ref_dict_with_valid_uuid(to_load):
        try:
            if (isinstance(to_load[0], dict)
            & all([len(obj) == 3 for obj in to_load])
            & set([j for i in to_load for  j in list(i.keys())]) == set(('from_uuid', 'from_property', 'to'))
            & all([is_uuid_valid(i['from_uuid'], True)&is_uuid_valid(i['to'], True) for i in to_load])):
                return True
        except Exception:
            pass
        return False
    
    def is_mix_dict(col,to_load,allowed_fields):
        try:
            k_clean=[k[2:] if k.startswith('<>') else k for i in to_load for k in i] 
            unique_k=list(set(k_clean))
            is_naming_ok = __(unique_k).apply(lambda x: x in allowed_fields)
            if all(is_naming_ok): 
                return True
            # assert all(is_naming_ok), f'{__(k_clean).get(is_naming_ok)} not in {allowed_keys}'
        except Exception:
            pass
        return False
    
    if is_ref_array_with_valid_uuid(to_load):
        format = 'ref_array_with_valid_uuid'
    elif is_ref_dict_with_valid_uuid(to_load):
        format = 'ref_dict_with_valid_uuid'
    elif is_mix_dict(col,to_load,allowed_fields):
        format = 'mix_dict'
    else:
        raise Exception('Format not recognized')
    return format

def resolve_ref_to_load(col,to_load,ref_format):
    if ref_format == 'ref_dict_with_valid_uuid':
        to_load=[[i['from_uuid'],i['from_property'],i['to']] for i in to_load]
        ref_format = 'ref_array_with_valid_uuid'

    if ref_format == 'ref_array_with_valid_uuid':
        unique_refs=list(set([i[1][2:] for i in to_load if i[1].startswith('<>')]))
        opp_refs={i: col.metal.get_opposite(i) for i in unique_refs}
        opp_clt_name={i: col.metal.context['ref_target'][col.name][i]['target_clt'] for i in unique_refs}
        res=[]
        for i in to_load:
            ref_temp=[]
            ref = i[1]
            if i[1].startswith('<>'):
                ref=i[1][2:]
                ref_temp.append({'ref':{'from_uuid':i[2],'from_property':opp_refs[ref],'to':i[0]},
                                 '$metal_meta$': {'from_clt': opp_clt_name[ref], 'to_clt': col.name}})
            ref_temp.append({'ref':{'from_uuid':i[0],'from_property':ref,'to':i[2]},
                             '$metal_meta$': {'from_clt': col.name, 'to_clt': opp_clt_name[ref]}})
            res.append(ref_temp)
        return res
    return False
    
def resolve_mix_to_load(col,to_load):
    ready_obj = []
    buffered_query = {}
    if isinstance(to_load, dict): to_load=[to_load]
    
    for obj in to_load:
        allowed_keys = col.metal.props+col.metal.refs+['vector']
        k_clean=[i[2:] if i.startswith('<>') else i for i in obj] 
        is_naming_ok = __(k_clean).apply(lambda x: x in allowed_keys)
        assert all(is_naming_ok), f'{__(k_clean).get(is_naming_ok)} not in {allowed_keys}'
        refs=__(k_clean).get(lambda x: x in col.metal.refs)
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
    prop_names, ref_names = col.metal.context['fields'][col.name].values()
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
        is2way='<>'+ref in obj
        ref_value = obj.pop('<>'+ref) if '<>'+ref in obj else obj.get(ref)

        opposite_clt_name=col.metal.context['ref_target'][col.name][ref]['target_clt']
        opposite_clt=col.metal.original_client().get_metal_collection(opposite_clt_name)
        opposite_relation=col.metal.get_opposite(ref)

        if not is_uuid_valid(ref_value):
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
        else:
            obj_copy[ref] = ref_value

        if is2way:
            opp_ref={'ref': {'from_uuid': obj_copy[ref], 'from_property': opposite_relation, 'to': generate_uuid5(str(obj)+str(random.random()))},
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
            clt.metal.original_client().metal.append_transaction(clt.name,temp_uuid,'object')
            uuids.append(temp_uuid)
    show_batch_error(clt,batch)
    return uuids

def batch_load_references(clt, refs):
    if isinstance(refs, dict):
        refs=[refs]
    with clt.batch.dynamic() as batch:
        for ref in refs:
            batch.add_reference(from_uuid=ref['from_uuid'],from_property=ref['from_property'],to=ref['to'])
            clt.metal.original_client().metal.append_transaction(clt.name,ref,'reference')
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
        from_clt=buffer_clt.get(clt_name, col.metal.original_client().get_metal_collection(clt_name))
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

############################################################################
######### compile metal filters to weaviate filters
############################################################################

def custom_one_of(allowed_fields):
    regex_pattern = r'\b(?:' + '|'.join(allowed_fields) + r')\b|\w+'
    return Regex(regex_pattern)

def one_of_checker(x, allowed_fields):
    # try:
        regex_pattern = r'^(?:' + '|'.join(map(re.escape, allowed_fields)) + ')$'
        is_match = bool(re.match(regex_pattern, x[0]))
        if not is_match:
            raise StopProcessingException(f'[bold magenta]Field name not found:[/] {x} \n[bold magenta]Existing fields:[/] {allowed_fields}')
    # except StopProcessingException as e:
    #     raise
    # except Exception as e:
    #     console.print(f'[bold magenta]Field name not found:[/] {x} \n[bold magenta]Existing fields:[/] {allowed_fields}')
    #     console.print_exception(extra_lines=5,show_locals=True)
    #     raise
    
def get_ident(allowed_fields): # sub optimal checking for authorised fields
    base_ident=custom_one_of(allowed_fields)
    base_ident.add_parse_action(lambda x: one_of_checker(x,allowed_fields))
    subfield_ident = Regex("[_A-Za-z][_0-9A-Za-z]{0,230}")
    ident = Combine(base_ident + ZeroOrMore('.' + subfield_ident))
    return ident

def get_expr(allowed_fields):
    try:
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
    except StopProcessingException as e:
        console.print(e)

def get_composed_weaviate_filter(col,operations,context={}):
    if isinstance(operations, list) and isinstance(operations[0], dict):
        return [get_composed_weaviate_filter(col,item) for item in operations]
    elif isinstance(operations, dict):
        for key, value in operations.items():
            op_left = get_composed_weaviate_filter(col,value[0])
            op_right = get_composed_weaviate_filter(col,value[1])
            if key == 'and':
                return op_left&op_right
            elif key == 'or':
                return op_left|op_right
    else:
        return get_atomic_weaviate_filter(col,operations[0],operations[1], operations[2],context)

def get_atomic_weaviate_filter(col, prop, op_symbol, value, context={}):
    if prop in context:
        value = context[prop]
    prop_names, ref_names = col.metal.props, col.metal.refs
    w_filter=Filter
    prop_split=prop.split('.')
    if len(prop_split) == 1:
        if prop_split[0] in prop_names+['uuid']:
            prop_split = prop_split[0]
            if prop_split == 'uuid':
                w_filter=w_filter.by_id()
            elif prop_split in prop_names:
                w_filter=w_filter.by_property(prop_split)
        else:
            if prop_split[0] in ref_names:
                raise Exception('metal references queries should be reference.property=value ex: hasArticle.name=articleName')
            else:
                raise Exception(f'property {prop_split[0]} does not exist, exsiting props: {prop_names}')
    elif len(prop_split) > 1:
        def ref_naming_checker(col,chain_split):
            assert chain_split[0] in col.metal.refs, f'{chain_split[0]} not in available reference fields: {col.metal.refs}'
            target_clt=__(col.metal.context).get(f'ref_target.{col.name}.{chain_split[0]}.target_clt')
            target_fields=__(col.metal.context).get(f'fields.{target_clt}')

            for i,v in enumerate(chain_split[1:]):
                if i == len(chain_split[1:])-1:
                    assert v in target_fields['properties'], f'{v} not in available property fields {target_fields["properties"]}'
                else:
                    target_clt=__(col.metal.context).get(f'ref_target.{col.name}.{v}.target_clt')
                    target_fields=__(col.metal.context).get(f'fields.{target_clt}')
                    fields = target_fields['properties']+target_fields['references']
                    assert v in fields, f'{v} not in available reference fields {fields}'
            

        refs,prop=prop_split[:-1], prop_split[-1]
        for i in refs:
            w_filter = w_filter.by_ref(link_on=i)
        if prop == 'uuid':
            w_filter = w_filter.by_id()
        else:
            w_filter = w_filter.by_property(prop)
        #     else:
        #         raise
        # except Exception as e:
        #     missing_ref = [i for i in refs if i not in ref_names]
        #     missing_prop = [i for i in prop_names+['uuid'] if i not in prop_names+['uuid']]
        #     # Exception(f'check_refs: {check_refs}, check_prop: {check_prop}')
        #     if missing_ref: console.print(f'[bold magenta]Not found ref:[/] {missing_ref}')
        #     if missing_prop: console.print(f'[bold magenta]Not found prop:[/] {missing_prop}')
        #     console.print_exception(show_locals=True)

    w_filter = getattr(w_filter, OPERATORS[op_symbol])
    w_filter = w_filter(value)
    return w_filter

def is_uuid_valid(uuid,bool_ouput=False):
    try:
        if uuid == None: return False
        if bool_ouput:
            return True
        return get_valid_uuid(uuid)
    except ValueError:
        return False
    except TypeError:
        return False

def translate_return_fields(return_fields):
    if return_fields == None:
        return None, None, False
    
    ret_prop,ret_ref,return_metadata,nested_segments=None,None,None,None
    include_vector=False
    top_split = return_fields.split(';;')
    if len(top_split) == 2:
        ret_prop, nested_segments = top_split
        ret_prop = [prop.strip() for prop in ret_prop.split(',')]
        if 'vector' in ret_prop:
            include_vector = True
            ret_prop.remove('vector')
        if (vector_str:=__(ret_prop).startswith('vector:')):
            include_vector=vector_str[0].split(':')[1].split(',')
            ret_prop.remove(vector_str[0])
        if (meta:=__(ret_prop).startswith('metadata:')):
            meta_bool={i:True for i in meta[0].split(':')[1].split(',')}
            return_metadata=MetadataQuery(**meta_bool)
            ret_prop.remove(meta[0])
    else:
        if ':' not in top_split[0]:
            ret_prop = [prop.strip() for prop in top_split[0].split(',')]
        else:
            nested_segments = top_split[0]
    
    if nested_segments != None:
        ret_ref = []
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
            ret_ref.append(current_reference)
        
    return ret_prop,ret_ref,return_metadata,include_vector


def extract_object(res, include_vector=False):
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
        
        if hasattr(item, 'metadata'):
            if hasattr(item.metadata, 'distance') and item.metadata.distance != None:
                result['metadata'] = {'distance': item.metadata.distance}
            if hasattr(item.metadata, 'score') and item.metadata.score != None:
                result['metadata'] = {'score': item.metadata.score}

        if include_vector and hasattr(item, 'vector'):
            result['vector'] = item.vector

        return result

    return [recursive_extract(i) for i in res.objects]

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