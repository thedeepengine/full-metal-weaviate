import re
from functools import reduce,wraps
from types import FunctionType
import pandas as pd
import copy
import random
import uuid
from collections import defaultdict
from rich.console import Console
from rich.traceback import install
from rich.theme import Theme
from rich.table import Table
from datetime import datetime
from dateutil import parser
from datetime import datetime, timezone

from weaviate.exceptions import WeaviateQueryError
from weaviate.classes.query import MetadataQuery, Filter, QueryReference
from weaviate.exceptions import WeaviateBaseError
from weaviate.util import generate_uuid5,get_valid_uuid
from weaviate.types import UUID

from pyparsing import (ZeroOrMore, FollowedBy, Suppress, delimitedList, nestedExpr,Literal, Combine, Regex, Group, Forward, infixNotation, opAssoc,Optional, oneOf,OneOrMore, ParseException)
from pydantic import validate_call
from typing import Union

from full_metal_monad import __
from full_metal_weaviate.utils import *


# global ___

DEBUG=False

custom_theme = Theme({
    "info": "dim cyan",
    "warning": "magenta",
    "error": "bold red"
})

console = Console(theme=custom_theme)

def metal_load(self,to_load,dry_run=True):
    if len(to_load) == 0: 
        console.print('[info]Empty to_load parameter')
        return None
    if isinstance(to_load, dict):
        to_load = [to_load]
        
    try:
        self.metal.original_client().metal.init_metal_batch()
        to_load=copy.deepcopy(to_load)
        to_load,load_type=check_format(self,to_load)
        if load_type == 'ref':
            resolved,opp_refs=pure_ref_resolver(self,to_load)
        elif load_type == 'mix':
            resolved,opp_refs,temp_uuids=mix_resolver(self,to_load)

        if load_type == 'ref':
            load_pure_ref(self,resolved,opp_refs,dry_run)
            res = None
        elif load_type == 'mix':
            uuids,pure_obj_create,pure_obj_update,pure_ref=load_mix(self,resolved,opp_refs,temp_uuids,dry_run)
            self.metal.run={'date': datetime.now(), 'uuids': uuids,'pure_obj_create': pure_obj_create,'pure_obj_update': pure_obj_update,'pure_ref': pure_ref}
            res = uuids
        if dry_run:
            console.print('\n[magenta blue]Dry Run Mode, set dry_run=False to load[/]\n')
        return res
    except MetalClientException:
        pass
    # except Exception as e:
    #     objs=self.metal.original_client().metal.current_transaction_object
    #     refs=self.metal.original_client().metal.current_transaction_reference
    #     console.print_exception(extra_lines=5,show_locals=True)
    #     if (objs or refs):
    #         console.print('\n----------[bold blue] Exception Raised Triggered Metal Rollback[/] --------------\n')
    #         rollback_transactions(objs,refs)

def search_unique_ref_uuid(col, search_query):
    r=col.q(search_query)
    if r == None or (r != None and len(r) == 0):
        raise NoResultException(search_query, col)
    if len(r) == 1:
        return r[0]['uuid']
    else:
        global ___
        def f():
            return col.q(search_query)
        ___= f
        raise NoUniqueUUIDException(search_query, col) 

def field_meta(col,ref_name):
    is_ref=False;is_2_way=False;cleaned=None
    if ref_name in col.metal.refs:
        is_ref=True;cleaned=ref_name
    if ref_name[2:] in col.metal.refs:
        is_ref=True;cleaned=ref_name[2:];is_2_way=True
    return is_ref,cleaned,is_2_way

def mix_resolver(col,to_load):
    resolved = copy.deepcopy(to_load)
    opp_refs=[]
    temp_uuids=[]
    
    for obj in resolved:
        keys = list(obj)
        temp_uuid=generate_uuid5(str(obj)+str(random.random()))
        temp_uuids.append(temp_uuid)
        for field in keys:
            is_ref,c_field,is_2_way=field_meta(col, field)
            # if not is_ref:
            #     resolved.append
            if is_ref:
                opp_clt=col.metal.get_opp_clt(c_field)
                listified = obj[field] if isinstance(obj[field], list) else [obj[field]]
                temp_resolved = []
                for i in listified:
                    if not is_uuid_valid(i):
                        uuid=search_unique_ref_uuid(opp_clt,i)
                        temp_resolved+=[uuid]
                    else:
                        temp_resolved+=[i]
                obj[field]=temp_resolved
                if is_2_way:
                    opp_field=col.metal.get_opposite(c_field)
                    for i in obj[field]:
                        opp_refs.append({opp_clt.name:[i, opp_field, temp_uuid]})
                    obj[c_field]=obj.pop(field)

    return resolved,opp_refs,temp_uuids

def pure_ref_resolver(col, to_load):
    resolved=copy.deepcopy(to_load)
    opp_refs=[]
    for item in resolved:
        _,c_field,is_2_way=field_meta(col, item[1])
        opp_clt=col.metal.get_opp_clt(c_field)
        if not is_uuid_valid(item[0]):
            uuid=search_unique_ref_uuid(col,item[0])
            item[0]=uuid
        if not is_uuid_valid(item[2]):
            uuid=search_unique_ref_uuid(opp_clt,item[2])
            item[2]=uuid
        if is_2_way:
            item[1]=item[1][2:]               
            opp_field=col.metal.get_opposite(c_field)
            opp_refs.append({opp_clt.name:[item[2], opp_field, item[0]]})
    return resolved,opp_refs

def load_pure_ref(col, resolved, opp_refs, dry_run=True):
    buffer_clt={}
    grouped={col.name: resolved}
    for d in opp_refs:
        for key, value in d.items():
            grouped.setdefault(key, []).append(value)

    for clt_name, refs in grouped.items():
        if clt_name not in buffer_clt:
            clt_obj=col.metal.original_client().get_metal_collection(clt_name)
        buffer_clt[clt_name]=clt_obj
        from_clt=buffer_clt.get(clt_name, clt_obj)
        batch_load_references(from_clt,refs,dry_run)

def load_mix(col,resolved,opp_refs,temp_uuids,dry_run=True):
    to_update = [i for i in resolved if 'uuid' in i]
    to_create = [i for i in resolved if not 'uuid' in i]

    pure_obj_update=__(to_update).apply(
        lambda x: ({'uuid':__(x).get('uuid').__,
                    'prop':__(x).get(col.metal.props).__,
                    'ref': __(x).get(col.metal.refs).__,
                    'vector': __(x).get('vector').__}))
    
    pure_obj_create=__(to_create).apply(
        lambda x: ({'prop':__(x).get(col.metal.props).__,
                    'ref': __(x).get(col.metal.refs).__,
                    'vector': __(x).get('vector').__}))
    
    uuids_created=batch_load_object(col,pure_obj_create,dry_run)
    uuids_updated=batch_update_object(col,pure_obj_update,dry_run)
    uuid_map=dict(zip(temp_uuids, uuids_created+uuids_updated))
    for i in opp_refs:
        k=list(i.keys())[0]
        i[k][2]=uuid_map[i[k][2]]
    pure_ref=load_pure_ref(col,[],opp_refs,dry_run)
    return uuids_created+uuids_updated,pure_obj_create,pure_obj_update,pure_ref

@validate_call
def metal_query(self,
                filters_str:Union[str, UUID]=None,
                return_fields:str=None,
                context:dict={},
                limit:int=100,
                return_raw:bool=False,
                query_vector=None,
                target_vector=None,
                auto_limit=None):
    try:
        try:
            w_filter=get_translate_filter(self,filters_str,context)
            ret_prop,ret_ref,ret_meta,include_vector=get_weaviate_return_fields(self.metal.compiler_return_f,return_fields)
            self.metal.w_filter=w_filter
            self.metal.ret_prop=ret_prop
            self.metal.ret_ref=ret_ref
            self.metal.ret_meta=ret_meta
            self.metal.include_vector=include_vector
            if query_vector != None:
                res = self.query.near_vector(near_vector=query_vector,target_vector=target_vector,filters=w_filter,return_properties=ret_prop,return_references=ret_ref,return_metadata=ret_meta,include_vector=include_vector,limit=limit,auto_limit=auto_limit)
            else:
                res = self.query.fetch_objects(filters=w_filter,return_properties=ret_prop,return_references=ret_ref,include_vector=include_vector,return_metadata=ret_meta,limit=limit)
                
            if not return_raw:
                res = extract_object(res)
            return res
        except (WeaviateQueryError) as e:
            match = re.search(r"no such prop with name '(\w+)' found in class '(\w+)'", str(e))
            
            if match:
                prop_name = match.group(1)
                class_name = match.group(2)
                raise MetalWeaviateQueryError(prop_name, class_name) from None
            else:
                raise WeaviateQueryError(e.message, e.protocol_type)
    except MetalClientException:
        if DEBUG:
            console.print_exception(show_locals=True)
        pass

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

def delete_opposite_refs(clt,target_uuid, rel, dry_run=True):
    to_remove=[]
    opp_rel=clt.metal.get_opposite(rel) 
    obj=clt.q(target_uuid, f'name,{rel}:*')
    opp_obj=clt.q(f'{opp_rel}.uuid={target_uuid}', f'name,{opp_rel}:*')
    
    ref=__(obj).get(0,{}).get('references',{}).get(rel, []).__
    # opp_ref=__(opp_obj).get(0,{}).get('references', {}).get(opp_rel, []).__
    # opp_ref=__(opp_ref).get(lambda x: x['uuid'] == target_uuid,[]).get(0,[]).__

    if len(ref)>0:
        to_remove.append({'from_uuid': target_uuid,'from_property':rel, 'to':ref[0]['uuid']})
    if len(opp_obj)>0:
        to_remove.append({'from_uuid': opp_obj[0]['uuid'],'from_property':opp_rel, 'to':target_uuid})

    if dry_run:
        return to_remove
    else:
        uuid_to_del=__(obj).get(0).get('uuid').__ 
        clt.data.delete_by_id(uuid_to_del)
        for i in to_remove:
            clt.data.reference_delete(**i)

def check_format(col,to_load):
    if isinstance(to_load, dict):
        to_load = [to_load]
    if isinstance(to_load, list) and (isinstance(to_load[0], str) or isinstance(to_load[0], uuid.UUID)):
        to_load = [to_load]

    allowed_fields = col.metal.props+col.metal.refs+['vector', 'uuid']

    def is_ref_array(to_load):
        if isinstance(to_load[0], list):
            if (all(len(obj) == 3 for obj in to_load)
            and all([is_uuid_valid(i[0], True)
                    and is_uuid_valid(i[2], True) for i in to_load])):
                if all([i[1] in col.metal.refs or i[1][2:] in col.metal.refs for i in to_load]):
                    return True
                else:
                    not_found_fields=[i[1] for i in to_load if i[1] not in col.metal.refs]
                    raise FieldNotFoundException(', '.join(not_found_fields), col.metal.refs)
        return False

    def is_ref_dict(to_load):
        if (isinstance(to_load[0], dict)
            and all([len(obj) == 3 for obj in to_load])
            and (set([j for i in to_load for j in list(i.keys())]) == set(('from_uuid', 'from_property', 'to')))
            and all([is_uuid_valid(i['from_uuid'], True)
                    and is_uuid_valid(i['to'], True) for i in to_load])):

                if all([i['from_property'] in col.metal.refs or i['from_property'][2:] in col.metal.refs for i in to_load]):
                    return True
                else:
                    not_found_fields=[i['from_property'] for i in to_load if i['from_property'] not in col.metal.refs]
                    raise FieldNotFoundException(', '.join(not_found_fields), col.metal.refs)
        else:
            return False

    def is_mix_dict(to_load,allowed_fields):
        try:
            if all([isinstance(i, dict) for i in to_load]):
                k_clean=[k[2:] if k.startswith('<>') else k for i in to_load for k in i] 
                unique_k=list(set(k_clean))
                is_naming_ok = __(unique_k).apply(lambda x: x in allowed_fields)
                if all(is_naming_ok): 
                    return True
                else:
                    not_found_fields=[unique_k[i] for i in range(len(unique_k)) if not is_naming_ok[i]]
                    raise FieldNotFoundException(', '.join(not_found_fields), allowed_fields)
        except Exception:
            pass
        return False
    
    if is_ref_array(to_load):
        load_type = 'ref'
    elif is_ref_dict(to_load):
        to_load=[[i['from_uuid'],i['from_property'],i['to']] for i in to_load]
        load_type = 'ref'
    elif is_mix_dict(to_load,allowed_fields):
        load_type = 'mix'
    else:
        raise FormatNotRecognisedException()
    return to_load, load_type

def check_naming(keys,prop_names,ref_names):
    is_pure_ref='$metal_meta$' in keys or (len(keys) == 3 and all(__(keys).apply(lambda x: x in ['from_uuid', 'from_property', 'to'])))
    is_prop=__(keys).apply(lambda x: x in prop_names+ref_names+['vector'])
    if is_pure_ref:
        return 'ref'
    if is_prop:
        return 'prop'

def batch_load_object(clt,objs,dry_run=True):
    uuids = []
    if isinstance(objs, dict):
        objs=[objs]
    for obj in objs:
        if all(obj.get(key) is None for key in ['prop', 'ref','vector']):
            raise Exception('all keys are None for batch load object')
    
    if dry_run:
        console.print('to_create', len(objs))
        return [generate_uuid5(random.random()) for _ in range(len(objs))]
    
    with clt.batch.dynamic() as batch:
        for obj in objs:
            temp_uuid=batch.add_object(properties=obj.get('prop'),references=obj.get('ref'),vector=obj.get('vector'))
            clt.metal.original_client().metal.append_transaction(clt.name,temp_uuid,'object')
            uuids.append(temp_uuid)
    show_batch_error(clt,batch)
    return uuids

def batch_update_object(clt,to_update,dry_run):
    uuids = []
    if len(to_update) > 0:
        for obj in to_update:
            fields = {'uuid': 'uuid', 'prop': 'properties', 'ref': 'references', 'vector': 'vector'}
            params = {value: obj.get(key) 
                    for key, value in fields.items() 
                    if obj.get(key) is not None and len(obj.get(key))>0}

            if params:
                if dry_run == False: 
                    clt.data.update(**params)
                uuids.append(obj.get('uuid'))
        if dry_run:
            console.print('to_update', len(to_update))
            if not hasattr(clt.metal, 'run'):
                clt.metal.run = {}
            clt.metal.run['to_udpate'] = [i.get('uuid') for i in to_update]
    return uuids

def batch_load_references(clt, refs, dry_run=True):
    if len(refs) > 0:
        if isinstance(refs, dict): refs=[refs]
        if dry_run:
            print('batch load ref: ', len(refs))
            # clt.metal.run['']
        else:
            with clt.batch.dynamic() as batch:
                for ref in refs:
                    batch.add_reference(from_uuid=ref[0],from_property=ref[1],to=ref[2])
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
                if hasattr(error, 'object_'):
                    table.add_row(str(error.object_.properties),error.message)
            console.print(table)

############################################################################
######### compile metal filters to weaviate filters
############################################################################

def custom_one_of(allowed_fields):
    regex_pattern = r'\b(?:' + '|'.join(allowed_fields) + r')\b|\w+'
    return Regex(regex_pattern)

def one_of_checker(x, allowed_fields):
        regex_pattern = r'^(?:' + '|'.join(map(re.escape, allowed_fields)) + ')$'
        is_match = bool(re.match(regex_pattern, x[0]))
        # if not is_match:
        #     raise FieldNotFoundException(x[0])

def get_ident(allowed_fields=None): # sub optimal checking for authorised fields
    if allowed_fields:
        base_ident=custom_one_of(allowed_fields)
    else:
        base_ident=Regex("[_A-Za-z][_0-9A-Za-z]{0,230}")
    base_ident#.add_parse_action(lambda x: one_of_checker(x,allowed_fields))
    subfield_ident=Regex("[_A-Za-z][_0-9A-Za-z]{0,230}")
    ident=Combine(base_ident + ZeroOrMore('.' + subfield_ident))
    return ident

def get_filter_compiler(allowed_fields=None):
    try:
        ident = get_ident(allowed_fields)
        operator = Regex("!=|=|<=|<|>=|>|~|any|all").setName("operator")
        value = Regex(r'(?:[^=<>~!&|()\s"]+|"[^"]*")(?:\s+[^=<>~!&|()\s"]+)*')
        condition = Group(ident + operator + value)
        condition.setParseAction(lambda t: {'field': t[0][0], 'operator': t[0][1], 'value': t[0][2]})
        lpar, rpar = map(Literal, "()")
        expr = Forward()
        g = Group(lpar + expr + rpar)
        atom = condition | Group(lpar + expr + rpar).setParseAction(lambda t: t[1])
        expr <<= infixNotation(atom, [
            ('&', 2, opAssoc.LEFT, lambda t: {'and': t[0][::2]}),
            ('|', 2, opAssoc.LEFT, lambda t: {'or': t[0][::2]})
        ])
        return expr
    except StopProcessingException as e:
        console.print(e)

def parse_filter(compiler, filters_str):
    try:
        operations=compiler.parseString(filters_str,parse_all=True)
        return operations
    except ParseException as e:
        raise FMWParseFilterException(filters_str)

def get_translate_filter(col,filters_str=None,context={}):
    try:
        if filters_str == None: return None
        if is_uuid_valid(filters_str):
            operations=[{'field': 'uuid', 'operator': '=', 'value': filters_str}]
        else:
            operations=parse_filter(col.metal.compiler, filters_str)
        w_filter=get_composed_weaviate_filter(col,operations[0],context)
        return w_filter
    except MetalClientException:
        raise MetalClientException

def get_composed_weaviate_filter(clt,operations,context={}):
    if len(operations) == 0:
        return None
    elif isinstance(operations, list) and isinstance(operations[0], dict):
        return [get_composed_weaviate_filter(clt,item,context) for item in operations]
    elif isinstance(operations, dict) and len(operations) == 1:
        for key, value in operations.items():
            op_left = get_composed_weaviate_filter(clt,value[0],context)
            if key == 'and':
                return reduce(lambda acc, x: acc & get_composed_weaviate_filter(clt,x,context), value[1:], op_left)
            elif key == 'or':
                return reduce(lambda acc, x: acc | get_composed_weaviate_filter(clt,x,context), value[1:], op_left)
    else:
        return get_atomic_weaviate_filter(clt,operations['field'],operations['operator'], operations['value'],context)

def get_atomic_weaviate_filter(col, prop, op_symbol, value, context={}):
    if value in context:
        value = context[value]
    prop_names, ref_names = col.metal.props, col.metal.refs
    w_filter=Filter
    prop_split=prop.split('.')
    last_clt=col.name
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
        refs,prop=prop_split[:-1], prop_split[-1]
        for i in refs:
            w_filter = w_filter.by_ref(link_on=i)
            last_clt=col.metal.context['ref_target'][last_clt][i]['target_clt']
        if prop == 'uuid':
            w_filter = w_filter.by_id()
        else:
            last_clt_properties=col.metal.context['fields'][last_clt]['properties']
            print(last_clt_properties)
            if prop not in last_clt_properties:
                raise FMWParseReturnException(prop)
                # raise FieldNotFoundException(prop, prop_names, col.name, extra='An attribute is required after a reference as in hasProperty.name=value')
            w_filter = w_filter.by_property(prop)

    w_filter = getattr(w_filter, OPERATORS[op_symbol])

    prop_type=__(col.metal.context).get(f'types.{last_clt}.{prop}').__
    if prop_type == 'NUMBER':
        try:
            value=float(value)
        except ValueError:
            raise TypeCantBeParsedException(prop, value, 'NUMBER')
    if prop_type == 'DATE':
            try:
                dt = parser.parse(value)
                value=dt.astimezone(timezone.utc).replace(tzinfo=timezone.utc)
            except ValueError:
                raise TypeCantBeParsedException(prop, value, 'DATE')
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

def atomic_return_ref(field_value, include_vector=False):
    try:
        if '.' in field_value:
            levels=field_value.split('.')
            field,values= levels[-1].split(':')
            values=values.split(',')
            if len(values) == 1 and values[0] == '*':
                values = None
            if 'vector' in values:
                include_vector=True
                values=[i for i in values if i != 'vector']
            if field == 'vector':
                include_vector=values
                values=None
            res=QueryReference(link_on=field,return_properties=values,include_vector=include_vector) 
            levels=levels[:-1]
            levels.reverse()
            for v in levels:
                res=QueryReference(link_on=v,return_properties=[],return_references=res)
            return res
        else:
            field,values=field_value.split(':')
            values=values.split(',')
            if len(values) == 1 and values[0] == '*':
                values = None
            if 'vector' in values:
                include_vector=True
                values=[i for i in values if i != 'vector']
            if field == 'vector':
                include_vector=values
                values=None
            return QueryReference(link_on=field, return_properties=values, include_vector=include_vector)
    except ValueError as e:
        print(e)
        return QueryReference(link_on=field_value, return_properties='$$$metal_temp_ref$$$')
    
def extract_object(res):
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

        if hasattr(item, 'vector'):
            result['vector'] = item.vector

        return result

    return [recursive_extract(i) for i in res.objects]


def merge_keys(parsed_data):
    prop=[i for i in parsed_data if 'property' in i]
    merged_prop=[i['property'] for i in prop]
    ref=[i for i in parsed_data if 'reference' in i]
    merged_ref=[i['reference'] for i in ref]
    nested=[i for i in parsed_data if 'nested' in i]
    if len(nested) > 0:
        nested=nested[0]['nested']
    else:
        nested=[]
    return merged_prop, merged_ref, nested

def recurse(parsed_data, res=None):
    include_vector=False
    if len(parsed_data) == 0: return None
    if res is None:
        res = []

    prop,ref,nested=merge_keys(parsed_data)
    if nested:
        prop2,ref2,nested2=merge_keys(nested[1])
        nested_ref = nested[0]['reference']
        if ':' not in nested_ref:
            nested_ref+=':'
        if not nested_ref.endswith(':'):
            nested_ref+=','
        nested_ref+=','.join(prop2)
        vec=[i for i in ref2 if i.startswith('vector')]
        if len(vec) > 0:
            ref2=[i for i in ref2 if not i.startswith('vector')]
            try:
                include_vector=vec[0].split(':')[1].split(',')
            except Exception as e:
                print('vec: ', vec)
                raise e

        nested = atomic_return_ref(nested_ref, include_vector)
        
        def set_last_ref(w_ref, last_ref):
            if w_ref.return_references != None:
                set_last_ref(w_ref.return_references, last_ref)
            else:
                w_ref.return_references=last_ref
            return w_ref
        
        last_ref=set_last_ref(nested, [atomic_return_ref(i) for i in ref2])
        # nested.return_references = [atomic_return_ref(i) for i in ref2]

    for i in ref:
        res.append(atomic_return_ref(i))

    if nested:
        if len(nested2) == 0:
            res.append(nested)
            return res
        else:
            if nested.return_references == None:
                nested.return_references = []
            nested.return_references+=recurse([{'nested': nested2}])
            res.append(nested)
    return res

def parse_return_field(compiler, return_fields):
    try:
        parsed_data=compiler.parseString(return_fields, parseAll=True).asList()
        return parsed_data
    except ParseException as e:
        raise FMWParseReturnException(return_fields)


def get_weaviate_return_fields(compiler_r, return_fields):
    if not return_fields: return None,None,None,False
    include_vector=[]
    parsed_data=parse_return_field(compiler_r, return_fields)
    props=[i['property'] for i in parsed_data if 'property' in i] 
    if 'vector' in props:
        include_vector=True
        props=[i for i in props if i != 'vector']

    ref=[i for i in parsed_data if 'reference' in i]
    refs_classic=[]
    for i in parsed_data:
        if 'reference' in i:
            field,values=i['reference'].split(':')
            if field == 'vector':
                values=values.split(',')
                include_vector+=values
            else:
                refs_classic.append(i)
        else:
            refs_classic.append(i)

    refs_classic=[i for i in refs_classic if 'property' not in i]
    refs=recurse(refs_classic)
    if isinstance(include_vector, list) and len(include_vector) == 0: 
        include_vector=False
    return props, refs, None,include_vector

def get_return_field_compiler():
    basic_prop=Regex("[_A-Za-z][_0-9A-Za-z]{0,230}")
    property = Combine(basic_prop + ~FollowedBy(oneOf("> :")))
    value=Regex("\\s*(\\*|[_A-Za-z][_0-9A-Za-z]{0,230}(\\.[_0-9A-Za-z]{1,230})*|[_A-Za-z][_0-9A-Za-z]{0,230})\\s*")
    values = delimitedList(Combine(value + ~FollowedBy(oneOf(":"))), combine= True)
    property.setParseAction(lambda t: {'property': t[0]})
    nested_expr = Forward()
    reference = Combine(value+':'+values+Suppress(Optional('>'))) | value+Suppress('>')

    def get_ref(t):
        return {'reference': t[0]}
        
    reference.setParseAction(get_ref)
    parenthesized = Group(Suppress('(') + nested_expr + Suppress(')'))

    nested = reference + OneOrMore(Group(nested_expr) | Group(reference) | values | parenthesized)
    nested.setParseAction(lambda t: {'nested':t.asList()})

    expression = delimitedList(nested|reference|property)
    nested_expr <<= expression
    return nested_expr

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

# 702