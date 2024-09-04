import jmespath
import os
import weakref
from types import MethodType
from itertools import zip_longest
from rich.console import Console
from weaviate import WeaviateClient
from weaviate.connect import ConnectionParams
from weaviate.auth import AuthApiKey

from full_metal_weaviate.weaviate_op import metal_query,metal_load
from full_metal_weaviate.container_op import __
from full_metal_weaviate.exception_helper import noOppositeFound

console = Console()

def metal(client_weaviate,opposite_refs=None):
    client_weaviate.get_metal_collection = MethodType(get_metal_collection, client_weaviate)
    if opposite_refs != None:
        register_opposite(client_weaviate,opposite_refs)
    return client_weaviate

def get_metal_collection(self,name,force_reload=False):
    if not force_reload and name in getattr(self,'metal_collection',[]):
        return getattr(self,'metal_collection')[name]
    else:
        col = self.collections.get(name)
        if is_clt_existing(col):
            col.client_parent=weakref.ref(self)
            col.metal_context=set_weaviate_context(self)
            col.q = MethodType(metal_query, col)
            col.metal_query = MethodType(metal_query, col)
            col.get_opposite = MethodType(get_opposite, col)
            col.register_opposite_ref = MethodType(register_opposite_ref, col)
            col.l = MethodType(metal_load, col)
            col.metal_load = MethodType(metal_load, col)
            if not hasattr(self, 'metal_collection'):
                setattr(self, 'metal_collection', {})
            getattr(self, 'metal_collection')[name] = col
            return col
        else:
            console.print("[bold red]Error:[/] [underline]Collection does not exist/typo: {}".format(name))

def get_opposite(self, key=None):
    if key == None:
        return jmespath.search(f'ref_target.{self.name}', self.metal_context) 
    else:
        opposite=jmespath.search(f'ref_target.{self.name}.{key}.opposite', self.metal_context)
        if opposite == None:
            raise Exception(noOppositeFound)
        else:
            return opposite

def set_weaviate_context(client_weaviate):
    all_schema = client_weaviate.collections.list_all(simple=False)
    fields={k: {'properties': [i.name for i in v.properties], 'references': [i.name for i in v.references]} for k,v in all_schema.items()}
    types={k: {i.name:i.data_type.name for i in v.properties} for k,v in all_schema.items()}
    ref_target={k: {i.name:{'target_clt':i.target_collections[0]} for i in v.references} for k,v in all_schema.items()}
    return {'fields': fields, 'types': types, 'ref_target': ref_target}

def register_opposite_ref(self,source_ref,opposite_ref):
    self.metal_context['ref_target'][self.name][source_ref]['opposite'] = opposite_ref
    opposite_clt_name = self.metal_context['ref_target'][self.name][source_ref]['target_clt']
    opposite_clt=self.client_parent().get_metal_collection(opposite_clt_name)
    opposite_clt.metal_context['ref_target'][opposite_clt_name][opposite_ref]['opposite'] = source_ref

def extract_opposite_refs(pattern):
    try:
        parts = [i.split('.') for i in pattern.split('<>')]
        clt_source = parts[0][0]
        rel_source = parts[0][1]
        clt_target = parts[1][0]
        rel_target = parts[1][1]
        return {'clt_source':clt_source,'rel_source':rel_source,'clt_target':clt_target,'rel_target':rel_target}
    except Exception:
        raise Exception('[bold yellow]Error:[/] Register opposite relationship with format: collectionName.reference<>collectionName.reference')
        # print('register opposite relationship with format: collectionName.reference<->collectionName.reference')
        # console.log(log_locals=True)

def is_clt_existing(clt):
    try:
        clt.config.get()
        return True
    except Exception as e:
        return False

def is_metal_client(client):
    return hasattr(client, 'get_metal_collection')

def is_metal_collection(clt):
    return hasattr(clt, 'metal_context')

def register_opposite(client,opposite_refs):
    try:
        buffer_clt = {}
        formatted_opposite=[extract_opposite_refs(i) for i in opposite_refs]
        for opposite_ref in formatted_opposite:
            clt_source_name=opposite_ref['clt_source']
            clt_target_name=opposite_ref['clt_target']
            for clt_name in [clt_source_name,clt_target_name]:
                if clt_name not in buffer_clt:
                    clt=client.get_metal_collection(clt_name)
                    if is_clt_existing(clt): 
                        buffer_clt[clt_name] = clt
                    else:
                        console.print("[bold red]Error:[/] [underline]Most likely collection does not exist or typo: {}".format(clt_source_name))
                        raise

            buffer_clt[clt_source_name].register_opposite_ref(opposite_ref['rel_source'],opposite_ref['rel_target'])
    except Exception as e:
        str_e = str(e)
        if str_e.startswith('[bold yellow]'):
            console.print(str(e))
        else:
            console.print_exception(show_locals=True)
            console.print(str(e))
    finally:
        if len(buffer_clt) > 0:
            for k,v in buffer_clt.items():
                print(k, v.get_opposite())

def get_weaviate_client(weaviate_client_url):
    api_key_weaviate=os.getenv('AUTHENTICATION_APIKEY_ALLOWED_KEYS')

    client = WeaviateClient(
    connection_params=ConnectionParams.from_params(
        # http_host="localhost",
        http_host=weaviate_client_url,
        http_port="8080",
        http_secure=False,
        # grpc_host="localhost",
        grpc_host=weaviate_client_url,
        grpc_port="50051",
        grpc_secure=False,
    ),
    auth_client_secret=AuthApiKey(api_key_weaviate))

    client.connect()
    return client


# def batch_load(self,props=[],refs=[],vectors=[]):
#     assert all(item in self.metal_context['fields'][self.name]['properties'] for item in props.keys()) 
#     uuids = []
#     with self.batch.dynamic() as batch:
#         for prop, ref, vector in zip_longest(props, refs, vectors, fillvalue=None):
#             uuids.append(batch.add_object(properties=prop,references=ref,vector=vector))

#     failed_objs_a = self.batch.failed_objects
#     failed_refs_a = self.batch.failed_references
#     number_errors = batch.number_errors
#     if number_errors>=1:
#         print('number_errors', number_errors)
#         print('messages: ', [i.message for i in failed_objs_a])
#         return {'failed_objs_a': failed_objs_a, 'failed_refs_a': failed_refs_a}
#     uuids = [str(uuid) for uuid in uuids]
#     return uuids
