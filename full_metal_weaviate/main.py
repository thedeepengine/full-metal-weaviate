"""
This is an example code for Sphinx tutorial!

* Here is a list!
* **bold** text :)
* *italic* text!
* Code style: ``cool_variable=42``

.. note::
    A cool note block!

.. warning::
    Also a cool warning block!

"""

import jmespath
import os
import weakref
from types import MethodType
from itertools import zip_longest
from rich.console import Console
from rich.theme import Theme
from weaviate import WeaviateClient
from weaviate.connect import ConnectionParams
from weaviate.auth import AuthApiKey
from full_metal_weaviate.utils import run_from_ipython

if not run_from_ipython():
    from full_metal_weaviate.weaviate_op import metal_query,metal_load, get_compiler, get_return_field_compiler
    from full_metal_monad import __, safe_jmes_search
    from full_metal_weaviate.utils import StopProcessingException

custom_theme = Theme({
    "info": "dim cyan",
    "warning": "magenta",
    "error": "bold red"
})

console = Console(theme=custom_theme)

def get_metal_client(client_weaviate,opposite_refs=None):
    """
    """
    client_weaviate.get_metal_collection = MethodType(get_metal_collection, client_weaviate)
    client_weaviate.metal=MetalClientContext(client_weaviate)
    if opposite_refs != None:
        register_opposite(client_weaviate,opposite_refs)
    return client_weaviate

def get_metal_collection(self,name,force_reload=False):
    try:
        if not force_reload and name in getattr(self,'buffer_clt',[]):
            return getattr(self,'buffer_clt')[name]
        else:
            col = self.collections.get(name)
            if is_clt_existing(col):
                col.metal=MetalCollectionContext(self, name)
                col.metal_query=MethodType(metal_query, col)
                col.q=col.metal_query
                col.metal_load=MethodType(metal_load, col)
                col.l=col.metal_load

                if not hasattr(self, 'buffer_clt'):
                    setattr(self, 'buffer_clt', {})
                getattr(self, 'buffer_clt')[name] = col
                return col
            else:
                raise StopProcessingException(f'[bold red]Error:[/] [underline] Collection {name} does not exist')
    except StopProcessingException as e:
        console.print(str(e))

go_metal = get_metal_collection

def init_metal_batch(self):
    self.current_transaction_object = []
    self.current_transaction_reference = []

def append_transaction(self,clt_name,data,trans_type):
    if not hasattr(self, 'current_transaction_object') or not hasattr(self, 'current_transaction_reference'):
        raise Exception('should call init_metal_batch first')
    if trans_type == 'object':
        self.current_transaction_object.append({'clt_name': clt_name, 'uuid': data})
    if trans_type == 'reference':
        self.current_transaction_reference.append({'clt_name': clt_name, 'ref': data})

def get_opposite(self, key=None):
    try:
        if key == None:
            return jmespath.search(f'ref_target.{self.name}', self.context) 
        else:
            opposite=jmespath.search(f'ref_target.{self.name}.{key}.opposite', self.context)
            if opposite == None:
                raise
            else:
                return opposite
    except Exception as e:
        raise StopProcessingException(f'[error] No opposite found for {key}')

class MetalClientContext:
    def __init__(self, client_weaviate):
        self.append_transaction = MethodType(append_transaction, client_weaviate)
        self.init_metal_batch = MethodType(init_metal_batch, client_weaviate)
        self.current_transaction_object = []
        self.current_transaction_reference = []
  
class MetalCollectionContext:
    def __init__(self, client_weaviate, clt_name):
        self.name=clt_name
        self.original_client=weakref.ref(client_weaviate)
        self.context=set_weaviate_context(client_weaviate)
        self.props=__(self.context).get(f'fields.{clt_name}.properties').__
        self.refs=__(self.context).get(f'fields.{clt_name}.references').__
        self.compiler=get_compiler(self.props+self.refs+['uuid'])
        self.compiler_return_f=get_return_field_compiler()
        self.get_opposite=MethodType(get_opposite, self)
        self.register_opposite_ref=MethodType(register_opposite_ref, self)
        self.get_opp_clt=MethodType(get_opp_clt, self)

def get_opp_clt(self,ref):
    opposite_clt_name=self.context['ref_target'][self.name][ref]['target_clt']
    opposite_clt=self.original_client().get_metal_collection(opposite_clt_name)
    return opposite_clt

def set_weaviate_context(client_weaviate):
    all_schema = client_weaviate.collections.list_all(simple=False)
    fields={k: {'properties': [i.name for i in v.properties], 'references': [i.name for i in v.references]} for k,v in all_schema.items()}
    types={k: {i.name:i.data_type.name for i in v.properties} for k,v in all_schema.items()}
    ref_target={k: {i.name:{'target_clt':i.target_collections[0]} for i in v.references} for k,v in all_schema.items()}
    return {'fields': fields, 'types': types, 'ref_target': ref_target}

def register_opposite_ref(self,source_ref,opposite_ref):
    self.context['ref_target'][self.name][source_ref]['opposite'] = opposite_ref
    opposite_clt_name = self.context['ref_target'][self.name][source_ref]['target_clt']
    opposite_clt=self.original_client().get_metal_collection(opposite_clt_name)
    opposite_clt.metal.context['ref_target'][opposite_clt_name][opposite_ref]['opposite'] = source_ref

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
    return hasattr(clt, 'metal')

def register_opposite(client,opposite_refs):
    buffer_clt = {}
    formatted_opposite=[extract_opposite_refs(i) for i in opposite_refs]
    for ref in formatted_opposite:
        for clt_name in [ref['clt_source'],ref['clt_target']]:
            if clt_name not in buffer_clt:
                buffer_clt[clt_name]=client.get_metal_collection(clt_name) 
        buffer_clt[ref['clt_source']].metal.register_opposite_ref(ref['rel_source'],ref['rel_target'])

def get_weaviate_client(weaviate_client_url):
    global weaviate_client, client
    api_key_weaviate = os.getenv('WEAVIATE_API_KEY')

    weaviate_client_check='weaviate_client' in globals() and isinstance(weaviate_client, WeaviateClient)
    client_check='client' in globals() and isinstance(client, WeaviateClient)
    if any([weaviate_client_check,client_check]):
        try:
            if weaviate_client_check: weaviate_client.close()
            if client_check: client.close()
            console.print("[info]Existing client closed, a new connection will be established.")
        except Exception as e:
            console.print(f"[error]Error closing the client: {str(e)}")

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
