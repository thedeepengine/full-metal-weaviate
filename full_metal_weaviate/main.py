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
from full_metal_monad import __, safe_jmes_search
from full_metal_weaviate.weaviate_op import metal_query,metal_load, get_filter_compiler, get_return_field_compiler
from full_metal_weaviate.utils import *
from full_metal_weaviate.utils import is_clt_existing

custom_theme = Theme({
    "info": "dim cyan",
    "warning": "magenta",
    "error": "bold red"
})

console = Console(theme=custom_theme)

def get_metal_client(weaviate_client,opposite_refs=None):
    try:
        weaviate_client.get_metal_collection = MethodType(get_metal_collection, weaviate_client)
        weaviate_client.metal=MetalClientContext(weaviate_client)
        if opposite_refs != None:
            register_opposite(weaviate_client,opposite_refs)
        return weaviate_client
    except MetalClientException as e:
        pass

def get_metal_collection(self,name,force_reload=False):
    try:
        if not force_reload and name in getattr(self,'buffer_clt',[]):
            return getattr(self,'buffer_clt')[name]
        else:
            col = self.collections.get(name)
            if is_clt_existing(col):
                col.metal=MetalCollectionContext(self, name)
                col.metal_query=MethodType(metal_query, col)
                col.metal_load=MethodType(metal_load, col)
                col.q=col.metal_query
                col.l=col.metal_load

                if not hasattr(self, 'buffer_clt'):
                    setattr(self, 'buffer_clt', {})
                getattr(self, 'buffer_clt')[name] = col
                return col
            else:
                raise CollectionNotFoundException(name)
    except MetalClientException:
        pass

go_metal = get_metal_collection

def init_metal_batch(self):
    self.current_transaction_object = []
    self.current_transaction_reference = []
    self.run = {}

def append_transaction(self,clt_name,data,trans_type):
    if not hasattr(self, 'current_transaction_object') or not hasattr(self, 'current_transaction_reference'):
        raise Exception('should call init_metal_batch first')
    if trans_type == 'object':
        self.current_transaction_object.append({'clt_name': clt_name, 'uuid': data})
    if trans_type == 'reference':
        self.current_transaction_reference.append({'clt_name': clt_name, 'ref': data})

class MetalClientContext:
    def __init__(self, client_weaviate):
        self.append_transaction = MethodType(append_transaction, client_weaviate)
        self.init_metal_batch = MethodType(init_metal_batch, client_weaviate)
        self.current_transaction_object = []
        self.current_transaction_reference = []
        self.context=set_weaviate_context(client_weaviate)
  
class MetalCollectionContext:
    def __init__(self, client_weaviate, clt_name):
        self.context = client_weaviate.metal.context
        self.name=clt_name
        self.original_client=weakref.ref(client_weaviate)
        self.props=__(self.context).get(f'fields.{clt_name}.properties').__
        self.refs=__(self.context).get(f'fields.{clt_name}.references').__
        self.compiler=get_filter_compiler(self.props+self.refs+['uuid'])
        self.compiler_return_f=get_return_field_compiler()
        self.get_opposite=MethodType(get_opposite, self)
        self.register_opposite=MethodType(register_opposite, self)
        self.get_opp_clt=MethodType(get_opp_clt, self)

def get_opp_clt(self,ref):
    opposite_clt_name=self.context['ref_target'][self.name][ref]['target_clt']
    opposite_clt=self.original_client().get_metal_collection(opposite_clt_name)
    return opposite_clt

def get_opposite(self, key=None):
    try:
        if key == None:
            path=f'ref_target.{self.name}'
        else:
            path=f'ref_target.{self.name}.{key}.opposite'
        opposite=jmespath.search(path, self.context)
        if opposite == None:
            raise NoOppositeException(key)
        return opposite
    except MetalClientException:
        pass

def set_weaviate_context(client_weaviate):
    all_schema = client_weaviate.collections.list_all(simple=False)
    fields={k: {'properties': [i.name for i in v.properties], 
                'references': [i.name for i in v.references],
                'all': [i.name for i in v.properties]+[i.name for i in v.references]} 
                for k,v in all_schema.items()}
    types={k: {i.name:i.data_type.name for i in v.properties} for k,v in all_schema.items()}
    ref_target={k: {i.name:{'target_clt':i.target_collections[0]} for i in v.references} for k,v in all_schema.items()}
    return {'fields': fields, 'types': types, 'ref_target': ref_target}

def register_opposite(self, ref_source, ref_target): 
    ctx=self.context['ref_target']
    source=ctx[self.name][ref_source]
    source['opposite']=ref_target
    target_clt_name=source['target_clt']
    ctx[target_clt_name][ref_target]['opposite']=ref_source
