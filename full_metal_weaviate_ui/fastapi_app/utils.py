import os
from weaviate import WeaviateClient
from weaviate.connect import ConnectionParams

def run_from_ipython():
    try:
        __IPYTHON__
        return True
    except NameError:
        return False

def get_work_env():
    if run_from_ipython():
        WORK_ENV = 'dev_host'
    else:
        WORK_ENV = os.getenv('WORK_ENV')
    return WORK_ENV

def get_weaviate_client(weaviate_client_url):

    client = WeaviateClient(
    connection_params=ConnectionParams.from_params(
        http_host=weaviate_client_url,
        http_port="8080",
        http_secure=False,
        grpc_host=weaviate_client_url,
        grpc_port="50051",
        grpc_secure=False,
    ))

    client.connect()
    return client
