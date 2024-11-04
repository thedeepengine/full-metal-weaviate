from weaviate.classes.config import Property, DataType, ReferenceProperty, Configure, Tokenization
from full_metal_weaviate import get_metal_client, get_weaviate_client
from full_metal_weaviate.weaviate_op import get_filter_compiler, parse_filter, get_return_field_compiler, get_weaviate_return_fields
from full_metal_weaviate.utils import console

TECH_CLT = 'Technology'
CONTRIB_CLT = 'Contributor'
TECH_PROP_CLT = 'TechnologyProperty'
PROP_CATEGORY_CLT = 'PropertyCategory'

########################################
## create collection
########################################

def delete_sample_data(client):
    client.collections.delete(TECH_CLT)
    client.collections.delete(CONTRIB_CLT)
    client.collections.delete(TECH_PROP_CLT)
    client.collections.delete(PROP_CATEGORY_CLT)

def create_sample_collection(client, 
                tech_clt=TECH_CLT, 
                contrib_clt=CONTRIB_CLT, 
                tech_prop_clt=TECH_PROP_CLT,
                prop_category_clt=PROP_CATEGORY_CLT):
    clt_names=[tech_clt, contrib_clt, tech_prop_clt, prop_category_clt]
    existing_clt = [i for i in clt_names 
                     if client.collections.exists(i)]
    if existing_clt:
        console(f'[warning]Warning: Collections {existing_clt} already exist. Delete or change sample data collection name parameters name')
        return
         
    Technology = client.collections.create(
        name=tech_clt,
        properties=[
            Property(name="name", data_type=DataType.TEXT, tokenization= Tokenization.FIELD),
            Property(name="description", data_type=DataType.TEXT),
            Property(name="github", data_type=DataType.TEXT),
            Property(name="nb_stars", data_type=DataType.INT),
            Property(name="release_date", data_type=DataType.DATE),
            Property(name="number_field", data_type=DataType.NUMBER),
        ],
        vectorizer_config=[Configure.NamedVectors.none(name="vect_field")]
    )

    TechnologyProperty = client.collections.create(
        name=tech_prop_clt,
        properties=[
            Property(name="name", data_type=DataType.TEXT),
            Property(name="description", data_type=DataType.TEXT)
        ]
    )

    PropertyCategory = client.collections.create(
        name=prop_category_clt,
        properties=[
            Property(name="name", data_type=DataType.TEXT),
            Property(name="description", data_type=DataType.TEXT),
            Property(name="coolness", data_type=DataType.TEXT_ARRAY)
        ]
    )

    Contributor = client.collections.create(
        name=contrib_clt,
        properties=[
            Property(name="name", data_type=DataType.TEXT)
        ]
    )

    Technology.config.add_reference(ReferenceProperty(name="hasProperty",target_collection=tech_prop_clt))
    TechnologyProperty.config.add_reference(ReferenceProperty(name="propertyOf",target_collection=tech_clt))

    Technology.config.add_reference(ReferenceProperty(name="hasContributor",target_collection=contrib_clt))
    Contributor.config.add_reference(ReferenceProperty(name="contributorOf",target_collection=tech_clt))

    TechnologyProperty.config.add_reference(ReferenceProperty(name="hasCategory",target_collection=prop_category_clt))
    PropertyCategory.config.add_reference(ReferenceProperty(name="categoryOf",target_collection=tech_prop_clt))

    existing_clt = all([client.collections.exists(i) for i in clt_names])
    if existing_clt:
        console.print(f'[info]Sample collections created')
        return True
    else:
        console.print(f'[error]Sample Collections not created')
        return False

def get_clt(weaviate_client):
    client_metal=get_metal_client(weaviate_client)
    technology=client_metal.get_metal_collection(TECH_CLT)
    technology_property=client_metal.get_metal_collection(TECH_PROP_CLT)
    property_category=client_metal.get_metal_collection(PROP_CATEGORY_CLT)
    contributor=client_metal.get_metal_collection(CONTRIB_CLT)
    return technology,technology_property,property_category,contributor

def get_sample_data(client):
    created=create_sample_collection(client)
    if created:
        metal_client=get_metal_client(client)
        load_sample_data(metal_client)
        
##################################################
############ get_collection
##################################################

def load_sample_data(metal_client):
    Technology=metal_client.get_metal_collection(TECH_CLT)
    TechnologyProperty=metal_client.get_metal_collection(TECH_PROP_CLT)
    TechnologyCategory=metal_client.get_metal_collection(PROP_CATEGORY_CLT)
    Contributor=metal_client.get_metal_collection(CONTRIB_CLT)

    Technology.metal.register_opposite('hasProperty', 'propertyOf')
    Technology.metal.register_opposite('hasContributor', 'contributorOf')
    TechnologyCategory.metal.register_opposite('categoryOf', 'hasCategory')
    
    TechnologyProperty.l([{'name': 'PQ', 
                            'description': 'Product Quantization Reduces index footprint'},
                            {'name': 'Flat Index', 
                            'description': 'Lightweight index that is designed for small datasets'},
                            {'name': 'HNSW', 
                            'description': 'Index that scales well to large datasets'},
                            {'name': 'Dynamic Index', 
                            'description': 'Automatically switch from a flat index to an HNSW index'},
                            {'name': 'Annoy', 
                            'description': 'Approximate Nearest Neighbors Oh Yeah, uses random forest, ideal for large datasets'},
                            {'name': 'IVF', 
                            'description': 'partitions data into clusters and creates an inverted index for efficient nearest neighbor search'},
                            {'name': 'LSH', 
                            'description': 'Locality-Sensitive Hashing, hashes high-dimensional data points into buckets'}], False)

    Contributor.l([{'name': 'dirkkul'},{'name': 'tsmith023'}], False)


    TechnologyCategory.l([{'name': 'performance', '<>categoryOf':['name=HNSW']},
                          {'name': 'efficiency', '<>categoryOf':['name=PQ', 'name=Annoy', 'name=IVF']},
                          {'name': 'adaptability', '<>categoryOf': ['name=Dynamic Index']},
                          {'name': 'accuracy', '<>categoryOf': ['name=Flat Index']}], 
                          False)

    Technology.l([{'name':'weaviate',
                    '<>hasProperty': ['name=HNSW', 'name=Dynamic Index', 'name=PQ', 'name=Flat Index'],
                    'hasContributor': ['name=dirkkul', 'name=tsmith023']}, 
                    {'name': 'pinecone',
                    '<>hasProperty': ['name=HNSW', 'name=PQ', 'name=Annoy']}, 
                    {'name':'milvus',
                    '<>hasProperty': ['name=HNSW', 'name=PQ', 'name=Annoy']},
                    {'name':'faiss',
                    '<>hasProperty': ['name=HNSW', 'name=IVF', 'name=PQ', 'name=LSH']}], False)
    console.print(f'[info]Sample data loaded')
