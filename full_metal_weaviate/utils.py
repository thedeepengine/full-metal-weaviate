# same level 
a='hasChildren:name,content|hasOntology:name,map'

# nested returns
a='hasChildren:name,content;hasOntology:name,map'
a='hasChildren.hasOntology:name,map'
a='name;;hasChildren.hasOntology:name,map'
a='name;;hasChildren.hasOntology:name,map;'


translate_return_fields(a)

get_query_reference()




fromUuid = 'ba18e163-35b8-44f2-98fd-514246ae399f'
toUuid = '97fd27b4-abb8-411b-a8e0-a89115ad56b8'


filters_str=f"uuid={fromUuid}&hasChildren.uuid={toUuid}"
return_fields = 'name;;hasChildren'
node_col.q(filters_str, return_fields)


isReferenceExisting(fromUuid, 'hasChildren', toUuid)


filters_str = f"uuid={fromUuid}&hasChildren.uuid={toUuid}"

filters_str = f"uuid={fromUuid}&hasChildren.hasOntology.uuid={toUuid}"
return_fields = 'name;;hasChildren:name;hasOntology.name'
node_col.q(filters_str, return_fields)




# I am writing a function for weaviate that would take as parameter an array of dict and load it to weaviate currently to load an object with some properties and some references you have to 
# first create the reference, then create the object with the associated reference, then if its a 2 way reference, create the new reference, with a different format from to
# first load the object with its references



#     uuid_table=table_col.load([{'name': table_name,
#                                 'sqlCreateTable': create_table_str,
#                                 'hasDatabase': 'name=makent-dec6'}])
