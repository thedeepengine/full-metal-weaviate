import re
from rich.console import Console
from rich.theme import Theme
from dataclasses import dataclass
from difflib import get_close_matches

custom_theme = Theme({
    "info": "dim cyan",
    "warning": "magenta",
    "error": "bold red"
})

console = Console(theme=custom_theme)

def find_closest_string(input_str, string_list):
    matches = get_close_matches(input_str, string_list, n=1)
    return matches[0] if matches else None

@dataclass
class two_way:
    forward: str
    reverse: str

def run_from_ipython():
    try:
        __IPYTHON__
        return True
    except NameError:
        return False

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

class MetalClientException(Exception):
    """Base class for exceptions in this module."""
    pass

class StopProcessingException(MetalClientException):
    """Custom exception to stop processing without an error trace."""
    pass

class CollectionNotFoundException(MetalClientException):
    def __init__(self, name):
        super().__init__()
        self.name = name
        console.print(f'❗Collection [bold yellow]{name}[/bold yellow] does not exist')

class ParsingException(MetalClientException):
    def __init__(self, message):
        super().__init__()
        self.message = message
        console.print(f'❗{message}')

class FieldNotFoundException(MetalClientException):
    def __init__(self, name, allowed=[], class_name = '', extra = ''):
        super().__init__()
        self.name = name
        closest_str=''
        if len(allowed) > 0:
            closest=find_closest_string(name, allowed)
            if closest != None:
                closest_str=f"\nDid you mean: [bold green]{closest}[/bold green]?"
        if class_name != '':
            class_name = f'in {class_name}'
        console.print(f'''❗Field [bold yellow]{name}[/bold yellow] does not exist {class_name} {closest_str}\n{extra}''')

class MetalWeaviateQueryError(MetalClientException):
    def __init__(self, prop_name, class_name):
        super().__init__()
        self.prop_name = prop_name
        self.class_name = class_name
        console.print(f'❗Field [bold yellow]{prop_name}[/bold yellow] does not exist in class {class_name}')

class MoreThanOneCollectionException(MetalClientException):
    def __init__(self, name):
        super().__init__()
        self.name = name
        console.print(f'❗Ambiguity to resolve more than one collection: {name}')

class NoCollectionException(MetalClientException):
    def __init__(self, name=''):
        super().__init__()
        self.name = name
        console.print(f'❗No matching collection: {name}')

class NoOppositeException(MetalClientException):
    def __init__(self, name=''):
        super().__init__()
        self.name = name
        console.print(f'❗No opposite relationship found for: [bold yellow]{name}[/bold yellow]')

from pyparsing import ParseFatalException
class FieldNotAllowedException(ParseFatalException):
    def __init__(self, name=''):
        super(FieldNotAllowedException).__init__()
        self.name = name
        console.print(f'❗Field not allowed: [bold yellow]{name}[/bold yellow]')

class NoUniqueUUIDException(MetalClientException):
    def __init__(self, search_query='', col = None):
        super().__init__()
        self.search_query = search_query
        console.print(f'❗No unique UUID for: [bold yellow]{search_query}[/bold yellow]')

class NoResultException(MetalClientException):
    def __init__(self, search_query='', col = None):
        super().__init__()
        self.search_query = search_query
        console.print(f'❗Empty result for: [bold yellow]{search_query}[/bold yellow]')

class FormatNotRecognisedException(MetalClientException):
    def __init__(self):
        super().__init__()
        console.print(f'''❗Format not recognized\n
                      [link=http://localhost:3000/docs/load_data]Full Metal Weaviate[/link]
                      ''')

class FMWParseFilterException(MetalClientException):
    def __init__(self, search_query=''):
        super().__init__()
        self.search_query = search_query
        console.print(f'''
Any query should be a composition of the three basic building blocks:\n
[blue underline]attribute:[/blue underline]name=value_name
[blue underline]logical:[/blue underline]name=value_name&age=value_age|desc=desc_value
[blue underline]reference:[/blue underline]hasChildren.name=name_value
[link=http://localhost:3000/docs/data_sample]Full Metal Weaviate[/link]
                      
❗Parsing exception: [bold yellow]{search_query}[/bold yellow]\n
''')

class FMWParseReturnException(MetalClientException):
    def __init__(self, search_query=''):
        super().__init__()
        self.search_query = search_query
        console.print(f'''
❗Parsing exception: [bold yellow]{search_query}[/bold yellow]
return_fields examples:\n
[blue underline]attribute:[/blue underline]name,desc,title
[blue underline]references:[/blue underline]hasProperty.hasChildren:name
[blue underline]deeply nested references w/o previous level attribute:[/blue underline]hasProperty.hasChildren:name,desc
[blue underline]deeply nested references with previous level attribute:[/blue underline]hasProperty:name,title>(hasChildren:name,desc)
[link=http://localhost:3000/docs/data_sample]Full Metal Weaviate[/link] for more examples
''')

