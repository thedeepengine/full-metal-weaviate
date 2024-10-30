from rich.console import Console
from rich.theme import Theme
from dataclasses import dataclass

custom_theme = Theme({
    "info": "dim cyan",
    "warning": "magenta",
    "error": "bold red"
})

console = Console(theme=custom_theme)


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

class FieldNotFoundException(MetalClientException):
    def __init__(self, name):
        super().__init__()
        self.name = name
        console.print(f'❗Field [bold yellow]{name}[/bold yellow] does not exist')

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
    def __init__(self, search_query=''):
        super().__init__()
        self.search_query = search_query
        console.print(f'❗No unique UUID for: [bold yellow]{search_query}[/bold yellow]')
