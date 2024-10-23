from rich.console import Console
from rich.theme import Theme

custom_theme = Theme({
    "info": "dim cyan",
    "warning": "magenta",
    "error": "bold red"
})

console = Console(theme=custom_theme)


def run_from_ipython():
    try:
        __IPYTHON__
        return True
    except NameError:
        return False


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
