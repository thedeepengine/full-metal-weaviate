def run_from_ipython():
    try:
        __IPYTHON__
        return True
    except NameError:
        return False

class StopProcessingException(Exception):
    """Custom exception to stop processing without an error trace."""
    pass

