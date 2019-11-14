
import sys

def importModule(module_path):
    try:
        module = __import__(module_path)
    except:
        raise ImportError, 'Could not import %s' % module_path
    if module_path == module.__name__:
        return module
    if module_path in sys.modules:
        return sys.modules[module_path]
    mod_names = module_path[len(module.__name__)+1:].split('.')
    for mod_name in mod_names: 
        module = getattr(module, mod_name, None)
        if module == None: break
    return module

def importObject(object_path):
    dot = object_path.rfind('.')
    object_name = object_path[dot+1:]
    module = importModule(object_path[:dot])
    if module is not None:
        return getattr(module, object_name, None)
    return None
