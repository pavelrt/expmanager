"""Various parameters string utils.

"""

import os
from typing import List
import re

def find_same_parms(parms_dict: List[dict]) -> dict:
    """Returns the parameters that are the same along all parms_dict
    """
    if not parms_dict:
        return None
    same_parms = parms_dict[0].copy()
    if 'parms' in same_parms:
        same_parms['parms'] = parms_dict[0]['parms'].copy()
    
    if 'timestamp' in same_parms:
        for parm_dict in parms_dict:
            if same_parms['timestamp'] != parm_dict['timestamp']:
                del same_parms['timestamp']
                break
    if 'name' in same_parms:
        for parm_dict in parms_dict:
            if same_parms['name'] != parm_dict['name']:
                del same_parms['name']
                break
    if 'parms' in same_parms:
        for parm_dict in parms_dict:
            if 'parms' not in parm_dict:
                del same_parms['parms']
                break
            same_parms_parms = same_parms['parms']
            for key in same_parms_parms:
                if (key not in parm_dict['parms']
                        or same_parms_parms[key] != parm_dict['parms'][key]):
                    del same_parms_parms[key]
                    break
            if not same_parms_parms:
                del same_parms['parms']
                break
    return same_parms


def find_experiments_dirs(
        experiment_results_dir: str,
        filter_dict: dict) -> list:
    """Returns all subdirectories of experiment_results_dir that
    satisfy all condition in filter_dict.

    Returns:
        list of tuples. (subdir, parms_dict) parms_dict is the set of
        parameters that are not in filter_dict.

    """
    result = []
    for subdir in next(os.walk(experiment_results_dir))[1]:
        dir_dict = parse_fras_exp_dirname(subdir)
        if dir_dict is None:
            continue
        if 'name' in filter_dict and filter_dict['name'] != dir_dict['name']:
            continue
        if ('timestamp' in filter_dict 
        and filter_dict['timestamp'] != dir_dict['timestamp']):
            continue
        if ('parms' in filter_dict 
        and not is_parm_dict_supersetset_of_another(
            dir_dict['parms'],
            filter_dict['parms'])):
            continue
        if 'parms' in filter_dict:
            sub_dict_parms = subtract_dicts(dir_dict['parms'], filter_dict['parms'])
        else:
            sub_dict_parms = dir_dict['parms']
        
        sub_dict = {}
        sub_dict['parms'] = sub_dict_parms
        if 'timestamp' not in filter_dict:
            sub_dict['timestamp'] = dir_dict['timestamp']
        if 'name' not in filter_dict:
            sub_dict['name'] = dir_dict['name']
        result.append((subdir, sub_dict))
    return result

def extract_number_of_uav_resources(game_dir: str) -> dict:
    match = re.search(r'_uav([0-9]+)x([0-9]+)', game_dir, re.IGNORECASE)
    if match is not None:
        p1_uav = int(match.group(1))
        p2_uav = int(match.group(2))
    else:
        match = re.search(r'_uavs([0-9]+)x([0-9]+)', game_dir, re.IGNORECASE)
        p1_uav = int(match.group(1))
        p2_uav = int(match.group(2))

    match = re.search(r'_res([0-9]+)', game_dir, re.IGNORECASE)
    if match is not None:
        resources = int(match.group(1))
    else:
        match = re.search(r'_R([0-9]+)', game_dir, re.IGNORECASE)
        resources = int(match.group(1))
    return {'p1_uav': p1_uav, 'p2_uav': p2_uav, 'resources': resources}



def parse_fras_exp_dirname(dirname: str) -> dict:
    """Parses dirname and returns dictionary with timestamp
    parameters
    name
    """
    dirname = dirname.split('/')[-1]
    if len(dirname) < 20 or dirname[-20] != '_':
        return None
    name_parms_part = dirname[:-20]
    timestamp = dirname[-19:]
    parms = parse_parameters_string(dirname)
    name = name_parms_part[:name_parms_part.find('_')]
    return {'name': name, 'parms': parms, 'timestamp': timestamp}


def create_dir_name(parm_dict: dict) -> str:
    """Creates directory name from a dictionary containg: name,
    parameters dictionary and timestamp.
    """
    if 'parms' in parm_dict:
        parms_str = parameters_string(parm_dict['parms'])
    else:
        parms_str = ''

    dir_name = (parm_dict.get('name','') + '_' 
    + parms_str + '_' + parm_dict.get('timestamp'))
    return dir_name

def extract_timestamp(s: str) -> str:
    """Returns the part of the path string that corresponds to timestamp.
    """
    t = s[-19:]
    return t


def extract_params(s: str) -> str:
    """Returns the part of the path string that corresponds to parm string.
    """
    p = s.split('/')[-1][:-20]
    return p


def extract_param_int(params):
    if not params:
        return [dict()]
    (p,values) = params.popitem()
    extracted = extract_parameters(params)
    if not isinstance(values,list):
        values = [values]
    retval = list()
    for e in extracted:
        for v in values:
            augmenteddict = e.copy()
            augmenteddict[p] = v
            retval.append(augmenteddict)
    return retval


def extract_parameters(params):
    """Creates all possible combinations of dictionary paramaters.
    Takes dictionary params. {'a':[1,2,3],'b': ['a','b']}. 
    Returns list of dictionaries: [{'a':[1],'b':'a'},{'a':[1],'b':'b'},...]
    """
    return extract_param_int(params.copy())


def extract_parameters_in_list(params):
    result = []
    for p in params:
        extracted = extract_parameters(p)
        result.extend(extracted)
    return result

    
def parameters_string(params):
    res = ''
    first = True
    for key in sorted(params.keys()):
        if not first:
            res += '_'
        else:
            first = False
        res += '%s-%s' % (key, params[key])
    return res


def parse_parameters_string(s: str) -> dict:
    """Creates parameters dictionary from a string.
    """
    res = {}
    for parm in s.split('_'):
        split = parm.split('-')
        if len(split) >= 2:
            key_value = '-'.join(split[:-1]), split[-1]
            res[key_value[0]] = key_value[1]
    return res


def subtract_dicts(d1: dict, d2: dict) -> dict:
    """Removes all keys in d1 that are present in d2.
    """
    d1 = d1.copy()
    for key2 in d2:
        if key2 in d1:
            del d1[key2]
    return d1

def is_parm_dict_supersetset_of_another(d1: dict, d2: dict) -> bool:
    """Returns True if d1 contains all keys as d2 and
    values of these keys are equal.
    """
    for key1 in d2:
        if key1 not in d1 and d2[key1]:
            return False
        if key1 in d1 and d1[key1] != d2[key1]:
            return False
    return True


if __name__ == "__main__":
    # test code
    params = {"1":["a","b"],"2":["c","d"]}
    print(params)
    extracted = extract_parameters(params)
    print(extracted)
    extracted = extract_parameters_in_list([params,params])
    print(extracted)
