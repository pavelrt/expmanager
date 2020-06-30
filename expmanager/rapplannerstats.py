from os.path import join,isfile
import os
import json
from expmanager import parameters
import statistics
import re
import matplotlib.pyplot as plt
import tabulate


def load_data_from_dsp_run(dsp_dir: str, dsp_timestamp: str = None) -> list:
    """Loads all DSP timing data from all the DSP runs with the given timestamp
    """
    rap_run_dirs = [
        join(dsp_dir, rap_run_dir)
        for rap_run_dir in next(os.walk(dsp_dir))[1]
        if isfile(join(dsp_dir, rap_run_dir, 'RAPResultPlans_1.json')) 
        and (dsp_timestamp is None or parameters.extract_timestamp(rap_run_dir) == dsp_timestamp)]
    match_obj = re.compile('RAPResultPlans_([0-9]+)_timing.json')
    rap_times = []
    for rap_run in rap_run_dirs:
        rap_parms = parameters.parse_parameters_string(rap_run.split('/')[-1])
        rap_files = [filename for filename in next(
            os.walk(rap_run))[2] if match_obj.match(filename)]
        total_times = []
        pddl_times = []
        sampling_times = []
        for rap_file_name in rap_files:
            rap_time_stats = json.load(open(join(rap_run, rap_file_name)))
            total_time = rap_time_stats['p2TotalTime']
            pddl_time = rap_time_stats['p2PDDLPlannerTime']
            sampling_time = total_time - pddl_time
            total_times.append(total_time)
            pddl_times.append(pddl_time)
            sampling_times.append(sampling_time)
        
        total_time_val = round(statistics.mean(total_times),1)
        total_time_std = round(statistics.stdev(total_times),1)
        pddl_time_val = round(statistics.mean(pddl_times),1)
        pddl_time_std = round(statistics.stdev(pddl_times),1)
        sampling_time_val = round(statistics.mean(sampling_times),1)
        sampling_time_std = round(statistics.stdev(sampling_times),1)
        num_est = int(
            rap_parms['EST'] if 'EST' in rap_parms else rap_parms['NUM-EST'])
        rap_times.append((
            num_est,
            total_time_val,
            total_time_std,
            pddl_time_val,
            pddl_time_std,
            sampling_time_val,
            sampling_time_std,
            ))
    rap_times.sort(key=lambda v: v[0])
    return rap_times


def generate_rap_time_do_time(
        results_dir: str,
        game: str,
        rap_timestamp: str = None,
        do_timestamp: str = None,
        do_parms_filter: dict = None):
    game_dir = join(results_dir, game)
    do_run_dirs = [
        join(game_dir, do_run_dir)
        for do_run_dir in next(os.walk(game_dir))[1]
        if isfile(join(game_dir, do_run_dir, 'AllResults.json'))
        and (do_timestamp is None or parameters.extract_timestamp(do_run_dir) == do_timestamp)
    ]
    if do_parms_filter is not None:
        temp_result = []
        for do_run_dir in do_run_dirs:
            parms_str = parameters.extract_params(do_run_dir)
            parms_dict = parameters.parse_parameters_string(parms_str)
            if parameters.is_parm_dict_supersetset_of_another(parms_dict,
                                                              do_parms_filter):
                temp_result.append(do_run_dir)
        do_run_dirs = temp_result

    do_total_time = round(
        json.load(
            open(
                join(
                    do_run_dirs[-1],
                    'AllResults.json')))['statistics'][-1]['elapsedTime'])

    dsp_times = load_data_from_dsp_run(game_dir, rap_timestamp)

    table_data = [['Double oracle', do_total_time, 0.0, 0.0, 0.0, 0.0 , 0.0]]
    table_data.extend(dsp_times)
    headers = [
        'Algorithm',
        'Total time',
        'std',
        'PDDL planner time',
        'std',
        'Sampling time',
        'std',
        ]
    
    latex_table = tabulate.tabulate(table_data,headers,tablefmt='latex_raw')
    with open(
        join(
            game_dir,
            'dsp_time_vs_do_time_dsp_%s_do_%s.tex'
            % (rap_timestamp, do_timestamp)),
            'w') as f:
        f.write(latex_table)
    
    headers2 = [
        'Algorithm',
        'Total time',
        'PDDL planner time',
        'Sampling time',
        ]
    brief_data = []
    for row in table_data:
        brief_data.append([
            row[0],
            row[1],
            row[3],
            row[5],
            # '%s ± %s' % (row[1], row[2]),
            # '%s ± %s' % (row[3], row[4]),
            # '%s ± %s' % (row[5], row[6]),
        ])
    latex_table = tabulate.tabulate(brief_data, headers2, tablefmt='latex_raw')
    with open(
        join(
            game_dir,
            'dsp_time_vs_do_time_brief_dsp_%s_do_%s.tex'
            % (rap_timestamp, do_timestamp)),
            'w') as f:
        f.write(latex_table)
    
