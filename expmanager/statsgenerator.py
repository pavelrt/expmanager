import os
import math
import re
import statistics
import json
import csv
import hashlib
from enum import Enum
import time
from itertools import chain
import pandas as pd
from os.path import isfile, join
import matplotlib.pyplot as plt

from expmanager import double_oracle_stats
from expmanager import parameters
from expmanager import analytics
import expmanager.errors


class StatsAggr(Enum):
    avg = 'avg'
    last = 'last'

    def __str__(self):
        return str(self.value)


def compare_timestamps(a: str, b: str) -> bool:
    """True if timestamp a is before timestamp b.
    """
    ta = time.strptime(a,'%Y_%m_%d_%H_%M_%S')
    tb = time.strptime(b,'%Y_%m_%d_%H_%M_%S')
    return time.mktime(ta) < time.mktime(tb)


def sum_dicts(a: dict, b: dict) -> dict:
    """For every key in a, computes a[key] + b[key] and save it to a[key].
    Returns a copy of a.
    """
    if not a:
        raise ValueError('First dict is empty => result will be empty')
    s = a.copy()
    for k in s:
        if k in b:
            s[k] = s[k] + b[k]
    return s


def divide_dict(d: dict, n: float) -> dict:
    """Divides every value in d by n.
    Returns a copy of d.
    """
    d = d.copy()
    for k in d:
        d[k] = d[k] / n
    return d


def combine_timestamps_result_stats(all_stats, aggr):
    combined_all_stats = {}
    for game in all_stats:
        param_timestamp_dict = all_stats[game]
        combined_timestamp_stats_dict = {}
        for param in param_timestamp_dict:
            timestamp_stats_dict = param_timestamp_dict[param]
            combined_stats = {}
            if aggr == StatsAggr.last:
                last_timestamp = None
                for timestamp in timestamp_stats_dict:
                    if last_timestamp is None or compare_timestamps(
                            last_timestamp, timestamp):
                        last_timestamp = timestamp
                        combined_stats = timestamp_stats_dict[timestamp]
            elif aggr == StatsAggr.avg:
                for timestamp in timestamp_stats_dict:
                    combined_stats = sum_dicts(
                        timestamp_stats_dict[timestamp], combined_stats)
                combined_stats = divide_dict(
                    combined_stats, len(timestamp_stats_dict))
            else:
                raise ValueError()
            combined_timestamp_stats_dict[param] = combined_stats
        combined_all_stats[game] = combined_timestamp_stats_dict
    return combined_all_stats

def generate_do_vs_dsp_stats(
        results_dir: str,
        games_dir: str, 
        game: str,
        do_filter: dict,
        dsp_filter: dict,
        overwrite: bool = False,
        results_do_dir: str = None,
        output_subdir: str = None) -> str:
    """For every combination of DO results and DSP with the corresponing
    parameters computes the game value when Player 1 plays
    strategy from DO and Player 2 plays the plan from DSP.

    Args:
      results_dir:

    Returns:
      The file name where the statistics have been saved.

    """
    if results_do_dir is None:
        results_do_dir = join(results_dir, game)
    else:
        results_do_dir = join(results_do_dir, game)

    do_dirs = parameters.find_experiments_dirs(
        results_do_dir,
        do_filter)

    dsp_dirs = parameters.find_experiments_dirs(
        join(results_dir, game), 
        dsp_filter)

    if not dsp_dirs:
        return None

    commom_dps_dirs_parms = parameters.find_same_parms(
        [parameters.parse_fras_exp_dirname(dsp_dir[0]) for dsp_dir in dsp_dirs])
    #print(commom_dps_dirs_parms)

    match_obj = re.compile('RAPResultPlans_([0-9]+).json')
    fras_analytics = analytics.FRASAnalytics(results_dir, games_dir)
    output_filenames = []
    for do_dir, _ in do_dirs:
        do_p2_eq_val = json.load(
            open(join(results_do_dir, do_dir, 'AllResults.json'))
        )['player2EquilibriumValue']
        output_filename = ('do_vs_dsp_vals_' + do_dir +
            '___' + parameters.create_dir_name(commom_dps_dirs_parms))
        if (os.path.isfile(join(results_dir, game, output_filename + '.json'))
                and not overwrite):
            continue
        results = []
        for dsp_dir, dsp_parms in dsp_dirs:
            dsp_files = [filename for filename in next(
                os.walk(join(results_dir, game, dsp_dir)))[2]
                if match_obj.match(filename)]
            p2_vals = [
                float(fras_analytics.compute_cross_strats_value(
                    join(games_dir, game, 'game.json'),
                    join(results_do_dir, do_dir, 'AllResults.json'),
                    join(results_dir, game, dsp_dir, dsp_file_name))['p2Value'])
                for dsp_file_name in dsp_files
            ]
            p2_val = statistics.mean(p2_vals)
            if len(p2_vals) < 2:
                p2_val_std = 0
            else:
                p2_val_std = statistics.stdev(p2_vals)
            p2_val_max = max(p2_vals)
            p2_val_min = min(p2_vals)
            results.append((
                int(dsp_parms['parms']['NUM-EST']),
                p2_val,
                p2_val_std,
                p2_val_max,
                p2_val_min)
            )

        all_results = {
            'do_p2_eq_val': do_p2_eq_val,
            'dsp_p2_vals': results}
        if output_subdir is None:
            output_filepath = join(results_dir, game, output_filename + '.json')
        else:
            output_filepath = join(results_dir, game, output_subdir, output_filename + '.json')
        with open(output_filepath, 'w') as f:
            json.dump(all_results, f)
        output_filenames.append(output_filename)
    return output_filename


def generate_cross_do_raps_stats(
        results_dir: str,
        games_dir: str, 
        game: str, 
        rap_timestamp: str = None, 
        do_timestamp: str = None,
        do_parms_filter: dict = None) -> str:
    """Deprecated. For every combination of DO results and DSP with the
    corresponing timestamps and parameters computes the game value when 
    Player 1 plays strategy from DO and Player 2 plays the plan from DSP.

    Args:
      results_dir:

    Returns:
      The file name where the statistics have been saved.

    """
    game_dir = join(results_dir, game)
    do_run_dirs = [
        join(game_dir, do_run_dir)
        for do_run_dir in next(os.walk(game_dir))[1]
        if isfile(join(game_dir, do_run_dir, 'AllResults.json')) 
        and (do_timestamp is None
        or parameters.extract_timestamp(do_run_dir) == do_timestamp)
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

    rap_run_dirs = [
        join(game_dir, rap_run_dir)
        for rap_run_dir in next(os.walk(game_dir))[1] 
        if isfile(join(game_dir, rap_run_dir, 'RAPResultPlans_1.json'))
        and (rap_timestamp is None
        or parameters.extract_timestamp(rap_run_dir) == rap_timestamp)
    ]
    fras_analytics = analytics.FRASAnalytics(results_dir, games_dir)
    match_obj = re.compile('RAPResultPlans_([0-9]+).json')
    stats = []
    for do_run in do_run_dirs:
        do_parm_str = do_run.split('/')[-1]
        val_est = []
        for rap_run in rap_run_dirs:
            rap_parms = parameters.parse_parameters_string(
                rap_run.split('/')[-1])
            rap_files = [filename for filename in next(
                os.walk(rap_run))[2] if match_obj.match(filename)]
            p2_vals = [
                float(fras_analytics.compute_cross_strats_value(
                    join(games_dir, game, 'game.json'),
                    join(do_run, 'AllResults.json'),
                    join(rap_run, rap_file_name))['p2Value'])
                for rap_file_name in rap_files
                ]
            p2_val = statistics.mean(p2_vals)
            if len(p2_vals) < 2:
                p2_val_std = 0
            else:
                p2_val_std = statistics.stdev(p2_vals)
            num_est = int(
                rap_parms['EST']
                if 'EST' in rap_parms
                else rap_parms['NUM-EST'])
            val_est.append((num_est, p2_val, p2_val_std))
        val_est.sort(key=lambda v: v[0])
        stats.append((do_parm_str, val_est))
    if do_timestamp is None:
        do_timestamp = 'all'
    if rap_timestamp is None:
        rap_timestamp = 'all'
    output_filename = 'cross_stats_do_%s_rap_%s' % (do_timestamp, rap_timestamp)
    with open(join(game_dir,output_filename + '.json'),'w') as f:
        json.dump(stats,f)
    return output_filename


def add_do_vs_dsp_graphs(
        ax,
        stats_data: list,
        label: str,
        set_axis: bool = True):
    """Plot Double oracle (Player1) vs deadline sampling planner game value
    graph to ax. On y-axis is Player 2 game value. On x-axis is the
    number of sampling estimates in deadline sampling planner.
    Args:
        ax: matplitlob axes
        stats_data: 
            dictionary {DSP version: list((numberOfEstimates,
            gameValue,gameValueStd))}
    """
    
    stats_data = [(int(d[0]), float(d[1]), float(d[2]), float(d[3]), float(d[4])) for d in stats_data] 
    stats_data.sort(key= lambda v: v[0])


# yerr=[v[2] for v in stats_data], for std

    ax.errorbar(
        [v[0] for v in stats_data],
        [v[1] for v in stats_data],
        yerr=[[v[1] - v[4] for v in stats_data], [v[3] - v[1] for v in stats_data]],
        label=label,
        elinewidth=0.3,
        capsize=5)
    
    if set_axis:    
        ax.set_xscale('log', basex=2)
        #ax.set_title(label)
        ax.set_xlabel('Number of estimates')
        ax.set_ylabel('P2 value')

def add_do_line_to_graph(ax, stats_data: list, do_player2_val: float):
    ax.plot(
        [v[0] for v in stats_data],
        [do_player2_val for v in stats_data],
        label='Double Oracle equilibrium value')


def extract_planner_name(filename: str) -> str:
    """Extract planner name and some paramaters from the given filename.
    It assumes that the filename is generated by
    generate_do_vs_dsp_stats function.
    """
    parms = parameters.parse_parameters_string(os.path.basename(filename))

    # some specific alterations.
    if '52_48' in filename:
        parms['MAXTIME'] = '900'
    if '14_58' in filename:
        parms['MAXTIME'] = '200'
    


    heur_coef = ('- %s heur. value' % parms['OPPHEUR']
                 if 'OPPHEUR' in parms
                 else '- 1.0 heur. value')
    if 'FDLAMARAP' in filename:
        max_time = ('Timeout %ss ' % parms['MAXTIME']
                    if 'MAXTIME' in parms
                    else '')
        return 'Lama ' + max_time + ' ' + heur_coef
    elif 'FDRAP' in filename:
        return 'Astar with potential heuristics ' + heur_coef
    else:
        return None


def create_do_vs_dsp_graph(stats_file_names: list, output_filename: str = None):
    """Creates Player 1 double oracle equilibrium vs deadline sampling planner
    graph. y-axis - value of Player 2. x-axis - number of sampling estimates.
    
    Args:
        stats_file_names: list of files to display in the graph. These files
            should contain data generated by: generate_do_vs_dsp_stats function.
        

    Saves it to a file.
    """
    fig, ax = plt.subplots(figsize=[6.4, 4.8])
    do_est_data = None
    do_p2_eq_val = None
    for file_name in stats_file_names:
        file_label = extract_planner_name(file_name)
        with open(file_name) as f:
            stats = json.load(f)
            if do_est_data is None:
                do_est_data = stats['dsp_p2_vals']
                do_p2_eq_val = stats['do_p2_eq_val']
            elif do_p2_eq_val != stats['do_p2_eq_val']:
                raise expmanager.errors.ExperimentDataError('Inconsistent data')
            add_do_vs_dsp_graphs(
                ax,
                stats['dsp_p2_vals'],
                file_label
                )
    add_do_line_to_graph(ax, do_est_data, do_p2_eq_val)
    ax.legend(loc="lower right")
    ax.set_ylim([-0.6,3.0])
    if output_filename is None:
        prefix = os.path.commonprefix([f for f in stats_file_names])
        suffixes = [filename[len(prefix):] for filename in stats_file_names]
        output_filename = prefix + '++'.join(suffixes)
    fig.savefig(truncate_file_name(output_filename + '.pdf'))
    fig.savefig(truncate_file_name(output_filename + '.svg'))
    plt.close(fig)


def compute_hash(s: str) -> str:
    hash_val = hashlib.md5(s.encode('utf-8')).hexdigest()
    return hash_val[:18]


def truncate_file_name(file_name_path: str, max_length: int = 255) -> str:
    """Replace the part of the file after 256th char by the hash value.
    """
    hash_length = 25  # It is actually less.
    file_name = os.path.basename(file_name_path)
    file_dir = os.path.dirname(file_name_path)
    if len(file_name) <= max_length:
        return file_name_path
    file_name_without_ext, extension = os.path.splitext(file_name)
    truncate_idx = max_length - hash_length - len(extension)
    trunc_file_name = file_name_without_ext[:truncate_idx]
    truncated_part = file_name_without_ext[truncate_idx:]
    hash_truncated_part = compute_hash(truncated_part)
    new_file_name = trunc_file_name + hash_truncated_part + extension
    return os.path.join(file_dir, new_file_name)


def save_do_vs_dsp_to_csv_table(
    stats_file_names: list,
    output_filename: str = None):
    """Save data contained in stats_file_names into an csv file.
    The format is the following:
    num_est, p2_do_val, p2_val_suffix, p2_val_std_suffix,...
    where suffix is the filename in stats_file_names.
    """
    data_for_table = {}
    do_p2_val = None
    for file_name in stats_file_names:
        cols_name_suffix = os.path.basename(file_name)
        with open(file_name) as f:
            stats = json.load(f)
            do_p2_val_s = stats['do_p2_eq_val']
            if do_p2_val is None:
                do_p2_val = do_p2_val_s
            elif do_p2_val != do_p2_val:
                raise expmanager.errors.ExperimentDataError('Inconsistent data')
            data = stats['dsp_p2_vals']
            data_dict = {
                num_est : (p2_val, p2_val_std)
                for num_est, p2_val, p2_val_std, _, _ in data
                }
            data_for_table[cols_name_suffix] = data_dict
    
    if output_filename is None:
        prefix = os.path.commonprefix(stats_file_names)
        suffixes = [filename[len(prefix):] for filename in stats_file_names]
        output_filename = prefix + '++'.join(suffixes)
    with open(truncate_file_name(output_filename + '.csv'), 'w', newline='') \
        as csvfile:
        csvwriter = csv.writer(
            csvfile,
            quoting=csv.QUOTE_MINIMAL)

        header_row = ['num-est','do_val_p2']
        for dict_key in sorted(data_for_table):
            header_row.append('dsp_p2_val_' + dict_key)
            header_row.append('dsp_p2_val_std_' + dict_key)
        csvwriter.writerow(header_row)
        
        row_keys = set()
        for data_dict in data_for_table.values():
            row_keys.update(data_dict.keys())
        row_keys = list(row_keys)
        row_keys.sort()
        for row_key in row_keys:
            row = [row_key, do_p2_val]
            for dict_key in sorted(data_for_table):
                if row_key in data_for_table[dict_key]:
                    dict_data = data_for_table[dict_key][row_key]
                    row.extend(dict_data)
            csvwriter.writerow(row)


def generate_cross_do_raps_graphs(
        results_dir: str,
        game: str,
        stats_filename: str):
    """Generates graphs of statistics of DSP.
    Number of estimete samples versus game value when played against
    double oracle strategy.

    Args:
        stats_filename: Filename of the statistics file without .json suffix.
    """
    game_dir = join(results_dir, game)
    stats = json.load(open(join(game_dir, stats_filename + '.json')))
    stats = [
        s for s in stats 
        if parameters.parse_parameters_string(s[0])['INCLREVERS'] == 'False'
        ]
    nrows = math.ceil(math.sqrt(len(stats)))
    ncols = math.ceil(math.sqrt(len(stats)))
    fig, ax = plt.subplots(nrows=nrows, ncols=ncols, figsize=(20,20))
    data_iter = iter(stats)
    if not isinstance(ax, list):
        ax = [ax]
    try:
        for row in ax:
            if not isinstance(row, list):
                row = [row]
            for col in row:
                graph_label, plot_data = next(data_iter)
                parms = parameters.parse_parameters_string(graph_label)
                label = '%s-%s' % (parms['HEUR'],parms['INCLREVERS'])
                do_p2_eq_val = json.load(
                    open(join(
                        game_dir,
                        graph_label,'AllResults.json'))
                        )['player2EquilibriumValue']
                col.errorbar(
                    [v[0] for v in plot_data],
                    [v[1] for v in plot_data],
                    yerr=[v[2] for v in plot_data])
                col.plot(
                    [v[0] for v in plot_data],
                    [do_p2_eq_val for v in plot_data])
                col.set_xscale('log')
                col.set_title(label)
                col.set_xlabel('Number of estimates')
                col.set_ylabel('P2 value')

                subfig, subax = plt.subplots()
                subax.errorbar(
                    [v[0] for v in plot_data],
                    [v[1] for v in plot_data],
                    yerr=[v[2] for v in plot_data])
                subax.plot(
                    [v[0] for v in plot_data],
                    [do_p2_eq_val for v in plot_data])
                subax.set_xscale('log')
                #subax.set_title(label)
                subax.set_xlabel('Number of estimates')
                subax.set_ylabel('P2 value')
                subfig.savefig(
                    join(game_dir, stats_filename + '_%s.pdf' % label))
                subfig.savefig(
                    join(game_dir, stats_filename + '_%s.svg' % label))
                plt.close(subfig)
    except StopIteration:
        pass
    fig.savefig(join(game_dir, stats_filename + '.pdf'))
    fig.savefig(join(game_dir, stats_filename + '.svg'))
    plt.close(fig)


def generate_aggregate_stats_by_game(results_dirs, output_dir=None, same_exp_aggr=[StatsAggr.last, StatsAggr.avg]):
    if output_dir is None:
        output_dir = results_dirs
    all_stats = double_oracle_stats.load_all_stats(results_dirs)
    for aggr in same_exp_aggr:
        combined_stats = combine_timestamps_result_stats(all_stats, aggr)
        for game, game_stats in combined_stats.items():
            outputfile = join(output_dir,game + '_' + str(aggr) +'_stats.xls')
            xls_table = []
            parms_keys = set()
            stat_keys = set()
            for param, stats in game_stats.items():
                parms_dict = parameters.parse_parameters_string(param)
                parms_keys.update(parms_dict.keys())
                stat_keys.update(stats.keys())
            parms_keys = list(parms_keys)
            parms_keys.sort()
            stat_keys = list(stat_keys)
            stat_keys.sort()

            for param, stats in game_stats.items():
                parms_dict = parameters.parse_parameters_string(param)
                name = param.split('_')[0]
                row = [name]
                for parm_key in parms_keys:
                    row.append(parms_dict.get(parm_key,''))
                for stat_key in stat_keys:
                    row.append(stats.get(stat_key,''))
                xls_table.append(row)
            columns = ['Name', *parms_keys, *stat_keys]
            game_stats_df = pd.DataFrame(xls_table, columns=columns)
            game_stats_df.to_excel(outputfile, sheet_name='sheet1', index=False)


def regenerate_all_stats(results_dir):
    double_oracle_stats.generate_double_oracle_stats_graphs_for_all_subdirs(results_dir)
    generate_aggregate_stats_by_game(results_dir)


def generate_stats(result_dir, configfilename):
    double_oracle_stats.generate_experiment_stats(result_dir)
    all_results_dir = '/'.join(result_dir.split('/')[:-2])
    generate_aggregate_stats_by_game(all_results_dir)


