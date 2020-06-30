import pandas as pd
import json
from os.path import join, isfile
import os
from matplotlib import pyplot
import numpy as np


def graph_double_oracle_progress(do_results_file, outputdir):

    df = load_do_results_to_dataframe(do_results_file)

    ax = df.plot(x='timePlanner', y='player1EqVal', style='-', label='Player 1 equilibrium value')
    try:
        df2 = df.drop(df[df['player1BestResponseVal'] == -1].index)
    except KeyError:
        print("INCOMPLETE DATA GRAPH not generated!")
        print(outputdir)
        return
    df2 = df2.drop(df2[df2['step'] == 'BRP2'].index)
    ax = df2.plot(x='timePlanner', y='player1BestResponseVal', style='x', label='Player 1 best response value', ax=ax)
    ax.set_xlabel("Time [s]")
    ax.set_ylabel("Utility")
    fig = ax.get_figure()
    fig.savefig(join(outputdir, 'do_progress.pdf'))
    pyplot.close(fig)

    df = df.drop(df.index[0])
    df = df.drop(df.index[-1])

    df = df[['stepDuration']]

    logbins = np.logspace(np.log10(1), np.log10(3600), 20)

    ax = df.hist(column='stepDuration', bins=logbins)

    ax = ax[0][0]
    ax.set_xscale('log')

    ax.set_title("Duration of best response [s]")
    # ax.set_xlabel("Duration of best response computation [s]")

    fig = ax.get_figure()
    fig.savefig(join(outputdir,'do_brhistogram.pdf'))
    pyplot.close(fig)


def load_all_stats(results_dirs):
    all_stats = {}
    for (dirpath, _, _) in os.walk(results_dirs):
        if isfile(join(dirpath, 'AllResults.json')):
            game, params_timestamp = dirpath.split('/')[-2:]
            params = params_timestamp[:-20]
            timestamp = params_timestamp[-19:]
            stats = load_stats(dirpath)
            game_params_dict = all_stats.get(game, {})
            param_timestamp_dict = game_params_dict.get(params, {})
            param_timestamp_dict[timestamp] = stats
            game_params_dict[params] = param_timestamp_dict
            all_stats[game] = game_params_dict

    return all_stats


def generate_experiment_stats(results_dir, output_dir=None):
    if output_dir is None:
        output_dir = results_dir
    stats = load_stats(results_dir)
    with open(join(output_dir, 'stats.json'), 'w') as f:
        json.dump(stats, f)
    graph_double_oracle_progress(
        join(results_dir, 'AllResults.json'),
        results_dir)


def load_stats(results_dir: str):
    """Loads double oracle statistics from a file.
    Args:
        results_dir: A directory where AllResults.json file is present.
    Returns:
        Json objects with the results.
    """
    expl_files = [
        join(results_dir, f)
        for f in next(os.walk(results_dir))[2]
        if f.startswith('Exploitability') and f.endswith('.json')
        and 'Progress' not in f and 'Plans' not in f]
    stats = get_stats(join(results_dir, 'AllResults.json'), expl_files)
    return stats


def load_do_results_to_dataframe(results_filename):
    doresults = json.load(open(results_filename))
    dostats = doresults['statistics']
    df = pd.DataFrame.from_dict(dostats)
    df['iteration_index'] = df['iteration'].copy()
    df = df.set_index('iteration_index')
    df['timePlanner'] = df['stepDuration'].cumsum()

    def non_zero(vector):
        return len([e for e in vector if e > 0.0])

    df['player1EqSupport'] = df['player1Equilibrium'].apply(non_zero)
    df['player2EqSupport'] = df['player2Equilibrium'].apply(non_zero)
    df['player1NumberOfPlans'] = df['player1Equilibrium'].apply(len)
    df['player2NumberOfPlans'] = df['player2Equilibrium'].apply(len)
    return df


def get_do_total_time(results_filename):
    df = load_do_results_to_dataframe(results_filename)
    return df.iloc[-1]['timePlanner']


def get_number_br_iterations(results_filename):
    df = load_do_results_to_dataframe(results_filename)
    return len(df.index) - 2


def get_stats(results_filename, expl_filenames=[]):
    df = load_do_results_to_dataframe(results_filename)
    stats = {
        'total_time': int(df.iloc[-1]['timePlanner']),
        'overall_time': int(df.iloc[-1]['elapsedTime']),
        'player1EqSupport': int(df.iloc[-1]['player1EqSupport']),
        'player2EqSupport': int(df.iloc[-1]['player2EqSupport']),
        'player1NumberOfPlans': int(df.iloc[-1]['player1NumberOfPlans']),
        'player2NumberOfPlans': int(df.iloc[-1]['player2NumberOfPlans']),
        'player1EqVal': float(df.iloc[-1]['player1EqVal']),
        'player2EqVal': float(df.iloc[-1]['player2EqVal']),
        'br_iterations': len(df.index) - 2,
        }
    max_expl = None
    for expl_filename in expl_filenames:
        name = expl_filename.split('/')[-1][14:-5]
        expl_data = json.load(open(expl_filename))
        exploitability = expl_data['p1Exploitability']
        stats['exploitability_' + name] = exploitability
        if max_expl is None or exploitability > max_expl:
            max_expl = exploitability
    stats['exploitability'] = max_expl

    # if expl_filenames:
    #     max_expl = 0
    #     for expl_filename in expl_filenames:
    #         expl_data = json.load(open(expl_filename))
    #         exploitability = expl_data['p1Exploitability']
    #         if exploitability > max_expl:
    #             max_expl = exploitability
    #     stats['exploitability'] = max_expl

    return stats


def generate_double_oracle_stats_graphs_for_all_subdirs(results_dir):
    """Goes through all subdirectories of resuts_dir. If a directory
    contains results of a double oracle experiments, it generates stats and
    graphs.
    Args:
        results_dir: Directory with experiments data.
    """
    for (result_dir, _, _) in os.walk(results_dir):
        if isfile(join(result_dir, 'AllResults.json')):
            generate_experiment_stats(result_dir)