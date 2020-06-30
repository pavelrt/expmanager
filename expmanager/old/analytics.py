import os
import json
from os.path import isdir, join, dirname
import pandas as pd
import numpy as np
from matplotlib import pyplot
import math




def analyze_expl_vs_time_expansion(d):

    records = []
    for (d,_,files) in os.walk(d):
        if ('params.json' in files and 'AllResults.json' in files and 'ExploitabilityLAMA.json' in files and
                'ExploitabilityAStar.json' in files):
            params=json.load(open('params.json'))
            results=json.load(open(join('output','AllResults.json')))
            explLama=json.load(open(join('output','ExploitabilityLAMA.json')))
            explAstar=json.load(open(join('output','ExploitabilityAstar.json')))

            stat = results['statistics']
            df = pd.DataFrame.from_dict(stat)
            df = df.set_index('iteration')
            df['timePlanner'] = df.stepDuration.cumsum()

            total_time = df.iloc[-1]['timePlanner']
            iterations = len(df)
            expl = explLama['p1Exploitability']
            explAstar = explAstar['p1Exploitability']
            eqValP1 = stat[-1]['player1EqVal']

            records.append((params['timeExpansion'], total_time, iterations, expl, explAstar, eqValP1))

    columns = ['timeExpansion', 'time', 'iterations', 'exploitability', 'exploitabilityAStar', 'eqValP1']
    df = pd.DataFrame(records, columns=columns)
    df["avgBRDuration"] = df["time"] / df["iterations"]
    df = df.sort_values('time')

    print(df)


def find_max_duration_br(resultsdir, df):
    overallmax = 0.0
    for d in df['results_dir']:
        dfdo = load_do_dataframe(d, resultsdir)
        m = dfdo['stepDuration'].max()
        overallmax = max(overallmax, m)
    return overallmax


def create_iters_brlength_agg_graph(resultsdir, df, outputdirectory, filenameprefix=None, yrange=None, ax=None):
    df = df.copy()
    ax_ac = ax
    ax_acp1 = None
    ax_acp2 = None
    scalestr = '' if yrange is None else '_scaled_'
    for i, txt in enumerate(df.timeExpansion):
        dfdo = load_do_dataframe(df['results_dir'].iat[i], resultsdir)
        ax_ac=graph_double_oracle_iteration_bestresplength(dfdo,txt,yrange=yrange,ax=ax_ac)
        ax_acp1 = graph_double_oracle_iteration_bestresplength(dfdo, txt,yrange=yrange, player=1, ax=ax_acp1)
        ax_acp2 = graph_double_oracle_iteration_bestresplength(dfdo, txt,yrange=yrange, player=2, ax=ax_acp2)
    if filenameprefix:
        fig = ax_ac.get_figure()
        fig.savefig(join(outputdirectory, filenameprefix + scalestr + "IterationsVsBRLength.pdf"))
        pyplot.close(fig)
        fig = ax_acp1.get_figure()
        fig.savefig(join(outputdirectory, filenameprefix + scalestr + "IterationsVsBRLengthP1.pdf"))
        pyplot.close(fig)
        fig = ax_acp2.get_figure()
        fig.savefig(join(outputdirectory, filenameprefix + scalestr + "IterationsVsBRLengthP2.pdf"))
        pyplot.close(fig)


    return ax_ac


def graph_double_oracle_iteration_bestresplength(df, label, yrange=None, player=None, outputdir=None, ax=None):

    df = df.copy()

    playerstr = ''

    if not player is None:
        playerstr = 'P1' if player == 1 else 'P2'
        df = df[df.apply(lambda r: playerstr in r['step'], axis=1)]

    if df.empty:
        return ax

    df['movingAvg'] = df['stepDuration'].rolling(1).mean()

    ax = df.plot(x='iteration', y='movingAvg', style='-', ylim=(0,yrange) if yrange is not None else None , label=label, ax=ax)

    if outputdir:
        fig = ax.get_figure()
        scalestr = '' if yrange is None else '_scaled_'
        fig.savefig(join(outputdir, 'iterations_vs_bestresplength' + scalestr + playerstr+'.pdf'))
        pyplot.close(fig)
    return ax

def create_exp_vs_iterations_graph(df, outputdirectory, filenameprefix=None, ax=None):
    df = df.copy()
    df = df.sort_values('iterations')
    #ax = df.plot(x='iterations', y='lamaExploitability', color="tab:blue", style="-x", label='Planner', ax=ax)
    ax = df.plot.scatter(x='iterations', y='lamaExploitability', c="tab:blue", ax=ax)
    #ax = df.plot(x='iterations', y='astarexploitability', style="-x", color="tab:orange", label='A*', ax=ax)
    ax = df.plot.scatter(x='iterations', y='astarexploitability', c="tab:orange", ax=ax)
    for i, txt in enumerate(df.timeExpansion):
        ax.annotate(txt, (df['iterations'].iat[i], df.lamaExploitability.iat[i]))
        ax.annotate(txt, (df['iterations'].iat[i], df.astarexploitability.iat[i]))
    if filenameprefix:
        fig = ax.get_figure()
        fig.savefig(join(outputdirectory, filenameprefix + "ExpVsIterations.pdf"))
        pyplot.close(fig)
    return ax


def create_exp_vs_time2_graph(dflist, labels, ax=None):
    if not isinstance(dflist,list):
        dflist = [dflist]
        labels = [labels]

    for df, label in zip(dflist, labels):
        df = df.copy()
        df = df.sort_values('total_time')
        ax = df.plot(x='total_time', y='astarexploitability',label=label, ax=ax)
        for i, txt in enumerate(df.timeExpansion):
            ax.annotate(txt, (df['total_time'].iat[i], df['astarexploitability'].iat[i]))
            ax.annotate(txt, (df['total_time'].iat[i], df['astarexploitability'].iat[i]))
    return ax

def create_exp_vs_time_graph(resultsdir, df, outputdirectory, filenameprefix=None, ax=None, withlama=False):
    df = df.copy()
    df = df.sort_values('timeExpansion')

    ax = df.plot.bar(x='timeExpansion',
                     y=['lamaExploitability','astarexploitability'] if withlama else 'astarexploitability')
    ax.set_xlabel('Granularity')
    ax.set_ylabel('Error')
    ax.get_legend().remove()


    axtime = create_exp_vs_time2_graph(df,'Error')

    #ax = df.plot(x='total_time', y='lamaExploitability', color="tab:blue", style="-x", label='Planner', ax=ax)
    #ax = df.plot.scatter(x='total_time', y='lamaExploitability', c="tab:blue", ax=ax)
    #ax = df.plot(x='total_time', y='astarexploitability', style="-x", color="tab:orange", label='A*', ax=ax)
    #ax = df.plot.scatter(x='total_time', y='astarexploitability', c="tab:orange", ax=ax)
    #for i, txt in enumerate(df.timeExpansion):
    #    ax.annotate(txt, (df.total_time.iat[i], df.lamaExploitability.iat[i]))
    #    ax.annotate(txt, (df.total_time.iat[i], df.astarexploitability.iat[i]))
    if filenameprefix:
        fig = ax.get_figure()
        fig.savefig(join(outputdirectory, filenameprefix + "ExpVsGran.pdf"))
        pyplot.close(fig)
        fig = axtime.get_figure()
        fig.savefig(join(outputdirectory, filenameprefix + "ExpVsTime.pdf"))


        def calculate_std(directory):
            df = load_do_dataframe(directory, resultsdir)
            s=df['stepDuration'][1:-1]
            return round(s.std())

        def calulate_mean(directory):
            df = load_do_dataframe(directory, resultsdir)
            s = df['stepDuration'][1:-1]
            return round(s.mean())



        df['br_std'] = df['results_dir'].apply(calculate_std)
        df['br_mean'] = df['results_dir'].apply(calulate_mean)
        df['iterations'] = df['iterations'] - 2 # subtract init and conv iterations



        dftosave = df[['timeExpansion','total_time','iterations','lamaExploitability','astarexploitability','br_std',
                       'br_mean']]

        #dftosave = dftosave.applymap(lambda x: round(x) if not math.isnan(x) else x)

        dftosave['total_time'] = dftosave['total_time'].apply(lambda x: round(x) if not math.isnan(x) else x)




        with open(join(outputdirectory, filenameprefix + "ExpVsTimeLatexTable.txt"),'w') as f:
            dftosave.to_latex(f,['timeExpansion','total_time','iterations','astarexploitability','br_std', 'br_mean'],
                              index=False)


    return ax



def generate_agregate_graphs(df, resultsdir,yrange=None):
    scenarios = set(df.index.get_level_values('scenario'))
    configurations = set(df['doconfiguration'])
    for scenario in scenarios:
        for configuration in configurations:
            subdf = df.loc[scenario]
            subdf=subdf[subdf['doconfiguration'] == configuration] #[(df['scenario'] == scenario) & (df['doconfiguration'] == configuration)]
            timestamps = set(subdf.index.get_level_values('timestamp'))
            print(timestamps)
            acc_ax_its = None
            acc_ax_time = None
            outputdir = join(resultsdir, scenario, 'graphs')
            os.makedirs(outputdir, exist_ok=True)

            for timestamp in sorted(timestamps):
                subsubdf=subdf.xs(timestamp, level='timestamp') #[subdf['timestamp'] == timestamp]
                create_exp_vs_iterations_graph(subsubdf,outputdir,configuration + "_" + timestamp + "_")
                create_exp_vs_time_graph(subsubdf, outputdir, configuration + "_" + timestamp + "_")
                acc_ax_its = create_exp_vs_iterations_graph(subsubdf, outputdir,ax=acc_ax_its)
                acc_ax_time = create_exp_vs_time_graph(subsubdf, outputdir,ax=acc_ax_time)
                create_iters_brlength_agg_graph(subsubdf, outputdir,configuration + "_" + timestamp + "_",yrange=yrange)

            fig_its = acc_ax_its.get_figure()
            fig_its.savefig(join(outputdir , configuration + '_ExpVsIts.pdf'))
            pyplot.close(fig_its)
            fig_time = acc_ax_time.get_figure()
            fig_time.savefig(join(outputdir, configuration + '_ExpVsTime.pdf'))
            pyplot.close(fig_time)


def generate_agregate_graphs2(df, resultsdir):
    scenarios = set(df['scenario'])
    configurations = set(df['doconfiguration'])
    for scenario in scenarios:
        for configuration in configurations:
            subdf = df[(df['scenario'] == scenario) & (df['doconfiguration'] == configuration)]

            timestampboth = '2019_06_26_14_07_51'
            timestampsingle = '2019_06_21_15_40_52'

            outputdir = join(resultsdir, scenario, 'graphs')
            os.makedirs(outputdir, exist_ok=True)

            subdfboth=subdf[subdf['timestamp'] == timestampboth]
            subdfsingle = subdf[subdf['timestamp'] == timestampsingle]

            if subdfboth.empty or subdfsingle.empty:
                continue

            axtime = create_exp_vs_time2_graph([subdfboth,subdfsingle], ['Both sides DO','Single side DO'])
            axtime.set_xlabel('Time [s]')
            axtime.set_ylabel('Error')

            fig = axtime.get_figure()
            fig.savefig(join(outputdir, configuration + "_" + timestampboth + "_" + timestampsingle + "_" + "ExpVsTime.pdf"))
            pyplot.close(fig)





def generate_experiment_graphs(resultsdir, df, yrange=None):
    for experiment_dir in df['results_dir']:
        exp_df = load_do_dataframe(experiment_dir, resultsdir)
        #graph_double_oracle_iteration_bestresplength(df,None,outputdir=join(resultsdir,experiment_dir),yrange=yrange)
        graph_double_oracle_progress(exp_df, join(resultsdir,experiment_dir,'output'))
    

def add_do_stats(resultsdir, df):

    def func(s):
        #print(s)
        directory = s['results_dir']
        #print(directory)
        exp_df = load_do_dataframe(directory, resultsdir)
        exp_df = exp_df.iloc[1:-1]
        number_iterations = len(exp_df)
        avg_br_length = exp_df['stepDuration'].mean()
        s['number_iterations'] = number_iterations
        s['br_avg'] = avg_br_length
        print(s)
        return s
    df = df.apply(func,axis=1)
    return df




def generate_total_graphs(df, resultsdir):
    df = df.copy()
    df['timestamp'] = df['timestamp'].apply(lambda ts: "".join(ts.split('_')[:4]))
    gdf = df.groupby(['scenario'])

    for scenario, group_df in gdf:
        print(scenario)
        ac_ax=None
        ac_ax_time=None
        subdf=group_df[['astarexploitability','note','timestamp','total_time','timeExpansion']]
        for timeExpansion, timeGroup in subdf.groupby(['timeExpansion']):

            ac_ax = timeGroup.plot(x='timestamp', y='astarexploitability',label=timeExpansion,ax=ac_ax)
            ac_ax_time = timeGroup.plot(x='timestamp', y='total_time',label=timeExpansion,ax=ac_ax_time)
        fig = ac_ax.get_figure()
        fig.savefig(join(resultsdir,scenario,'graphs','TimeStampExp_'+scenario+'.pdf'))
        pyplot.close(fig)
        fig = ac_ax_time.get_figure()
        fig.savefig(join(resultsdir, scenario, 'graphs', 'totalTimeExp_' + scenario + '.pdf'))
        pyplot.close(fig)



