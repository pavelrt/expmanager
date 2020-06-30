from os.path import isfile, join
import pickle
import os
import json
import subprocess
from expmanager.parameters import parameters_string, parse_parameters_string
import pandas as pd


class FRASAnalytics:
    def __init__(self, resultsdir, gamesdir, dbfilename=None, frasanalyticsbin='/Users/pavel/Documents/Projects/expmanager/expmanager/.build/x86_64-apple-macosx/debug/FRASAnalytics'):
        self.dbfilename = dbfilename
        self.resultsdir = resultsdir
        self.gamesdir = gamesdir
        self.frasanalyticsbin = frasanalyticsbin
        if dbfilename is not None and isfile(dbfilename):
            self.db = pickle.load(open(dbfilename,"rb"))
        else:
            self.db = {}
    
    def print_do_stats(self):
        for game in sorted(next(os.walk(self.resultsdir))[1]):
            print('Game: ' + game)
            params_timestamp = [(params_dir[:-20], params_dir[-19:], params_dir) for params_dir in next(os.walk(join(self.resultsdir,game)))[1]]
            params_timestamp = sorted(params_timestamp)
            
            last_params = ''

            for (params,timestamp,params_dir) in params_timestamp:
                experiment_result_dir = join(self.resultsdir,game,params_dir)
                if not isfile(join(experiment_result_dir,'AllResults.json')):
                    continue

                exploitability_results = {ef[14:-5]:json.load(open(join(experiment_result_dir,ef))) for ef in next(os.walk(join(experiment_result_dir)))[2] if ef.endswith('.json') and ef.startswith('Exploitability')}

                if last_params != params:
                    print(5 * ' ' + params)
                    last_params = params
                print(10 * ' ' + timestamp)
                doresults = json.load(open(join(experiment_result_dir,'AllResults.json')))
                dostats = doresults['statistics']
                duration = self.extract_total_time(dostats)
                print(15 * ' ' + "Number of br computations: %d  -- Duration: %d " % (len(dostats), duration))

                for name, exp_res in exploitability_results.items():
                    print(15 * ' ' + 'Exploitability %s P1: %.2f  -  P2: %.2f' % (name, exp_res['p1Exploitability'],exp_res['p2Exploitability']))


    def compute_cross_strats_value(self, gamefile, file1, file2):
        command = [self.frasanalyticsbin, '--command', 'evaluateStrats', '--game',
                           gamefile, '--output', '/dev/stdout']
        if file1.split('/')[-1].startswith('RAPResultPlans_'):
            command.append('--p1plans')
        elif file1.endswith('AllResults.json'):
            command.append('--p1do')
        else:
            raise ValueError()
        command.append(file1)

        if file2.split('/')[-1].startswith('RAPResultPlans_'):
            command.append('--p2plans')
        elif file2.endswith('AllResults.json'):
            command.append('--p2do')
        else:
            raise ValueError()
        command.append(file2)
        result = subprocess.run(command, stdout=subprocess.PIPE)
        resultjson = json.loads(result.stdout.decode('ascii'))
        return resultjson

    
    def compute_cross_strats_values(self, game):
        game_results_dir = join(self.resultsdir,game)
        gamefile = join(self.gamesdir,game,'game.json')
        params_timestamp = [(params_dir[:-20], params_dir[-19:], params_dir) for params_dir in next(os.walk(join(self.resultsdir,game)))[1] if params_dir != 'stats' and isfile(join(game_results_dir,params_dir,'AllResults.json'))]
        params_timestamp = sorted(params_timestamp)
        
        params_timestamp_dict = {}
        for param, timestamp, ptdir in params_timestamp:
            experiments = params_timestamp_dict.get(param,[])
            experiments.append((timestamp,ptdir))
            params_timestamp_dict[param] = experiments
        
        params_last_timestamp = []
        for param,timestamps in params_timestamp_dict.items():
            timestamps = sorted(timestamps, key= lambda x : x[0])
            params_last_timestamp.append((param,timestamps[-1]))
        
        os.makedirs(join(game_results_dir, 'stats'),exist_ok=True)

        #computation_combinations = []
        cross_data = []
        cross_cols = [parse_parameters_string(pt2[0])['HEUR'] for pt2 in params_last_timestamp]
        cross_rows = []
        for pt1 in params_last_timestamp:
            cross_row = []
            cross_rows.append(parse_parameters_string(pt1[0])['HEUR'])
            for pt2 in params_last_timestamp:
                do1file = join(game_results_dir, pt1[1][1], 'AllResults.json')
                do2file = join(game_results_dir, pt2[1][1], 'AllResults.json')
                outputfile = join(game_results_dir, 'stats',
                                  'cross_' + pt1[0] + '___' + pt2[0] + '.json')
                #computation_combinations.append((do1file,do2file,outputfile))
                command = [self.frasanalyticsbin, '--command', 'evaluateStrats', '--game',
                           gamefile, '--p1do', do1file, '--p2do', do2file, '--output', outputfile]
                #print(command)
                subprocess.run(command)

                cross_result = json.load(open(outputfile))
                cross_row.append((cross_result['p1Value']))
               
            cross_data.append(cross_row)
        

        latex_tabular = r'\begin{tabular}{r' + r'|r' * len(cross_cols) + r'}' + '\n'
        latex_tabular += ' & ' + ' & '.join(cross_cols) + '\\\\\n'
        latex_tabular += '\\hline\n'
        print(cross_cols)
        for rowname,row in zip(cross_rows,cross_data):
            print(rowname, row)
            latex_tabular += rowname + ' & ' + ' & '.join(["%.2f" % round(cell,2)for cell in row]) + '\\\\\n'
            latex_tabular += '\\hline\n'
        latex_tabular += r'\end{tabular}'
        print(latex_tabular)
        with open(join(game_results_dir,'do_cross_vals_latex.txt'),'w') as f:
            f.write(latex_tabular)




            





        #print(computation_combinations)

    def extract_total_time(self,stats):
        df = self.load_do_results_to_dataframe(stats)
        return df.iloc[-1]['timePlanner']

    def load_do_results_to_dataframe(self, stats):
        df = pd.DataFrame.from_dict(stats)
        df['iteration_index'] = df['iteration'].copy()
        df = df.set_index('iteration_index')
        df['timePlanner'] = df['stepDuration'].cumsum()
        return df








