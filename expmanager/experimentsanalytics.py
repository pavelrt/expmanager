import os
import json
from os.path import isdir, join, dirname, split
import pandas as pd
import numpy as np
from matplotlib import pyplot
import math

class AggregateExperimentsResults:
    def __init__(self,experiment_definition_dir):
        self.experiment_definition_dir = experiment_definition_dir
    

    def get_experiments(self):
        results = []
        for (results_dir,_,files) in os.walk(self.experiment_definition_dir):
            if ('params.json' in files and 'AllResults.json' in files and 'ExploitabilityLAMA.json' in files and
                'ExploitabilityAStar.json' in files):
                experimentResults = ExperimentsResults(results_dir,self.experiment_definition_dir)
                results.append(experimentResults)
        return results
    



class ExperimentsResults:
    def __init__(self, results_dir, experiment_definition_dir):
        self.results_dir = results_dir
        self.experiment_definition_dir = experiment_definition_dir
        self.params=json.load(open(join(self.results_dir,'params.json')))
        self.results=json.load(open(join(self.results_dir,'output','AllResults.json')))
        try:
            self.explLama=json.load(open(join(self.results_dir, 'output','ExploitabilityLAMA.json')))
        except FileNotFoundError:
            self.explLama = None
        try:
            self.explAstar=json.load(open(join(self.results_dir, 'output','ExploitabilityAstar.json')))
        except FileNotFoundError:
            self.explAstar = None
        try:
            self.explAstarProgress = json.load(open(join(self.results_dir, 'output','ExploitabilityAstarProgress.json')))
        except FileNotFoundError:
            self.explAstarProgress = None
        self.stats = self.results['statistics']

        self.df = pd.DataFrame.from_dict(self.stats)
        self.df['iteration_index'] = self.df['iteration'].copy()
        self.df = self.df.set_index('iteration_index')
        self.df['timePlanner'] = self.df['stepDuration'].cumsum()

    def get_game_filename(self):
        return join(self.experiment_definition_dir,'game.json')
    def total_time(self):
        return self.df.iloc[-1]['timePlanner']
    
    def number_iterations(self):
        return len(self.df) - 1

    def get_avg_br_duration(self):
        return self.df['stepDuration'][1:-1].mean()

    def get_scenario_name(self):
        path = os.path.normpath(self.results_dir)
        return path.split(os.sep)[-3]

    def get_parms(self):
        path = os.path.normpath(self.results_dir)
        return path.split(os.sep)[-1]
        

    def get_double_oracle_progress_player1(self):
        self.df[['iteration','step','player1BestResponseVal','player1EqVal','stepDuration']]

    def get_best_response_duration_number_of_allowed_predicates_df(self):
        result = []
        for (_,row) in self.df.iterrows():
            if row['step'] == 'BRP1' or row['step'] == 'BRP2' or row['step'] == 'FBRP2' or row['step'] == 'FBRP1':
                player = 1 if row['step'] == 'BRP1' or row['step'] == 'FBRP1' else 2
                iteration = row['iteration']
                plannerdesc = 'LAMAT7200' #row['plannerDescription']
                problempddlfile = open(join(self.results_dir,'output','planCache','player%sBestResponseIteration_%s_%s.json.problem.pddl') % (player,iteration,plannerdesc))
                numberofallowed = 0
                numberofcollectedgoals = 0
                for line in problempddlfile:
                    for word in line.split():
                        #print(word)
                        if word == '(allowed':
                            numberofallowed += 1
                        if word == '(collected':
                            numberofcollectedgoals += 1
                result.append((iteration,row['stepDuration'],numberofallowed,numberofcollectedgoals,row['step']))
        columns = ['iteration','duration','allowed','collected','type']

        res_df = pd.DataFrame(result, columns=columns)
        return res_df





if __name__ == '__main__':
    exp = ExperimentsResults('/Users/pavel/Documents/Projects/FRAS/Experiments/AAMAS2020/Scenario12b_uavs3x3_res0_controls0_mode-sensorsNoComm/results/bestRespTime-7200_critactionheur-True_doconfig-LAMAINITLAMABESTANYTIMESENSORS_explbestresptime-7200_initTime-300_timeExpansion-1_2019_10_23_11_28_09', '/Users/pavel/Documents/Projects/FRAS/Experiments/AAMAS2020/Scenario12b_uavs3x3_res0_controls0_mode-sensorsNoComm')
    df = exp.get_best_response_duration_number_of_allowed_predicates_df()
    print(df)




    
    
