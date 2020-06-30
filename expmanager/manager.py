
import os
from os.path import isdir, isfile, join
from os import listdir
import json
from expmanager import errors
from time import gmtime, strftime
from shutil import copyfile, copytree, rmtree
import pickle
import pandas as pd
import re

from expmanager.parameters import extract_parameters_in_list, extract_parameters
from expmanager.parameters import parameters_string, parse_parameters_string
from expmanager import statsgenerator
from expmanager import parameters


class Manager:

    def __init__(self, dbfilename, resultsdir, executor, gamesdir):
        self.dbfilename = dbfilename
        self.executor = executor
        self.resultsdir = resultsdir
        self.gamesdir = gamesdir
        if isfile(dbfilename):
            self.db = pickle.load(open(dbfilename,"rb"))
        else:
            self.db = {}

    def __substitute_into_template(self, template, params):
        result = {}
        for key, value in template.items():
            if isinstance(value, dict):
                subresult = self.__substitute_into_template(value, params)
                result[key] = subresult
            elif (isinstance(value, str)
                  and value.startswith('__')
                  and value.endswith('__')):
                params_key = value.strip('_')
                result[key] = params[params_key]
            else:
                result[key] = value
        return result

    def __generate_configs(self, paramsgrid, templates):
        configs = []
        paramscombinations = extract_parameters_in_list(paramsgrid)
        for template in templates:
            name=template['name']
            for paramscombination in paramscombinations:
                param_string = parameters_string(paramscombination)
                config = self.__substitute_into_template(
                    template,
                    paramscombination)
                configs.append((name+"_"+param_string, config))
        return configs

    def __check_experiment_started(self, command):
        return False
    
    def run_do_experiments_by_config(
            self,
            configname='do_lama_exp_config.json',
            note='',
            frasconfigfilename='fras_singularity_configuration.json',
            config_dir=None):
        config_with_path = (join(self.resultsdir, configname)
                            if config_dir is None
                            else join(config_dir, configname))
        config = json.load(open(config_with_path))
        frasconfigfile = join(self.resultsdir, frasconfigfilename)
        games = config['games']
        memory = config['domemory']
        parmsgrid = config['parametersGrid']
        templates = config['doconfig']
        next_json = config.get('next',[])

        timestamp = strftime("%Y_%m_%d_%H_%M_%S", gmtime())
        commands = []
        for game in games:
            scenariodir = game
            gamefile = join(self.gamesdir,scenariodir, "game.json")
            configs = self.__generate_configs(parmsgrid, templates)
            for configstr,config in configs:
                tempdir = join(scenariodir, configstr + "_" + timestamp)
                command = {
                    'config': config,
                    'scenario': game,
                    'params': configstr,
                    'timestamp': timestamp,
                    'gamefile': gamefile,
                    'tempdir': tempdir,
                    'note': note,
                    'memory': memory,
                    'frasconfigfile': frasconfigfile,
                    'next_json': next_json
                }
                if self.__check_experiment_started(command):
                    continue
                sameexperiments = self.db.get((scenariodir, configstr), [])
                sameexperiments.append({
                    "status": "started",
                    "timestamp": timestamp,
                    'note': note
                })
                self.db[(scenariodir, configstr)] = sameexperiments
                commands.append(command)

        self.executor.executeDO(commands)
        return timestamp

    def savedb(self):
        pickle.dump(self.db,open(self.dbfilename,"wb"))

    def printdb(self):
        print(self.db)

    def run_expl_for_job(
            self,
            job,
            configfilename,
            frasconfigfilename='fras_singularity_configuration.json',
            config_dir=None):
        config_with_path = (
            join(self.resultsdir, configfilename)
            if config_dir is None
            else join(config_dir, configfilename))
        config = json.load(open(config_with_path))
        expl_name = config['name']
        memory = config['memory']
        next_json = config.get('next', [])
        progress = config.get('progress', False)
        frasconfigfile = join(self.resultsdir,frasconfigfilename)
        experiment_result_dir = join(self.resultsdir, job)
        game, params = job.split('/')
        configstr = params[:-20]
        exp_timestamp = params[-19:]
        if params[-20] != '_':
            raise errors.ExperimentDataError()
        gamefile = join(experiment_result_dir, 'game.json')
        tempdir = join(game, configstr + "_" + exp_timestamp)
        if not isfile(join(experiment_result_dir, 'AllResults.json')):
            raise errors.ExperimentDataError('Cannot compute expl.'
                                             ' on unfinished DO run.')
        command = {
            'config': config,
            'scenario': game,
            'params': configstr,
            'timestamp': exp_timestamp,
            'gamefile': gamefile,
            'tempdir': tempdir,
            'memory': memory,
            'frasconfigfile': frasconfigfile,
            'resultsdir': experiment_result_dir,
            'name': expl_name,
            'next_json': next_json
        }
        self.executor.executeExploitability([command], progress)

    def run_rap_planner(
            self,
            configname,
            frasconfigfilename='fras_singularity_configuration.json',
            config_dir=None):
        config_with_path = (join(self.resultsdir, configname)
                            if config_dir is None
                            else join(config_dir, configname))
        config = json.load(open(config_with_path))
        frasconfigfile = join(self.resultsdir, frasconfigfilename)
        games = config['games']
        memory = config['rapmemory']
        #number_of_trials = config['numberOfTrials']
        parmsgrid = config['parametersGrid']
        templates = config['rapconfig']
        next_json = config.get('next',[])

        timestamp = strftime("%Y_%m_%d_%H_%M_%S", gmtime())
        commands = []
        for game in games:
            scenariodir = game
            gamefile = join(self.gamesdir,scenariodir, "game.json")
            configs = self.__generate_configs(parmsgrid, templates)
            for configstr,config in configs:
                tempdir = join(scenariodir, configstr + "_" + timestamp)
                command = {
                    'config': config, 
                    'scenario': game, 
                    'params':configstr,
                    'timestamp':timestamp,
                    'gamefile':gamefile,
                    'tempdir':tempdir, 
                    'memory': memory,
                    'frasconfigfile':frasconfigfile, 
                    'next_json': next_json
                    }
                if self.__check_experiment_started(command):
                    continue
                commands.append(command)

        self.executor.executeDO(commands,'--rapconfig','rap')
        return timestamp

    def calculate_exploitability(
            self, 
            timestamp=None, 
            progress=False, 
            rerun=False, 
            configfilename='exploitability_configuration.json', 
            frasconfigfilename='fras_singularity_configuration.json', 
            config_dir=None):
        config_with_path = (join(self.resultsdir, configfilename)
                            if config_dir is None
                            else join(config_dir, configfilename))
        config = json.load(open(config_with_path))
        expl_name = config['name']
        memory = config['memory']
        frasconfigfile = join(self.resultsdir,frasconfigfilename)
        commands = []
        astarexpkey = ('exploitabilityprogress'
                       if progress
                       else 'exploitability') + expl_name

        for game in next(os.walk(self.resultsdir))[1]:
            for params in next(os.walk(join(self.resultsdir,game)))[1]:
                experiment_result_dir = join(self.resultsdir,game,params)
                if isfile(join(experiment_result_dir,'AllResults.json')):
                    gamefile = join(experiment_result_dir,'game.json')
                    configstr = params[:-20]
                    exp_timestamp = params[-19:]
                    tempdir = join(game, configstr + "_" + exp_timestamp)
                    if params[-20] != '_':
                        raise errors.ExperimentDataError()
                    if timestamp is not None and timestamp != exp_timestamp:
                        continue
                    if isfile(join(
                            experiment_result_dir,
                            'Exploitability'+expl_name+'.json')) and not rerun:
                        continue
                    command = {
                        'config': config, 
                        'scenario': game, 
                        'params': configstr, 
                        'timestamp': exp_timestamp, 
                        'gamefile': gamefile,
                        'tempdir': tempdir, 
                        'memory': memory, 
                        'frasconfigfile': frasconfigfile, 
                        'resultsdir': experiment_result_dir, 
                        'name': expl_name
                    }
                    commands.append(command)
                    same_experiments = self.db.get((game, configstr), [])
                    for exp in same_experiments:
                        if exp['timestamp'] == exp_timestamp:
                            exp[astarexpkey] = {'status': 'started'}
                    print("Scheduling: " + game + ' - ' + configstr +
                          " " + exp_timestamp)
        self.executor.executeExploitability(commands, progress)


    def check_if_res_dir_is_dsp(self, results_dir: str) -> bool:
        """Check if tthe given dir is result of deadline sampling
        planner computation.
        """
        return isfile(join(
        results_dir,
        'output',
        'RAPResultPlans_1.json'))

    def check_DSP_finished_and_copy(
            self, 
            experiment_result_dir: str, 
            dirtosave: str):
        if not self.check_if_res_dir_is_dsp(experiment_result_dir):
            return False
        
        jobid = [
            dostdout[10:-4]
            for dostdout in next(
                os.walk(join(experiment_result_dir, 'output')))[2]
            if (dostdout.endswith('.txt')
                and dostdout.startswith('rapstdout-'))
        ][0]

        os.makedirs(dirtosave, exist_ok=True)
        match_obj = re.compile('RAPResultPlans_([0-9]+).json')
        result_files = [
            filename
            for filename in next(
                os.walk(
                    join(experiment_result_dir, 'output')))[2]
            if match_obj.match(filename)
        ]
        match_obj = re.compile(
            'RAPResultPlans_([0-9]+)_timing.json')
        result_timing_files = [
            filename
            for filename in next(
                os.walk(join(experiment_result_dir, 'output')))[2]
            if match_obj.match(filename)
        ]

        for result_file, result_file_timing in zip(
                result_files,
                result_timing_files):
            copyfile(
                join(experiment_result_dir, 'output', result_file),
                join(dirtosave, result_file))
            copyfile(
                join(
                    experiment_result_dir,
                    'output',
                    result_file_timing),
                join(dirtosave, result_file_timing))

        copyfile(
            join(
                experiment_result_dir,
                'output',
                'rapstdout-%s.txt' % jobid),
            join(dirtosave, 'rapstdout.txt'))
        copyfile(
            join(experiment_result_dir, 'output', 'rapstderr.txt'),
            join(dirtosave, 'rapstderr.txt'))
        if isdir(join(dirtosave, 'planCacheRAP')):
            rmtree(join(dirtosave, 'planCacheRAP'))
        copytree(
            join(
                experiment_result_dir,
                'output',
                'planCacheRAP'),
            join(dirtosave, 'planCacheRAP'))

        copyfile(
            join(experiment_result_dir, 'rap_configuration.json'),
            join(dirtosave, 'rap_configuration.json'))
        copyfile(
            join(experiment_result_dir, 'fras_configuration.json'),
            join(dirtosave, 'fras_configuration.json'))
        copyfile(
            join(experiment_result_dir, 'game.json'),
            join(dirtosave, 'game.json'))
        
        
        return True

    def check_results(self):
        next_jobs = []
        for game in next(os.walk(self.executor.inputdirectory))[1]:
            for params in next(os.walk(
                    join(self.executor.inputdirectory, game)))[1]:
                experiment_result_dir = join(
                    self.executor.inputdirectory, game, params)
                dirtosave = join(self.resultsdir, game, params)
                configstr = params[:-20]
                timestamp = params[-19:]
                if params[-20] != '_':
                    raise errors.ExperimentDataError()

                if self.check_DSP_finished_and_copy(
                        experiment_result_dir, dirtosave):
                    print('RAP planner finished for game: %s - %s_%s'
                          % (game, configstr, timestamp))
                    next_job = (
                        json.load(
                            open(join(experiment_result_dir, 'next.json'))),
                        join(game, params))
                    next_jobs.append(next_job)
                

                if isfile(join(
                        experiment_result_dir,
                        'output',
                        'AllResults.json')):
                    jobid = [
                        dostdout[9:-4]
                        for dostdout in next(
                            os.walk(join(experiment_result_dir, 'output')))[2]
                        if (dostdout.endswith('.txt')
                            and dostdout.startswith('dostdout-'))
                    ][0]
                    os.makedirs(dirtosave, exist_ok=True)
                    copyfile(
                        join(
                            experiment_result_dir, 
                            'output', 
                            'AllResults.json'), 
                            join(dirtosave, 'AllResults.json'))
                    copyfile(
                        join(
                            experiment_result_dir,
                            'output',
                            'dostdout-%s.txt' % jobid),
                        join(dirtosave, 'dostdout.txt'))
                    copyfile(
                        join(experiment_result_dir, 'output', 'dostderr.txt'),
                        join(dirtosave, 'dostderr.txt'))
                    if isdir(join(dirtosave, 'planCache')):
                            rmtree(join(dirtosave, 'planCache'))
                    copytree(
                        join(experiment_result_dir, 'output', 'planCache'),
                        join(dirtosave, 'planCache'))

                    copyfile(
                        join(experiment_result_dir, 'do_configuration.json'),
                        join(dirtosave, 'do_configuration.json'))
                    copyfile(
                        join(experiment_result_dir, 'fras_configuration.json'),
                        join(dirtosave, 'fras_configuration.json'))
                    copyfile(
                        join(experiment_result_dir, 'game.json'),
                        join(dirtosave, 'game.json'))

                    started_experiments = self.db.get((game, configstr), [])
                    for exp in started_experiments:
                        if exp['timestamp'] == timestamp:
                            exp['status'] = 'finished'
                            break
                    else:
                        started_experiments.append({
                            "status": "finished",
                            "timestamp": timestamp,
                            'note': 'missing starting record'
                        })

                    self.db[(game,configstr)] = started_experiments
                    
                    print("DO finished for game: " + game + " - " + 
                          configstr + "_" + timestamp)
                    next_job = (
                        json.load(
                            open(join(experiment_result_dir, 'next.json'))),
                        join(game, params)
                    )
                    next_jobs.append(next_job)

                exploitability_result_files = [
                    ef
                    for ef in next(
                        os.walk(join(experiment_result_dir, 'output')))[2]
                    if (ef.endswith('.json') and ef.startswith('Exploitability')
                    and not ef.startswith('ExploitabilityPlans'))]
                for ef in exploitability_result_files:
                    name = ef[14:-5]
                    jobid = [
                        expstdout[11 + len(name):-4]
                        for expstdout in next(
                            os.walk(join(experiment_result_dir, 'output')))[2]
                        if (expstdout.endswith('.txt')
                            and expstdout.startswith('expl'+name+'stdout-'))
                    ][0]
                    copyfile(
                        join(experiment_result_dir, 'output', ef),
                        join(dirtosave, ef))
                    if isfile(join(
                            experiment_result_dir,
                            'output','ExploitabilityPlans' + name + '.json')):
                        copyfile(
                            join(
                                experiment_result_dir,
                                'output',
                                'ExploitabilityPlans' + name + '.json'),
                            join(
                                dirtosave, 
                                'ExploitabilityPlans' + name + '.json')
                        )
                    copyfile(
                        join(
                            experiment_result_dir,
                            'output',
                            'expl'+name+'stderr.txt'),
                        join(dirtosave, 'expl'+name+'stderr.txt'))
                    copyfile(
                        join(
                            experiment_result_dir,  
                            'output',
                            'expl'+name+'stdout-' + jobid + '.txt'), 
                        join(dirtosave, 'expl'+name+'stdout.txt'))
                    copyfile(
                        join(
                            experiment_result_dir, 
                            'exploitability_configuration.json'), 
                        join(
                            dirtosave, 
                            'exploitability_'+name+'configuration.json'))
                    exploitPlansDir = 'planCacheExploit' + name
                    
                    if isdir(join(
                            experiment_result_dir,
                            'output',
                            exploitPlansDir)):
                        if isdir(join(dirtosave, exploitPlansDir)):
                            rmtree(join(dirtosave, exploitPlansDir))
                        copytree(
                            join(
                                experiment_result_dir, 
                                'output',
                                exploitPlansDir), 
                            join(dirtosave, exploitPlansDir))

                    started_experiments = self.db.get((game,configstr),[])
                    explkey = 'exploitability'+name
                    for exp in started_experiments:
                        if exp['timestamp'] == timestamp:
                            exprecord = exp.get(explkey,{})
                            exprecord['status'] = 'finished'
                            exp[explkey] = exprecord
                    print('Exploitability computation ' + name +
                          'finished for game: ' + game + " - " +
                          configstr + "_" + timestamp)
                    next_job = (
                        json.load(open(
                            join(experiment_result_dir, 'next.json'))),
                        join(game, params)
                    )
                    next_jobs.append(next_job)

        return next_jobs


    def get_started_experiments(self):
        data = []
        for (scenario_params, experiments) in self.db.items():
            unfinished_experiments = [
                exp
                for exp in experiments
                if exp['status'] != 'finished'
            ]
            for exp in unfinished_experiments:
                scenario = scenario_params[0]
                paramsstring = scenario_params[1]
                data.append((scenario, paramsstring, exp['timestamp']))
        return data

    def remove_experiments(self, experiments_to_remove):
        for exp_to_remove in experiments_to_remove:
            exps = self.db[exp_to_remove[:2]]
            if exps:
                newexps = [
                    e
                    for e in exps
                    if e['timestamp'] != exp_to_remove[2]
                ]
                if newexps:
                    self.db[exp_to_remove[:2]] = newexps
                else:
                    del self.db[exp_to_remove[:2]]

    def delete_experiments_with_timestamp(self, timestamp: str):
        """Delete all experiments directories with the given timestamp.

        :param timestamp:
        """
        if not timestamp:
            raise ValueError()
        dirs_to_delete = []
        for path, dir_names, _ in os.walk(self.resultsdir):
            for dir_name in dir_names:
                parms = parameters.parse_fras_exp_dirname(dir_name)
                if parms is not None and parms['timestamp'] == timestamp:
                    dirs_to_delete.append(os.path.join(path, dir_name))

        for dir_to_delete in dirs_to_delete:
            rmtree(dir_to_delete)


    def get_experiments_results(self):
        data = []
        for (scenario_params, experiments) in self.db.items():
            finishedexperiments = [
                e
                for e in experiments
                if (e['status'] == 'finished'
                    and e.get(
                        'astarexplotability',
                        {'status': ''})['status'] == 'finished'
                    and e.get(
                        'lamaexploitability',
                        {'status': ''})['status'] == 'finished')
            ]
            for experiment in finishedexperiments:

                scenario = scenario_params[0]
                paramsstring = scenario_params[1]

                experiment_dir = join(
                    scenario, 
                    'results', 
                    paramsstring + "_" + experiment["timestamp"])
                inputdir = join(self.resultsdir, experiment_dir)
                params = json.load(open(join(inputdir,'params.json')))
                note = params.get('note','')
                results = json.load(
                    open(join(inputdir,'output', 'AllResults.json')))
                explLama = json.load(
                    open(join(inputdir,'output', 'ExploitabilityLAMA.json')))
                explAstar = json.load(
                    open(join(inputdir,'output', 'ExploitabilityAstar.json')))

                stat = results['statistics']
                df = pd.DataFrame.from_dict(stat)
                df = df.set_index('iteration')
                df['timePlanner'] = df.stepDuration.cumsum()

                total_time = df.iloc[-1]['timePlanner']
                iterations = len(df)
                expl = explLama['p1Exploitability']
                explAstar = explAstar['p1Exploitability']
                eqValP1 = stat[-1]['player1EqVal']
                timestamp = experiment['timestamp']

                data.append((
                    scenario,
                    paramsstring,
                    float(params['timeExpansion']),
                    params['initTime'],
                    params['bestRespTime'],
                    params['doconfig'],
                    params['critactionheur'],
                    total_time,
                    iterations,
                    expl,
                    explAstar,
                    eqValP1,
                    experiment_dir,
                    timestamp,
                    note
                ))

        columns = [
            'scenario',
            'allparams',
            'timeExpansion',
            'initTime',
            'bestRespTime',
            'doconfiguration',
            'critactionheur',
            'total_time',
            'iterations',
            'lamaExploitability',
            'astarexploitability',
            'equilibriumValueP1',
            'results_dir',
            'timestamp',
            'note'
        ]
        df = pd.DataFrame(data, columns=columns)
        df = df.set_index(['scenario', 'allparams', 'timestamp'])
        return df

    def get_double_oracle_data(self, scenario_params_timestamp):
        inputfile = open(
            join(
                self.resultsdir,
                scenario_params_timestamp[0],
                'results',
                scenario_params_timestamp[1] + "_"
                + scenario_params_timestamp[2],
                'output',
                'AllResults.json'))
        stats = json.load(inputfile)['statistics']
        df = pd.DataFrame.from_dict(stats)
        df.set_index('iteration')
        df['timePlanner'] = df['stepDuration'].cumsum()
        return df



    def schedule_next_jobs(self,jobs):
        """Schedule jobs given by the jobs argument.
        Args:
            jobs: list of tuples (config, job(gameparam)) to be scheduled.
        """
        for configs, gameparm in jobs:
            for config in configs:
                config_filename = config.split('/')[-1]
                if not os.path.isfile(config):
                    continue
                if config_filename.startswith('expl'):
                    self.run_expl_for_job(gameparm, config)
                    self.executor.start_jobs()
                    self.executor.delete_results_local_cache_output()
                elif config_filename.startswith('stats'):
                    result_dir = join(self.resultsdir,gameparm)
                    statsgenerator.generate_stats(result_dir,config)


    def get_experiments_with_finished_do(self):
        """Finds all subdirectorires with finished do computation.
        """
        finished = []
        for (dirpath, _ , filenames) in os.walk(self.resultsdir):
            if 'AllResults.json' in filenames:
                game, param = dirpath.split('/')[-2:]
                finished.append('/'.join([game, param]))
        return finished


    def compute_jobs_on_finished_do(self, job_config):
        finished = self.get_experiments_with_finished_do()
        self.run_job_for_dirs(finished, job_config)


    def run_job_for_dirs(self, dirs, job_config):
        jobs = [([job_config], directory) for directory in dirs]
        self.schedule_next_jobs(jobs)
        


