import json
from expmanager.manager import Manager
from expmanager.localexecutor import LocalExecutor
from expmanager.rci_cluster_executor import RciClusterExecutor
from expmanager.builder import Builder
from expmanager.analytics import FRASAnalytics
from os.path import join
import pandas as pd
import expmanager.statsgenerator as statsgenerator


experiment_directory = '/Users/pavel/Projects/FRASExperiments/ECAI2020b'
# '/Users/pavel/Documents/Projects/expmanager/Experiments/IJCAI2020b/'
games_directory = '/Users/pavel/Documents/Projects/expmanager/Experiments/Games'
imagename = 'fraslama2020Feb04'

rciexecutor = RciClusterExecutor(partition='cpu', imagename=imagename)
manager = Manager(join(experiment_directory, "expdb.pickle"), experiment_directory, rciexecutor, games_directory)

builder = Builder(imagename)

analytics = FRASAnalytics(join(experiment_directory,"expdb.pickle"),experiment_directory,games_directory)

# manager.compute_jobs_on_finished_do('/Users/pavel/Documents/Projects/expmanager/Experiments/Configs/exploitability_fd_potent_configuration.json')


#manager.run_expl_for_job('SC33_UAV3x3_R6_SENSORS2_SYM/FDINITFDBESTANYTIME_MAXTIME-1800_SEARCH-ASTAR-POTENT-1_2020_01_19_11_22_19','/Users/pavel/Documents/Projects/expmanager/Experiments/Configs/exploitability_astar_configuration.json')

#rciexecutor.restart_failed_experiments()

#print("hi")

#statsgenerator.generate_aggregate_stats_by_game(experiment_directory)

#statsgenerator.regenerate_all_stats(experiment_directory)

#manager.run_do_experiments_by_config('do_lama_exp_config_ecai2020.json')

#rciexecutor.start_jobs()

#manager.calculate_exploitability()

#manager.check_results()

#print('done')

#manager.savedb()
