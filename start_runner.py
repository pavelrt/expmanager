from expmanager.rci_cluster_executor import RciClusterExecutor
from expmanager.manager import Manager
from expmanager.runner import Runner
from os.path import join


experiment_directory = '/Users/pavel/Projects/FRASExperiments/ECAI2020b'
games_directory = '/Users/pavel/Documents/Projects/expmanager/Experiments/Games'
imagename = 'fraslama2020Feb04'

rciexecutor = RciClusterExecutor(partition='cpudeadline', imagename=imagename)
manager = Manager(
    join(experiment_directory, "expdb.pickle"),
    experiment_directory,
    rciexecutor,
    games_directory)
runner = Runner(manager)
runner.run()
