import os
import time
from time import gmtime, strftime
import shutil
from os.path import join
from expmanager.rci_cluster_executor import RciClusterExecutor
from expmanager.manager import Manager
from expmanager import statsgenerator


DEFAULT_RUNNER_DIR = '/Users/pavel/Documents/Projects/FRAS/Experiments/Runner'


class Runner:
    """Class for automatic execution of experiments on a cluster.
    
    """
    def __init__(
            self,
            manager,
            runner_dir=DEFAULT_RUNNER_DIR):
        self.manager = manager
        self.schedule_dir = join(runner_dir,'to_schedule')
        self.processed_dir = join(runner_dir,'processed')

    def run(self):
        """Starts the runner.
        """
        print('Runner started ' + strftime("%H:%M", gmtime()))
        iteration = 0
        while True:
            self.scheduled_new_experiments_by_config()

            if iteration % 10 == 0:
                self.check_finished()

            time.sleep(60)
            iteration += 1
            if iteration % 60 == 0:
                number_of_jobs = self.manager.executor.get_number_of_scheduled_jobs()
                print('Runner alive - time: %s --- Number of running jobs: %i'
                      % (strftime("%H:%M", gmtime()), number_of_jobs))

    def scheduled_new_experiments_by_config(self):
        """Start new experiments by config
        """
        configs = [
            config
            for config in next(os.walk(self.schedule_dir))[2]
            if config.endswith('.json')
        ]
        time.sleep(1)
        for config in configs:
            print('Scheduling experiments by config ' + config)
            self.schedule(config)

    def schedule(self, config):
        """ Determines type of config and call the manager to schedule it.
        TODO: Move to manager.
        """
        if config.startswith('do'):
            timestamp = self.manager.run_do_experiments_by_config(
                configname=config, config_dir=self.schedule_dir)
        elif config.startswith('expl'):
            timestamp = strftime("%Y_%m_%d_%H_%M_%S", gmtime())
            self.manager.calculate_exploitability(
                configfilename=config,
                config_dir=self.schedule_dir)
        elif config.startswith('rap'):
            timestamp = self.manager.run_rap_planner(
                config,
                config_dir=self.schedule_dir)
        else:
            raise ValueError('Unknown config')
        shutil.move(join(self.schedule_dir, config),
                    join(self.processed_dir, timestamp + '-' + config))
        self.manager.executor.start_jobs()
        self.manager.executor.delete_results_local_cache_output()

    def check_finished(self):
        """Check which experiments finished and copy data to local.
        Then deletes the data on the server.
        """
        finished_jobs = self.manager.executor.get_finished_jobs()
        for job in finished_jobs:
            self.manager.executor.copy_job_results_to_local(job)
            next_jobs = self.manager.check_results()
            self.manager.executor.delete_remote_job_data(job)
            self.manager.executor.delete_results_local_cache_input()
            self.manager.schedule_next_jobs(next_jobs)

    
