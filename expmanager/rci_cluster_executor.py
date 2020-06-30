import json
from os.path import join
import os
import subprocess
from shutil import copyfile
from time import gmtime, strftime


local_temp_dirs_location = '/Users/pavel/temp'
remote_temp_dirs_location = '/home/rytirpav/ExperimentsTemp'


class RciClusterExecutor:
    """Class for execution computations on RCI cluster.
    """

    def __init__(
            self,
            imagename, 
            local_directory=join(local_temp_dirs_location, 'ExperimentsTemp'),
            remote_dir=remote_temp_dirs_location,
            address='rytirpav@login.rci.cvut.cz',
            partition='cpu'):
        self.outputdirectory = join(local_directory, 'output')
        self.inputdirectory = join(local_directory, 'input')
        self.partition = partition
        self.address = address
        self.imagename = imagename
        if partition == 'gpu':
            self.clustercommand = 'srun -A ai --partition gpu'
        elif partition == 'cpu':
            self.clustercommand = 'srun -x "n33" -A ai --partition cpu'
        elif partition == 'gpudeadline':
            self.clustercommand = ('srun -x "n[01-20],n33"'
                                   ' -A ai --partition deadline')
        elif partition == 'cpudeadline':
            self.clustercommand = ('srun -x "n[21-33]"'
                                   ' -A ai --partition deadline')
        else:
            raise ValueError('Unsupported partition')
            
        self.remote_dir = remote_dir

        self.jobs_to_start = []
        self.jobs_scheduled = []
        self.running_jobs = {}
        self.python_scripts_path = join('/home/rytirpav', 'scripts')
        self.python_scripts_path_source = ('/Users/pavel/Documents/'
                                           'Projects/expmanager/Clusters/rci')

    def get_number_of_scheduled_jobs(self):
        """Gets number of jobs scheduled on slurm.
        """
        result = subprocess.run([
            'ssh',
            self.address,
            'squeue -a | grep rytirpav | wc -l',
        ], stdout=subprocess.PIPE)
        return int(result.stdout.decode('ascii').strip())

    def get_scheduled_jobs_ids(self):
        """Return list of job ids scheduled on slurm.
        """
        jobids = []
        result = subprocess.run([
            'ssh',
            self.address,
            'squeue -a | grep rytirpav'
        ], stdout=subprocess.PIPE)
        result.check_returncode()
        result_str = result.stdout.decode('ascii')
        for line in result_str.splitlines():
            jobid = line.split()[0]
            jobids.append(jobid)
        return jobids

    def get_scheduled_jobs(self):
        """Finds all scheduled jobs
        """
        result = subprocess.run([
            'ssh',
            self.address,
            'python',
            join(self.python_scripts_path, 'find_scheduled_and_running_jobs.py'),
            join(self.remote_dir,'ExperimentsTemp')
        ], stdout=subprocess.PIPE)
        result.check_returncode()
        result_str = result.stdout.decode('ascii')
        result = json.loads(result_str)
        return result

    def get_all_jobs(self):
        """Finds all jobs
        """
        result = subprocess.run([
            'ssh',
            self.address,
            'python',
            join(self.python_scripts_path, 'find_all_jobs.py'),
            join(self.remote_dir, 'ExperimentsTemp')
        ], stdout=subprocess.PIPE)
        result.check_returncode()
        result_str = result.stdout.decode('ascii')
        result = json.loads(result_str)
        return result

    def cancel_job(self, jobid):
        subprocess.run(['ssh', self.address, 'scancel ' + jobid])

    def cancel_jobs(self, jobids: list):
        for jobid in jobids:
            self.cancel_job(jobid)

    def cancel_all_jobs(self):
        jobids = self.get_scheduled_jobs_ids()
        self.cancel_jobs(jobids)

    def get_results_to_local(self):
        """Copy all results to local.
        Use rather job specific copy.
        """
        subprocess.run([
            'scp',
            '-qr',
            self.address + ":" + self.remote_dir + '/ExperimentsTemp/.',
            self.inputdirectory,
        ]).check_returncode()

    def copy_data_to_remote(self):
        """Copy all temp data to the server.
        Use rather job specific copy.
        """
        subprocess.run([
            'scp',
            '-qr',
            join(self.outputdirectory, '.'),
            self.address + ":" + self.remote_dir + '/ExperimentsTemp/'
        ])

    def copy_start_script_to_remote(self):
        subprocess.run([
            'scp',
            '-q',
            join(self.outputdirectory, 'startScript.sh'),
            self.address + ":" + self.remote_dir + '/ExperimentsTemp/',
        ])

    def delete_results_local_cache_output(self):
        subprocess.run(['rm', '-rf', self.outputdirectory])

    def delete_results_local_cache_input(self):
        subprocess.run(['rm', '-rf', self.inputdirectory])

    def delete_results_remote_cache(self):
        subprocess.run([
            'ssh',
            self.address,
            'rm -rf',
            self.remote_dir + '/ExperimentsTemp/'
        ])

    def check_if_computation_finished(self):
        raise NotImplementedError()

    def copy_job_data_to_remote(self, job):
        subprocess.run([
            'ssh',
            self.address,
            'mkdir',
            '-p',
            '%s/ExperimentsTemp/%s' % (self.remote_dir, job)
        ])
        subprocess.run([
            'scp', 
            '-qr', 
            join(self.outputdirectory, job, '.'),
            self.address + ':'
            + '%s/ExperimentsTemp/%s' % (self.remote_dir, job),
            ])

    def start_job(self, job):
        script_path = '%s/ExperimentsTemp/%s/start_computation.sh' % (
            self.remote_dir, job)
        subprocess.run(['ssh', self.address, 'chmod', '+x', script_path])
        subprocess.run([
            'ssh',
            self.address,
            'nohup',
            '>/dev/null',
            '</dev/null',
            '2>&1', 
            script_path
        ])
        print('Started job -- ' + job + ' time: ' + strftime("%H:%M", gmtime()))
        self.jobs_to_start.remove(job)
        self.jobs_scheduled.append(job)

    def start_jobs(self):
        for job in self.jobs_to_start.copy():
            self.copy_job_data_to_remote(job)
            self.start_job(job)
        # self.jobs_to_start.clear()

    def get_finished_jobs(self):
        """Returns list of relative directory paths of completed jobs on
        the cluster. A job is considered completed if its directory contains
        file 'job_finished.txt'
        """
        proc_output = subprocess.run([
            'ssh',
            self.address,
            'python',
            join(self.python_scripts_path, 'find_finished.py'),
            self.remote_dir
        ], stdout=subprocess.PIPE)
        json_output = proc_output.stdout.decode('ascii')
        # print(json_output)
        finished_jobs = json.loads(json_output)
        return finished_jobs

    def get_interrupted_jobs(self):
        """Find all started experiments by checking presence of 'job_started'
        file. Check if the process is running in the cluster.
        """
        proc_output = subprocess.run([
            'ssh',
            self.address,
            'python',
            join(self.python_scripts_path, 'find_interrupted_jobs.py'),
            self.remote_dir
        ], stdout=subprocess.PIPE)
        json_output = proc_output.stdout.decode('ascii')
        # print(json_output)
        failed_jobs = json.loads(json_output)
        return failed_jobs

    def get_failed_jobs(self):
        """Find all failed jobs by checking presence of 'job_failes'
        file. Check if the process is running in the cluster.
        """
        proc_output = subprocess.run([
            'ssh',
            self.address,
            'python',
            join(self.python_scripts_path, 'find_failed_jobs.py'),
            self.remote_dir
        ], stdout=subprocess.PIPE)
        json_output = proc_output.stdout.decode('ascii')
        # print(json_output)
        failed_jobs = json.loads(json_output)
        return failed_jobs

    def get_not_started_jobs(self):
        """Find all started experiments by checking presence of 'job_started'
        file. Check if the process is running in the cluster.
        """
        proc_output = subprocess.run([
            'ssh',
            self.address,
            'python',
            join(self.python_scripts_path, 'find_not_started.py'),
            self.remote_dir
        ], stdout=subprocess.PIPE)
        json_output = proc_output.stdout.decode('ascii')
        # print(json_output)
        failed_jobs = json.loads(json_output)
        return failed_jobs

    def change_partition_of_all_not_started_job(self, partition):
        """Find all started experiments by checking presence of 'job_started'
        file. Check if the process is running in the cluster. If not it
        reexecutes 'start_computation.sh' script.
        """
        failed_experiments = self.get_not_started_jobs()
        for failed_experiment in failed_experiments:
            self.change_partition_of_job(failed_experiment, partition)
            print('Changed job: ' + failed_experiment)

    def change_partition_of_all_interrupted_job(self, partition):
        """Find all started experiments by checking presence of 'job_started'
        file. Check if the process is running in the cluster. If not it
        reexecutes 'start_computation.sh' script.
        """
        failed_experiments = self.get_interrupted_jobs()
        for failed_experiment in failed_experiments:
            self.change_partition_of_job(failed_experiment, partition)
            print('Changed job: ' + failed_experiment)

    def change_partition_of_job(self, job, partition):
        subprocess.run([
            'ssh', 
            self.address,
            'python',
            join(self.python_scripts_path, 'change_partition.py'),
            "'%s'" % partition,
            self.remote_dir + '/ExperimentsTemp/%s/start_computation.sh'
            % job
            ])

    def restart_job(self, job):
        """Restarts the given job. Assumes all data are allready on the server.
        job is game/params string.
        """
        subprocess.run([
            'ssh', 
            self.address, 
            'chmod', 
            '+x', 
            self.remote_dir + '/ExperimentsTemp/%s/start_computation.sh'
            % job
            ])
        subprocess.run([
            'ssh',
            self.address,
            'nohup',
            self.remote_dir + '/ExperimentsTemp/%s/start_computation.sh' % job,
            '>/dev/null',
            '</dev/null',
            '2>&1'
        ])

    def restart_not_started_jobs(self):
        """Find all started experiments by checking presence of 'job_started'
        file. Check if the process is running in the cluster. If not it
        reexecutes 'start_computation.sh' script.
        """
        failed_experiments = self.get_not_started_jobs()
        for failed_experiment in failed_experiments:
            self.restart_job(failed_experiment)
            print('Restarted job: ' + failed_experiment)

    def restart_interrupted_jobs(self, timestamp: str = None):
        """Find all started experiments by checking presence of 'job_started'
        file. Check if the process is running in the cluster. If not it
        executes 'start_computation.sh' script.
        """
        failed_experiments = self.get_interrupted_jobs()
        if timestamp is not None:
            failed_experiments = [e for e in failed_experiments
                                  if timestamp in e]
        for failed_experiment in failed_experiments:
            self.restart_job(failed_experiment)
            print('Restarted job: ' + failed_experiment)

    def delete_remote_job_data(self, job):
        # print(self.remote_dir + '/ExperimentsTemp/' + job)
        subprocess.run([
            'ssh',
            self.address,
            'rm -rf',
            self.remote_dir + '/ExperimentsTemp/' + job,
        ])

    def copy_job_results_to_local(self, job):
        subprocess.run(['mkdir', '-p', self.inputdirectory + '/' + job])
        subprocess.run([
            'scp',
            '-qr',
            self.address + ':' + self.remote_dir
            + '/ExperimentsTemp/' + job + '/.',
            self.inputdirectory + '/' + job
        ]).check_returncode()

    def start_remote_computation(self):
        subprocess.run([
            'ssh', 
            self.address, 
            'chmod', 
            '+x', 
            self.remote_dir + '/ExperimentsTemp/startScript.sh'
            ])
        subprocess.run([
            'ssh',
            self.address,
            'nohup',
            self.remote_dir + '/ExperimentsTemp/startScript.sh',
            '>/dev/null',
            '</dev/null',
            '2>&1'
        ])

    def generateDOScriptItem(self, tempdir, memory, action, outfile_prefix):
        script_item = (self.clustercommand
                       + ' --cpus-per-task 2 --mem ' + memory + '  --input none'
                       ' --output %s/ExperimentsTemp/%s/output/%sstdout-%%j.txt'
                       ' --error %s/ExperimentsTemp/%s/output/%sstderr.txt'
                       '  singularity run ~/%s %s'
                       ' ExperimentsTemp/ExperimentsTemp/%s params.json ') % (
            self.remote_dir,
            tempdir,
            outfile_prefix,
            self.remote_dir,
            tempdir,
            outfile_prefix,
            self.imagename,
            action,
            tempdir,
        )
        return script_item

    def executeDO(self, commands, action='DO', file_prefix='do'):
        script = []
        for command in commands:
            # join(command['scenario'], command['params'] + "_" + command['timestamp'])
            tempdir = command['tempdir']
            memory = command.get('memory', '32G')
            script_item = self.generateDOScriptItem(
                tempdir, 
                memory, 
                action, 
                file_prefix) + ' &\n'
            script.append(script_item)
            subdir = join(self.outputdirectory, tempdir)
            config = command['config']
            os.makedirs(subdir, exist_ok=False)
            os.makedirs(join(subdir, 'output'))
            json.dump(
                config, 
                open(join(subdir, file_prefix + '_configuration.json'), 'w'))
            json.dump(
                command['next_json'], 
                open(join(subdir, "next.json"), 'w'))
            copyfile(command['gamefile'], join(subdir, "game.json"))
            copyfile(command['frasconfigfile'], join(
                subdir, 'fras_configuration.json'))
            with open(join(subdir, "start_computation.sh"), 'w') \
                    as scriptitemfile:
                scriptitemfile.write("#!/bin/bash\n\n" + script_item)
            self.jobs_to_start.append(tempdir)

        scriptcontent = "#!/bin/bash\n\n" + "\n".join(script)
        scriptcontent += "\n exit 0\n\n"

        scriptfile = open(join(self.outputdirectory, "startScript.sh"), "w")
        scriptfile.write(scriptcontent)

    def generateExploitabilityScriptItem(self, command, progress):
        comstring = 'EXPLOITABILITY'
        # 'EXPLOITABILITYPROGRESS' if progress else 'EXPLOITABILITY'
        tempdir = command['tempdir']
        name = command['name']
        if progress:
            name += 'Progress'
        scriptitem = (self.clustercommand + ' '
                      + '--cpus-per-task 2 --mem %s ' % (command['memory'],)
                      + ' --input none'
                      ' --output %s/ExperimentsTemp/%s/output/'
                      'expl%sstdout-%%j.txt '
                      ' --error %s/ExperimentsTemp/%s/output/expl%sstderr.txt '
                      ' singularity run ~/%s %s '
                      ' ExperimentsTemp/ExperimentsTemp/%s ') % (
            self.remote_dir,
            tempdir, name,
            self.remote_dir,
            tempdir,
            name,
            self.imagename,
            comstring,
            tempdir,
        )
        return scriptitem

    def executeExploitability(self, commands, progress):
        script = []
        for command in commands:
            tempdir = command['tempdir']
            scriptitem = self.generateExploitabilityScriptItem(
                command, progress=progress) + " &\n"
            script.append(scriptitem)

            subdir = join(self.outputdirectory, tempdir)
            os.makedirs(join(subdir, 'output'), exist_ok=True)
            copyfile(join(command['resultsdir'], 'AllResults.json'),
                     join(subdir, 'AllResults.json'))
            copyfile(command['gamefile'], join(subdir, 'game.json'))
            copyfile(command['frasconfigfile'], join(
                subdir, 'fras_configuration.json'))
            json.dump(command['config'], open(
                join(subdir, "exploitability_configuration.json"), 'w'))
            json.dump(
                command['next_json'],
                open(join(subdir, "next.json"), 'w'))
            with open(join(subdir, "start_computation.sh"), 'w') \
                    as scriptitemfile:
                scriptitemfile.write("#!/bin/bash\n\n" + scriptitem)
            self.jobs_to_start.append(tempdir)

        scriptcontent = "#!/bin/bash\n\n" + "\n".join(script)
        scriptcontent += "\n exit 0\n\n"

        scriptfile = open(join(self.outputdirectory, "startScript.sh"), "w")
        scriptfile.write(scriptcontent)

    def update_python_scripts(self):
        subprocess.run([
            'scp',
            '-qr',
            join(self.python_scripts_path_source, '.'),
            self.address + ":" + self.python_scripts_path
        ])
