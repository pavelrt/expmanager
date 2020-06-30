from os.path import join
from .rci_cluster_executor import RciClusterExecutor


local_temp_dirs_location_planserver = '/Users/pavel/temp2'

class PlanServerExecutor(RciClusterExecutor):
    def __init__(self, outputdirectory=join(local_temp_dirs_location_planserver,'ExperimentsTemp'),
                 remote_dir='/home/rytir/ExperimentsTemp',
                 address = 'rytir@plan.felk.cvut.cz', imagename='fraslama'):
        RciClusterExecutor.__init__(self, outputdirectory, remote_dir, address, '', imagename, '')

    def generateDOScriptItem(self, command, doversion='LAMADO'):
        tempdir = join(command['scenario'], command['params'] + "_" + command['timestamp'])
        script_item = ' singularity run ~/%s %s' \
                      ' ExperimentsTemp/ExperimentsTemp/%s params.json' % (self.imagename, doversion, tempdir)
        return script_item

    def generateLAMAExploitabilityScriptItem(self, command):
        tempdir = join(command['scenario'], command['params'] + "_" + command['timestamp'])
        scriptitem = ' singularity run ~/%s LAMAEXPL '\
                     ' ExperimentsTemp/ExperimentsTemp/%s paramsLAMAExpl.json' % (self.imagename, tempdir)
        return scriptitem

    def generateAstarExploitabilityScriptItem(self, command):
        tempdir = join(command['scenario'], command['params'] + "_" + command['timestamp'])
        scriptitem = ' singularity run ~/%s ASTAREXPL ' \
                     ' ExperimentsTemp/ExperimentsTemp/%s paramsAstarExpl.json' % (self.imagename, tempdir)
        return scriptitem