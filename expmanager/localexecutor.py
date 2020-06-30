
import json
from os.path import join
from shutil import copytree

default_double_oracle_path='/Users/pavel/Documents/Projects/FRAS/FRAS/.build/release/'
double_oracle_exec='FRASDoubleOracleRunner'

class LocalExecutor:

    def __init__(self, outputdirectory, double_oracle_path = default_double_oracle_path):
        self.outputdirectory = outputdirectory
        self.double_oracle_path = double_oracle_path



    def executeAStar(self, commands):
        script = []
        for command in commands:
            tempdir = join(command['scenario'], command['params'] + "_" + command['timestamp'])
            resultsdir = command['resultsdir']
            outputdir= join(self.outputdirectory,tempdir)
            copytree(resultsdir,outputdir)
            scriptitem = [join(self.double_oracle_path,double_oracle_exec), '-g', join(outputdir,'game.json'), '-o',
                       outputdir, '-c', command['configuration'], '--cache', join(outputdir, 'planCache'),
                       '--astarinitheur', str(1.0), '--exploitability',
                        '--astarbestrespheurs', str(1.0),'--astarexplbestrespheur', str(command['heuristicsweight'])]

            script.append(" ".join(scriptitem))

        scriptcontent = "#!/bin/bash\n\n" + "\n".join(commands)

        scriptfile = open(join(self.outputdirectory, "startAStarExplScript.sh"), "w")
        scriptfile.write(scriptcontent)
