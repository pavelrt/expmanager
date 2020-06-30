import subprocess

class Builder:

    def __init__(self,imagename):
        self.build_server_url = 'rytir@plan.felk.cvut.cz'
        self.build_server_singularity_dir = '/home/rytir/Singularity'
        self.fras_sources_dir = '/Users/pavel/Documents/Projects/FRAS/FRAS'
        self.singularity_files_dir = '/Users/pavel/Documents/Projects/FRAS/Singularity/FRAS'
        self.singularity_build_script = '/Users/pavel/Documents/Projects/expmanager/Singularity/buildFRASSingularity.sh'
        self.target_server_url = 'rytirpav@login.rci.cvut.cz'
        self.imagename = imagename

    def delete_singularity_files_from_build_server(self):
        subprocess.run(['ssh', self.build_server_url, 'rm', '-rf', self.build_server_singularity_dir + '/expmanager'])

    def copy_singularity_files_to_build_server(self):
        subprocess.run(["rsync", "-aqzpL", "-e", "ssh",
                        self.singularity_files_dir,
                        self.build_server_url + ':' + self.build_server_singularity_dir])
        subprocess.run(['scp','-q', self.singularity_build_script,
                        self.build_server_url + ":" +
                        self.build_server_singularity_dir])

    def delete_sources_from_build_server(self):
        subprocess.run(['ssh',self.build_server_url, 'rm', '-rf', self.build_server_singularity_dir +'/expmanager/expmanager'])


    def copy_sources_to_build_server(self):
        subprocess.run(["rsync", "-aqzpL", "-e", "ssh", "--exclude", ".git", "--exclude",
                        ".build/x86_64*", "--exclude", ".build/debug", "--exclude",
                        ".build/release", "--exclude", "CCplex.git*",
                        "--exclude", ".build/checkouts/CCplex",
                        self.fras_sources_dir,
                        self.build_server_url + ':' + self.build_server_singularity_dir + '/expmanager'])

        subprocess.run(['ssh', self.build_server_url, 'cp', '-r' ,
                        self.build_server_singularity_dir + '/expmanager/cplexheaders/CCplex',
                        self.build_server_singularity_dir + '/expmanager/expmanager/.build/checkouts'])


    def build_singularity_image(self):
        #subprocess.run(['ssh',self.build_server_url, 'rm', '-f', self.build_server_singularity_dir + '/expmanager/'+self.imagename])
        subprocess.run(['ssh',self.build_server_url, self.build_server_singularity_dir + '/buildFRASSingularity.sh', self.imagename])

    def copy_image_from_build_to_target_server(self):
        subprocess.run(['ssh', self.target_server_url, 'rm', '-f', '/home/rytirpav/' + self.imagename])
        subprocess.run(['ssh', self.build_server_url , 'scp','-q', self.build_server_singularity_dir + '/expmanager/' + self.imagename,
                        self.target_server_url + ":" +
                        '/home/rytirpav'])

    def copy_image_from_plan_to_plan(self):
        subprocess.run(['ssh', self.build_server_url, 'rm', '-f', '/home/rytirpav/' + self.imagename])
        subprocess.run(['ssh', self.build_server_url, 'cp', self.build_server_singularity_dir+ '/expmanager/' + self.imagename,
                        '/home/rytir'])

    def delete_image_from_build_server(self):
        subprocess.run(['ssh',self.build_server_url, 'rm', '-f', self.build_server_singularity_dir + '/expmanager/'+self.imagename])


    def update_singularity_image(self):
        print("Image name:" + self.imagename)
        self.delete_sources_from_build_server()
        self.copy_sources_to_build_server()
        self.delete_image_from_build_server()
        self.build_singularity_image()
        self.copy_image_from_build_to_target_server()
        self.copy_image_from_plan_to_plan()
        self.delete_image_from_build_server()
    
    def complete_update_singularity_image(self):
        self.delete_singularity_files_from_build_server()
        self.copy_singularity_files_to_build_server()
        self.update_singularity_image()
        


