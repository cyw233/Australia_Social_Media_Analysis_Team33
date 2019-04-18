# Environment Deployment

Use **Boto** to create instances, create volumes, attach a  volume to an instance, and etc.

Use **Ansible** to automate the environments, like installing packages, uploading code, and etc.



## Usage

1. Go to *instances_deployment_boto* folder,

   * compile and run `launch_small.py` , then three small-size instances will be created. Scenarios python files run on these instances.

   * compile and run `launch_medium.py`, then one medium-size instance will be created. Front end application runs on this instance.

     

2. Go to *ansible_playbooks* folder,

   * run `ansible-playbook -i hosts -u ubuntu --key-file=cluster1.key main.yaml` to

     - [x] **mount** the first three instances, and **install docker and required python packages** on them.
     - [x] **mount** the last instance, and **install docker, Node.js, npm and some required npm packages**  on it.

   * revise `upload_code.yaml` if need and execute

     ​	 `ansible-playbook -i hosts -u ubuntu --key-file=cluster1.key upload_code.yaml`  

     to upload python files for different scenarios to specified VM.

   * revise `run_scenarios.yaml` if need and execute 

     ​        `ansible-playbook -i hosts -u ubuntu --key-file=cluster1.key run_scenarios.yaml`

     to run the python files for different scenarios on specified VM.



3. Follow the instructions in  *cluster_setup.sh* file to setup the couchdb cluster.
