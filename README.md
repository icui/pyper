### Command line arguments
-n: run the job in a sub-directory
-r: try to requeue when job exited due to insufficient time
--dir=<job_dir>: job directory

### [job]
name: job display name
account: account name for job submission
walltime: job execution time
nnodes: number of nodes to run job
cpus_per_node: overwrite cluster CPU configuration
gpus_per_node: overwrite cluster GPU configuration
