from pyper import config


class Job:
    """Configurations for a job."""
    # number of CPUs per node
    cpus_per_node: int

    # number of GPUs per node
    gpus_per_node: int

    # number of nodes to run the job
    nnodes: int

    # estimated execution time
    walltime: int

    # time interval between checkpoints
    checkpoint_interval: int

    def __init__(self, **kwargs):
        """Get cluster and job configurations."""
        self._get_cluster_config(kwargs)
        self._get_job_conofig(kwargs)

    def _get_cluster_config(self, kwargs):
        """Get node configuration of cluster."""
        # cluster module
        cluster = config.get_module('pyper.cluster')
        
        # cpu configuration
        cpus_per_node = kwargs.get('cpus_per_node') or \
            (cluster.cpus_per_node if hasattr(cluster, 'cpus_per_node') else
             config.get_input('job', 'cpus_per_node', 'Enter the number of CPUs per node in you system:', int))
        
        if not isinstance(cpus_per_node, int) or cpus_per_node <= 0:
            raise ValueError('Number of CPUs must be a positive integer.')

        self.cpus_per_node = cpus_per_node
    
        # gpu configuration
        gpus_per_node = kwargs.get('gpus_per_node') or \
            (cluster.gpus_per_node if hasattr(cluster, 'gpus_per_node') else
             config.get_input('job', 'gpus_per_node', 'Enter the number of GPUs per node in you system:', int))
        
        if not isinstance(gpus_per_node, int) or gpus_per_node < 0:
            raise ValueError('Number of GPUs must be a non-negative integer.')

        self.gpus_per_node = gpus_per_node
    
    def _get_job_conofig(self, kwargs):
        """Get job configurations."""
        # number of nodes to run the job
        nnodes = kwargs.get('nnodes') or \
            config.get_input('job', 'nnodes', 'Enter the number of nodes to run your job:', int)

        if not isinstance(nnodes, int) or nnodes <= 0:
            raise ValueError('Number of nodes must be a positive integer.')

        self.nnodes = nnodes

        # estimated execution time
        walltime = kwargs.get('walltime') or \
            config.get_input('job', 'walltime', 'Enter the execution time of your job in minutes:', int)
            
        if not isinstance(walltime, int) or walltime <= 0:
            raise ValueError('Walltime must be a positive integer.')

        self.walltime = walltime

        # time interval between checkpoints
        checkpoint_interval = kwargs.get('checkpoint_interval') or \
            config.get('job', 'checkpoint_interval') or 5
            
        if not isinstance(checkpoint_interval, int) or checkpoint_interval <= 0:
            raise ValueError('Checkpoint interval must be a positive integer.')

        self.checkpoint_interval = checkpoint_interval
