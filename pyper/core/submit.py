from subprocess import check_call
from typing import Union

from pyper import config, shell, Task, Block
from pyper.core.job import Job
from pyper.core.workflow import Workflow


def submit(main: Union[Task, Block], **kwargs):
    """Finalize and submit to job scheduler.
    
    Args:
        main (Union[Task, Block]): main task or block to be submitted
        **kwargs: job parameters
    """
    # create job and workflow
    job = Job(**kwargs)
    workflow = Workflow(main, job)

    # create directories
    shell.mkdir('scratch/tasks')
    shell.mkdir('output')

    # check workflow configurations and save checkpoint
    workflow.log()
    workflow.save()

    # cluster module
    cluster = config.get_module('pyper.cluster')

    # write job script
    script = '\n'.join(cluster.header(job))

    pre_exec = config.get('job', 'pre_exec')
    if pre_exec:
        script += '\n%s\n' % '\n'.join(pre_exec)

    script += f'\npython -m "pyper.core.main"\n'

    post_exec = config.get('job', 'post_exec')
    if post_exec:
        script += '\n%s\n' % '\n'.join(post_exec)

    # save job script
    shell.write('scratch/job.bash', script)

    # submit to scheduler and get job id
    check_call(cluster.jobexec('scratch/job.bash'), shell=True)
