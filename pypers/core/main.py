from asyncio import run, iscoroutine

from pypers.core.config import getarg, hasarg, getsys, getcfg
from pypers.core.job import load, add_error
from pypers.core.workflow.directory import Directory


if __name__ == "__main__":
    # execute saved job or MPI task
    if hasarg('mpiexec'):
        try:
            cwd, fid = getarg('mpiexec').split(':')
            func = Directory(cwd).load(f'{fid}.pickle')

            if iscoroutine(result := func()):
                run(result)
        
        except Exception as e:
            add_error(e)

    else:
        job = load()

        if hasarg('s'):
            job.submit()
        
        else:
            job.run()

            # requeue if job stopped because of insufficent time
            if not job.done and not job.error and job.exception and hasarg('r') and getcfg('job', 'requeue'):
                getsys('requeue')()
