# pylint: skip-file
import sys
import pickle

from pyper import shell


if __name__ == '__main__':
    # load task object
    with open(f'{sys.argv[1]}.pickle', 'rb') as f:
        func, args = pickle.load(f)
    
    if isinstance(args, list):
        # assign unique arguments for each task
        from mpi4py import MPI
        args = args[MPI.COMM_WORLD.Get_rank()]
    
    try:
        # use universal arguments
        result = func(*args)
    
    except Exception as e:
        import traceback
        result = None
        shell.write(f'{sys.argv[1]}.err', traceback.format_exc(), 'a')

    # gather and save results
    if result is not None:
        from mpi4py import MPI
        rank = MPI.COMM_WORLD.Get_rank()
        result = MPI.COMM_WORLD.gather(result, root=0)

        if rank == 0:
            with open(f'{sys.argv[1]}.result.pickle', 'wb') as f:
                pickle.dump(result, f)
