from pypers import Directory, field
from pypers.utils.specfem import getsize

from .gd import GD


class LBFGS(GD):
    """L-BFGS optimization."""
    # number of steps stored
    mem: int = field()

    async def compute_direction(self, ws: Directory, i: int):
        if i == 0:
            # steepest descent for the first iteration
            await super().compute_direction(ws, i)
        
        else:
            # input paths
            path_file = ws.abs('path.txt')
            direction = ws.abs('direction.bp')
            solver_file = ws.abs('kernel/solver_synthetic/DATABASES_MPI/solver_data.bp')
            nprocs = getsize()

            # write path to history kernels and directions
            lines = [str(min(self.mem, i))]
            
            for j in range(max(0, i-self.mem), i):
                lines.append(self.abs(f'iter_{j:02d}/kernels.bp'))
                lines.append(self.abs(f'iter_{j:02d}/direction.bp'))
            
            lines.append(ws.abs('kernels.bp'))
            ws.write('\n'.join(lines), path_file)

            await ws.mpiexec(f'../adios/xlbfgs {path_file} {solver_file} {direction}', nprocs=nprocs, walltime='adios')
