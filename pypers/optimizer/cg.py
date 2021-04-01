from pypers import Directory
from pypers.utils.specfem import getsize

from .gd import GD


class CG(GD):
    """Conjugate gradient optimization."""
    async def compute_direction(self, ws: Directory, i: int):
        if i == 0:
            # steepest descent for the first iteration
            await super().compute_direction(ws, i)
        
        else:
            # input paths
            direction = ws.abs('direction.bp')
            kernels = ws.abs(f'kernels.bp')
            solver_file = ws.abs('kernel/solver_synthetic/DATABASES_MPI/solver_data.bp')
            nprocs = getsize()

            # previous iteration
            direction0 = self.abs(f'iter_{i-1:02d}/direction.bp')
            kernels0 = self.abs(f'iter_{i-1:02d}/kernels.bp')

            cmd = f'../adios/xcg_direction {kernels0} {kernels} {direction0} {solver_file} {direction}'
            await ws.mpiexec(cmd, nprocs=nprocs, walltime='adios')
