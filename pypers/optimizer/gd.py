from functools import partial

from pypers import Directory, getpath, Workspace, getcfg, field
from pypers.kernel import create_kernel
from pypers.search import create_search
from pypers.utils.specfem import getsize

from .optimizer import Optimizer


class GD(Optimizer):
    """Steepest gradient descent optimization."""
    def setup(self):
        # link adios binaries
        self.ln(getpath('adios', 'bin'), 'adios')

        for i in range(self.niters):
            # workspace for the i-th iteration
            self.add(ws := Workspace(f'iter_{i:02d}', {'iteration': i}))

            # link initial model
            path_model = getpath('model_init') if i == 0 else self.abs(f'iter_{i-1:02d}/model_new.bp')
            ws.add(partial(ws.ln, path_model, 'model_init.bp'), 'link_models')

            # compute kernels
            ws.add(create_kernel('kernel', {'path_model': ws.abs('model_init.bp')}))
            ws.add(partial(ws.ln, ws.abs('kernel/kernels.bp'), 'kernels.bp'), 'link_kernels')

            # compute direction
            ws.add(partial(self.compute_direction, ws, i))

            # line search
            ws.add(create_search('search'))
    
    async def compute_direction(self, ws: Directory, i: int):
        nprocs = getsize()
        await ws.mpiexec(f'../adios/xsteepDescent kernels.bp direction.bp', nprocs=nprocs, walltime='adios')
