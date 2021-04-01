from typing import List, cast
from functools import partial

import numpy as np

from pypers import Workspace, field, basedir as d
from pypers.kernel import create_kernel
from pypers.misfit import read_misfit
from pypers.utils.specfem import getsize

from .search import Search


# TODO: rewind compatibility


class Bracket(Search):
    # current iteration
    iteration: int = field()

    # history of misfit values
    misfits: List[float] = field()

    # history of step lengths
    steps: List[float] = field()

    def setup(self):
        self.clear()

        # get last search step
        step_init = self.step_init

        for node in self.parent.parent:
            if isinstance(node, Workspace) and node['search_step'] is not None:
                step_init = cast(float, node['search_step'])

        self.steps = [0.0, step_init]

        # get misfit value from kernel computation
        self.misfits = [read_misfit(self.abs('../kernel'))]

        # add search step
        self.add_step()

        # log misfit value
        d.write(f'Iteration {self.iteration}\nstep 0: {self.steps[0]:.4e} {self.misfits[0]:.4e}\n', 'misfit.log', 'a')
    
    def add_step(self):
        step = len(self.misfits) - 1

        self.add(ws := Workspace(f'step_{step:02d}'))
        ws.add(ws.mkdir)

        # update model
        model = self.abs('../model_init.bp')

        if step == 0:
            mesh = self.abs('../kernel/solver_synthetic/DATABASES_MPI/solver_data.bp')
        
        else:
            mesh = self.abs(f'step_{step-1:02d}/kernel_misfit/solver_synthetic/DATABASES_MPI/solver_data.bp')
        
        cmd = f'{self.abs("../../adios/xupdate_model")} {self.steps[-1]} {model} {mesh} {self.abs("../direction.bp")} .'
        ws.add(partial(ws.mpiexec, cmd, nprocs=getsize()))
        
        # compute misfit
        ws.add(kernel := create_kernel('kernel_misfit', {
            'misfit_only': True, 'path_model': ws.abs('model_gll.bp'), 'path_encoded': self.abs('../kernel/observed.ft.h5')
        }))

        # compute next step
        ws.add(partial(self.bracket, kernel))
    
    def bracket(self, ws: Workspace):
        # update misfit value
        step = len(self.misfits)
        self.misfits.append(read_misfit(ws.abs()))
        d.write(f'step {step}: {self.steps[-1]:.4e} {self.misfits[-1]:.4e}\n', 'misfit.log', 'a')

        x, f = self.get_history()
        alpha = None

        if self.check_bracket(x, f):
            if self.good_enough(x, f):
                step = x[f.argmin()]

                for j, s in enumerate(self.steps):
                    if np.isclose(step, s):
                        d.write(f'new model: step {j}\n\n', 'misfit.log', 'a')
                        self.ln(f'step_{j-1:02d}/model_gll.bp', '../model_new.bp')
                        self.parent['search_step'] = s
                        return
                
            alpha = self.polyfit(x,f)
            
        elif len(self.steps) - 1 < self.nsteps:
            if all(f <= f[0]):
                alpha = 1.618034 * x[-1]
            
            else:
                alpha = x[1] / 1.618034
        
        if alpha:
            self.steps.append(alpha)
            self.add_step()
        
        else:
            raise RuntimeError('line search failed', self.steps, self.misfits)

    def get_history(self):
        """Sort steps."""
        x = np.array(self.steps)
        f = np.array(self.misfits)
        
        f = f[abs(x).argsort()]
        x = x[abs(x).argsort()]
        
        return x, f
    
    def check_bracket(self, x, f):
        """Check history has bracket shape."""
        imin, fmin = f.argmin(), f.min()

        return (fmin < f[0]) and any(f[imin:] > fmin)

    def good_enough(self, x, f):
        """Check current result is good."""
        if not self.check_bracket(x,f):
            return 0

        x0 = self.polyfit(x,f)

        return any(np.abs(np.log10(x[1:]/x0)) < np.log10(1.2))

    def polyfit(self, x, f):
        i = np.argmin(f)
        p = np.polyfit(x[i-1:i+2], f[i-1:i+2], 2)
        
        if p[0] > 0:
            return -p[1]/(2*p[0])
        
        raise RuntimeError('polifit failed', self.steps, self.misfits)
