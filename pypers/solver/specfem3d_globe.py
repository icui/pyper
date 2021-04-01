from functools import partial

from pypers import Directory, field, getpath, cache
from pypers.utils.asdf import asdf_task
from pypers.utils.specfem import probe_mesher, probe_solver, probe_smoother, getsize, setpars, Par_file

from .solver import Solver


class Specfem3D_Globe(Solver):
    # use LDDRK time scheme
    lddrk: bool = field(False)

    def setup(self):
        self.clear()

        # number of processors to use
        nprocs = getsize()

        if self.path_forward:
            # prepare adjoint simulation
            self.add(self._setup_adjoint)

            # call solver
            self.add(partial(self.mpiexec, 'bin/xspecfem3D', nprocs, 1, 1, 'solver_adjoint', True), prober=partial(probe_solver, self))

            # move OUTPUT_FILES/kernels.bp to kernels_raw.bp
            self.add(partial(self.mv, 'OUTPUT_FILES/kernels.bp', 'kernels_raw.bp'), 'move_kernels')

            # smooth kernels / hessian
            if self.smooth_kernels:
                self.add(partial(self._apply_smoother, False), 'smooth_kernels', prober=partial(probe_smoother, self, False))
            
            if self.smooth_hessian:
                self.add(partial(self._apply_smoother, True), 'smooth_hessian', prober=partial(probe_smoother, self, True))
            
            # apply preconditioner
            if self.precondition:
                self.add(self._apply_preconditioner)
            
            # link final kernels to kernels.bp
            self.add(self._finalize_adjoint)

        else:
            # prepare forward simulation
            self.add(self._setup_forward)

            # call mesher and solver
            self.add(partial(self.mpiexec, 'bin/xmeshfem3D', nprocs, 1, 0, 'mesher'), prober=partial(probe_mesher, self))
            self.add(partial(self.mpiexec, 'bin/xspecfem3D', nprocs, 1, 1, 'solver_forward', True), prober=partial(probe_solver, self))

            # move OUTPUT_FILES/synthetic.h5 to traces_raw.h5
            self.add(partial(self.mv, 'OUTPUT_FILES/synthetic.h5', 'traces_raw.h5'), 'move_traces')

            # process traces
            if self.process_traces:
                if 'dst' in self.process_traces:
                    self.add(asdf_task(self.abs('traces_raw.h5'), **self.process_traces))
                
                else:
                    self.add(asdf_task(self.abs('traces_raw.h5'), self.abs('traces_proc.h5'), **self.process_traces))

            # link final traces to traces.h5
            self.add(self._finalize_forward)
    
    def _setup_forward(self):
        # specfem directory warpper
        d = Directory(getpath('specfem'))

        # setup specfem workspace
        self.mkdir('DATA')
        self.mkdir('OUTPUT_FILES')
        self.mkdir('DATABASES_MPI')

        self.ln(d.abs('bin'))
        self.cp(d.abs('DATA/Par_file'), 'DATA')
        self.cp(self.path_event if self.path_event else d.abs('DATA/CMTSOLUTION'), 'DATA/CMTSOLUTION')
        self.cp(self.path_stations if self.path_stations else d.abs('DATA/STATIONS'), 'DATA/STATIONS')

        # link model
        for subdir in d.ls('DATA', isdir=True):
            if subdir != 'GLL' or not self.path_model:
                self.ln(d.abs('DATA', subdir), 'DATA')
            
        if self.path_model:
            self.mkdir('DATA/GLL')
            self.ln(self.path_model, 'DATA/GLL/model_gll.bp')

        # update Par_file
        pars: Par_file = {'SIMULATION_TYPE': 1}

        if self.save_forward is not None:
            pars['SAVE_FORWARD'] = self.save_forward
        
        if self.monochromatic_source is not None:
            pars['USE_MONOCHROMATIC_CMT_SOURCE'] = self.monochromatic_source
        
        if self.duration is not None:
            pars['RECORD_LENGTH_IN_MINUTES'] = self.duration
        
        if self.path_model is not None:
            pars['MODEL'] = 'GLL'
        
        if self.lddrk is not None:
            pars['USE_LDDRK'] = self.lddrk

        if self.transient_duration is not None:
            if self.duration is None:
                raise ValueError('solver duration must be set if transient_duration exists')

            pars['STEADY_STATE_KERNEL'] = True
            pars['STEADY_STATE_LENGTH_IN_MINUTES'] = self.duration - self.transient_duration
        
        else:
            pars['STEADY_STATE_KERNEL'] = False

        setpars(self, pars)
    
    def _setup_adjoint(self):
        # specfem directory for forward simulation
        if not self.path_forward:
            raise ValueError('forward solver not specified')
        
        if not self.path_adjoint:
            raise ValueError('adjoint source not specified')

        d = Directory(self.path_forward)

        # update and Par_file
        self.mkdir('SEM')
        self.mkdir('DATA')
        self.mkdir('OUTPUT_FILES')
        
        self.ln(d.abs('bin'))
        self.ln(d.abs('DATABASES_MPI'))
        self.ln(d.abs('DATA/*'), 'DATA')
        self.rm('DATA/Par_file')
        self.cp(d.abs('DATA/Par_file'), 'DATA')

        self.ln(self.path_adjoint, 'SEM/adjoint.h5')
        self.cp('DATA/STATIONS', 'DATA/STATIONS_ADJOINT')

        setpars(self, {'SIMULATION_TYPE': 3, 'SAVE_FORWARD': False})

    async def _apply_smoother(self, hess: bool):
        import adios2

        names = []

        if hess and self.precondition:
            src = 'hess_vel_raw.bp'
            dst = 'hess_vel_smooth.bp'
            cmd = f'xconvert_hessian kernels_raw.bp DATABASES_MPI/solver_data.bp hess_vel_raw.bp {self.precondition}'
            
            await self.mpiexec(getpath('adios', 'bin', cmd), getsize(self), 1, 0, 'adios')
        
        else:
            src = 'kernels_raw.bp'
            dst = 'hessian_smooth.bp' if hess else 'kernels_smooth.bp'
        
        # get the names of the kernels to be smoothed
        with adios2.open(self.abs(src), 'r') as fh: # type: ignore
            pf = '_crust_mantle/array'

            for fstep in fh:
                step_vars = fstep.available_variables()

                for name in step_vars:
                    if name.endswith(pf):
                        name = name.split(pf)[0]
                        
                        if name.startswith('hess_'):
                            if hess:
                                names.append(name)
                        
                        else:
                            if not hess:
                                names.append(name)

        # save the number of kernels being smoothed for probe_smoother
        kind = 'smooth_' + ('hess' if hess else 'kl')
        cache[kind] = len(names)

        # get the command to call smoother
        cmd = 'bin/xsmooth_laplacian_sem_adios'
        radius = self.smooth_hessian if hess else self.smooth_kernels

        if isinstance(radius, list):
            radius = max(radius[1], radius[0] * radius[2] ** self.iteration)

        kl = ','.join(names)

        await self.mpiexec(f'{cmd} {radius} {radius} {kl} {src} DATABASES_MPI/ {dst} > OUTPUT_FILES/{kind}.txt',
            getsize(self), 1, 0, 'smooth_kernels')
        
        # reset status
        del cache[kind]

    async def _apply_preconditioner(self):
        kl = 'kernels_smooth.bp' if self.smooth_kernels else 'kernels_raw.bp'
        hess = 'hess_vel_smooth.bp' if self.smooth_hessian else 'kernels_raw.bp'
        cmd = f'xprecond_kernels {kl} {hess} kernels_precond.bp {self.precondition}'

        await self.mpiexec(getpath('adios', 'bin', cmd), getsize(self), 1, 0, 'adios')
    
    def _finalize_forward(self):
        if self.process_traces:
            src = self.process_traces.get('dst') or 'traces_proc.h5'
        
        else:
            src = 'traces_raw.h5'
        
        self.ln(src, 'traces.h5')
    
    def _finalize_adjoint(self):
        if self.precondition:
            src = 'kernels_precond.bp'
        
        elif self.smooth_kernels:
            src = 'kernels_smooth.bp'
        
        else:
            src = 'kernels_raw.bp'
        
        self.ln(src, 'kernels.bp')
        
