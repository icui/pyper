from functools import partial
from typing import Optional, Dict, List, cast

import numpy as np

from pypers import cache
from pypers.utils.asdf import asdf_task
from pypers.fwi.process import rotate_frequencies

from .misfit import Misfit, field


class Ortho(Misfit):
    """Source encoded misfit."""
    # weight of phase misfit
    phase_factor: float = field()

    # weighti of amplitude misfit
    amplitude_factor: float = field()

    # frequency slots assigned to events
    fslots: Optional[Dict[str, List[int]]] = field()

    # frequency weights from source normalization
    famp: Optional[np.ndarray] = field()

    # frequency weights from geographical weighting
    gamp: Optional[np.ndarray] = field()

    # station geographical weighting
    samp: Optional[dict] = field()

    # double difference misfit
    double_difference: bool = field(False)

    def setup(self):
        self.clear()

        # avoid amplitude measurement when double-difference is disabled
        if self.amplitude_factor > 0 and not self.double_difference:
            raise RuntimeError('double_difference must be enabled to use amplitude measurement')
        
        # save traces as pickle for better double difference performance
        if self.double_difference:
            self.add(partial(self.mpiexec, self._diff, walltime='encode_observed'))
        
        # compute misfit / adjoint source
        self.add(asdf_task((self.path_synthetic, self.path_observed), self.abs('adjoint.h5'), self._adjoint,
            input_type='auxiliary_group', input_tag='FT', accessor=True,
            output_tag='AdjointSources', walltime='compute_misfit'
        ))
    
    def _diff(self):
        import numpy as np
        from pyasdf import ASDFDataSet

        with ASDFDataSet(self.path_synthetic, mode='r', mpi=False) as syn_ds, \
            ASDFDataSet(self.path_observed, mode='r', mpi=False) as obs_ds:
            fellows = {}

            syn_aux = syn_ds.auxiliary_data.FT
            obs_aux = obs_ds.auxiliary_data.FT
            
            syn_keys = syn_aux.list()
            obs_keys = obs_aux.list()

            for key in syn_keys:
                if key not in obs_keys:
                    continue

                keypath = key.split('_')
                cha = keypath[-1]
                station = '.'.join(keypath[:-1])
                
                if cha not in fellows:
                    fellows[cha] = {}

                # phase and amplitude difference
                syn = np.array(syn_aux[key].data)
                obs = np.array(obs_aux[key].data)

                phase_diff = np.angle(syn / obs)
                amp_diff = np.abs(syn) / np.abs(obs)

                fellows[cha][station] = phase_diff, amp_diff, np.squeeze(np.where(np.isnan(syn) | np.isnan(obs)))
            
            self.dump(fellows, 'fellows.pickle')

    def _adjoint(self, syn_acc, obs_acc):
        from scipy.fftpack import ifft
        from scipy.signal import resample

        station = syn_acc.station
        syn_group = syn_acc.auxiliary_group
        obs_group = obs_acc.auxiliary_group
        
        if self.double_difference and 'fellows' not in cache:
            cache['fellows'] = self.load('fellows.pickle')
        
        fellows = cache.get('fellows')

        # source encoding parameters
        params = syn_group[list(syn_group.keys())[0]].parameters
        dt = params['dt']
        nt = params['nt']
        nt_se = params['nt_se']
        npts = params['npts']
        fidx = params['fidx']

        # frequency domain adjoint sources
        adjs_fd = {}

        # misfit values
        misfits = {}

        for cha in syn_group:
            # phase and amplitude difference
            syn = syn_group[cha].data
            obs = obs_group[cha].data

            # empty slots
            nan = np.squeeze(np.where(np.isnan(syn) | np.isnan(obs)))

            if self.double_difference:
                # sum of phase and amplitude differences
                phase_diff = np.zeros(len(syn))    
                amp_diff = np.zeros(len(syn))

                # misfit of current station
                phase_diff1, amp_diff1, _ = fellows[cha][station]

                # sum double difference measurements
                for station2, (phase_diff2, amp_diff2, nan2) in fellows[cha].items():
                    if station2 == station:
                        continue
                    
                    # double difference misfit
                    phase_dd = np.sin(phase_diff1 - phase_diff2)
                    phase_dd[nan2] = 0.0

                    amp_dd = np.log(amp_diff1 / amp_diff2)
                    amp_dd[nan2] = 0.0

                    # station geographical weighting
                    if self.samp is not None and station2 in self.samp:
                        phase_dd *= self.samp[station2]
                        amp_dd *= self.samp[station2]

                    phase_diff += phase_dd
                    amp_diff += amp_dd
            
            else:
                # single difference measurements
                phase_diff = np.angle(syn / obs_group[cha].data)
                amp_diff = np.zeros(len(syn))

            # apply measurement weightings
            phase_diff *= self.phase_factor
            amp_diff *= self.amplitude_factor

            # misfit values and adjoint sources
            phase_mf = np.nansum(phase_diff ** 2)
            phase_adj = phase_diff * (1j * syn) / abs(syn) ** 2
            phase_adj[nan] = 0.0

            amp_mf = np.nansum(amp_diff ** 2)
            amp_adj = amp_diff * syn / abs(syn) ** 2
            amp_adj[nan] = 0.0

            # fourier transform of adjoint source time function
            ft_adj = phase_adj + amp_adj
            misfits[cha] = np.zeros(0), phase_mf + amp_mf

            # amplify high frequencies (compensate for attenuation)
            if self.famp is not None:
                ft_adj *= self.famp
            
            # event geographical weighting
            if self.gamp is not None:
                ft_adj *= self.gamp
                
            # station geographical weighting
            if self.samp is not None and station in self.samp:
                ft_adj *= self.samp[station]
            
            adjs_fd[cha[-1]] = ft_adj

        if self.misfit_only:
            return misfits
        
        if 'MXR' in syn_group or 'MXT' in syn_group:
            if self.fslots is None:
                raise RuntimeError('unable to rotate because frequency slots are missing')
            
            adjs_fd = rotate_frequencies(adjs_fd, self.fslots, params, station, syn_acc.inventory)
        
        # (fake) rotate misfit values
        if 'MXR' in misfits:
            misfits['MXN'] = misfits['MXR']
        
        if 'MXT' in misfits:
            misfits['MXE'] = misfits['MXT']

        # time domain adjoint sources
        adjs = {}

        for cmp, ft_tau in adjs_fd.items():
            # fill full frequency band
            ft_adstf = np.zeros(nt_se, dtype=complex)
            ft_adstf[fidx[0]: fidx[-1]] = ft_tau
            ft_adstf[-fidx[0]: -fidx[-1]: -1] = np.conj(ft_tau)

            # stationary adjoint source
            adstf_tau = ifft(ft_adstf).real

            # repeat to fill entrie adjoint duration
            adstf_tile = np.tile(adstf_tau, int(np.ceil(nt / nt_se)))
            adstf = adstf_tile[-nt:]
            self.apply_taper(adstf,dt)

            if npts != nt:
                adstf = cast(np.ndarray, resample(adstf, num=npts))
            
            adjs[f'MX{cmp}'] = adstf, {'misfit': misfits[f'MX{cmp}'][1], **params}

        return adjs
