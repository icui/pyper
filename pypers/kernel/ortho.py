from __future__ import annotations

from sys import stderr
from random import seed, sample
from functools import partial
from typing import List, Dict, Optional, Union, TYPE_CHECKING

import numpy as np
from scipy.fftpack import fft, fftfreq

from pypers import Workspace, field, getpath
from pypers.fwi import catalogdir, get_catalog, get_events, is_rotated, process, has_station
from pypers.fwi.process import rotate_frequencies
from pypers.fwi.weightings import compute_weightings
from pypers.solver import create_solver
from pypers.misfit import create_misfit
from pypers.utils.asdf import asdf_task
from pypers.utils.specfem import merge_stations

from .kernel import Kernel

if TYPE_CHECKING:
    from asdfy import ASDFAccessor


class Ortho(Kernel):
    # taper traces
    taper: Optional[float] = field()

    # time step for frequency selection
    dt: float = field(required=True)

    # simulation duration
    duration: float = field(required=True)
    
    # transient state duration for source encoding
    transient_duration: float = field(required=True)

    # normalize source to the magnitude
    normalize_source: bool = field(True)

    # remove response for observed traces
    remove_response: bool = field(False)

    # test orthogonality (1: synthetic encoded traces 2: synthetic observed traces)
    test_encoding: int = field(0)

    # path to encoded observed traces
    path_encoded: Optional[str] = field()

    # event condition number geographical weighting
    event_weighting: Optional[float] = field()

    # station condition number geographical weighting
    station_weighting: Optional[float] = field()

    # amplify high frequency kernels to compensate for attenuation
    compensate_attenuation: Optional[Union[int, float]] = field()

    # randomize frequency every x iteration, 0 for not randomizing frequency
    randomize_frequency: int = field(1)

    # frequency interval
    df: float = field()

    # frequency step for observed traces
    kf: int = field(1)

    # frequency indices used for encoding
    fidx: List[int] = field()

    # frequency slots assigned to events
    fslots: Dict[str, List[int]] = field()

    # number of time steps in transient state
    nt_ts: int = field()

    # number of time steps in stationary state
    nt_se: int = field()

    # frequency weights from source normalization
    famp: Optional[np.ndarray] = field()

    # frequency weights from geographical weighting
    gamp: Optional[np.ndarray] = field()

    # station geographical weighting
    samp: Optional[dict] = field()

    @property
    def freq(self):
        """Frequencies used for encoding."""
        return fftfreq(self.nt_se, self.dt)[self.fidx[0]: self.fidx[-1]]
    
    @property
    def freqstr(self):
        """String representation of frequency band."""
        period = f'p{int(self.period_range[0])}-{int(self.period_range[-1])}'
        duration = f'd{int(self.transient_duration)}-{int(self.duration)}'
        dt = f't{self.dt}'
        seed = f's{self.rng}'

        return f'{period}_{duration}_{dt}_{seed}'
    
    @property
    def rng(self):
        return int(self.iteration / self.randomize_frequency) + self.seed
    
    @property
    def ampstr(self):
        """String representation of geographical weightings."""
        return f'weightings_{self.event_weighting}_{self.station_weighting}'

    def setup(self):
        self.clear()

        # add steps to compute and process adjoint sources
        self.add(self._prepare_frequencies)

        # create super source
        self.add(self._encode_events)

        # compute weighting
        if self.event_weighting or self.station_weighting:
            if not catalogdir.has(self.ampstr):
                self.add(compute_weightings(self.event_weighting, self.station_weighting, catalogdir.abs(self.ampstr)))
            
            self.add(self._load_weightings)

        if self.path_encoded:
            self.add(partial(self.ln, self.path_encoded, 'observed.ft.h5'), 'link_observed')
        
        elif self.test_encoding == 1:
            # generate observed traces
            self.add(solver := create_solver('solver_observed', {
                'path_event': self.abs('SUPERSOURCE'),
                'path_stations': self.abs('SUPERSTATION'),
                'path_model': getpath('model_true'),
                'monochromatic_source': True,
                'save_forward': False,
                'process_traces': {
                    'dst': self.abs('observed.ft.h5'),
                    'func': partial(self._ft, None),
                    'input_type': 'stream',
                    'output_tag': 'FT',
                    'accessor': True
                }
            }))
        
        else:
            # prepare observed data and save to catalog directory
            ws = Workspace('prepare_observed', concurrent=True)

            for event in get_events():
                # location of processed traces
                if catalogdir.has(fname := f'{self.freqstr}/{event}.ft.h5'):
                    continue
            
                dst = catalogdir.abs(fname)

                # taks name
                name = f'process_{event}'

                if not catalogdir.has(f'traces/{event}.h5'):
                    # generate and process observed data
                    ws.add(solver := create_solver(f'solver_{event}', {
                        'path_event': catalogdir.abs(f'events/{event}'),
                        'path_stations': catalogdir.abs(f'stations/STATIONS.{event}'),
                        'path_model': getpath('model_true'),
                        'monochromatic_source': False,
                        'save_forward': False,
                        'process_traces': {
                            'dst': dst,
                            'func': partial(self._ft, event),
                            'input_type': 'stream',
                            'output_tag': 'FT',
                            'accessor': True
                        }
                    }))
            
                else:
                    # process observed data
                    ws.add(asdf_task(
                        catalogdir.abs(f'traces/{event}.h5'), dst, partial(self._ft, event),
                        input_type='stream', output_tag='FT', accessor=True, name=name
                    ))
            
            if len(ws):
                self.add(ws)

            # get Fourier coefficients from observed traces
            self.add(partial(self.mpiexec, self._encode_observed, walltime='encode_observed'))
        
        # generate synthetic traces
        self.add(solver := create_solver('solver_synthetic', {
            'path_event': self.abs('SUPERSOURCE'),
            'path_stations': self.abs('SUPERSTATION'),
            'path_model': self.path_model or getpath('model_init'),
            'monochromatic_source': True,
            'save_forward': True,
            'process_traces': {
                'dst': self.abs('synthetic.ft.h5'),
                'func': partial(self._ft, None),
                'input_type': 'stream',
                'accessor': True,
                'output_tag': 'FT'
            }
        }))

        # compare traces only if test_encoding == 2
        if self.test_encoding == 2:
            return

        # compute misfit and adjoint sources
        self.add(misfit := create_misfit('misfit', {
            'path_observed': self.abs('observed.ft.h5'), 'path_synthetic': self.abs('synthetic.ft.h5')
        }))

        self.add(partial(self.ln, misfit.abs('adjoint.h5')), 'link_misfit')
        
        # compute kernels
        if not self.misfit_only:
            self.add(solver := create_solver('solver_adjoint', {
                'path_forward': solver.abs(), 'path_adjoint': misfit.abs('adjoint.h5')
            }))

            self.add(partial(self.ln, solver.abs('kernels.bp')), 'link_kernels')
    
    def _prepare_frequencies(self):
        """Prepare frequencies and extract frequency components of observed traces."""
        from math import ceil

        if self.fidx:
            return

        if self.duration <= self.transient_duration:
            raise ValueError('duration should be larger than transient_duration')

        # number of time steps to reach steady state
        nt_ts = int(round(self.transient_duration * 60 / self.dt))

        # number of time steps after steady state
        nt_se = int(round((self.duration - self.transient_duration) * 60 / self.dt))

        # frequency step
        df = 1 / nt_se / self.dt

        # factor for observed data
        kf = int(ceil(nt_ts / nt_se))

        # frequencies to be encoded
        freq = fftfreq(nt_se, self.dt)

        fidx = list(int(np.round(1 / period / df)) for period in self.period_range)
        fidx.reverse()
        
        while 1 / freq[fidx[0]] > self.period_range[-1]:
            fidx[0] += 1 # type: ignore
        
        while 1 / freq[fidx[-1]] < self.period_range[0]:
            fidx[-1] -= 1
        
        fidx[-1] += 1

        # save and print source encoding parameters
        self.nt_ts = nt_ts
        self.nt_se = nt_se
        self.df = df
        self.kf = kf
        self.fidx = fidx

        self.write('\n'.join([
            f'time step length: {self.dt}',
            f'frequency step length: {self.df:.2e}',
            f'transient state duration: {nt_ts * self.dt / 60:.2f}min',
            f'transient state time step: {nt_ts}',
            f'steady state duration: {nt_se * self.dt / 60:.2f}min',
            f'steady state time step: {nt_se}',
            f'frequency slots: {fidx[-1]-fidx[0]}', # type: ignore
            f'period range: {", ".join(f"{1/freq[i if i == fidx[0] else i - 1]:.2f}s" for i in fidx)}',
            f'frequency indices: {fidx} {kf}',
            ''
        ]), 'encoding.log')
    
    async def _encode_events(self):
        """Generate super source."""
        # load catalog
        cmt = ''

        # merge stations into a single station file
        merge_stations(catalogdir.subdir('stations'), self.abs('SUPERSTATION'), get_catalog())

        # randomize frequency
        freq = self.freq
        fslots = self.fslots = {}

        if self.randomize_frequency:
            seed(self.rng)

        # encode events
        for group in range(len(self.fidx) - 1):
            # get events and frequency indices in current frequency group
            events = get_events(group)
            nfreq = self.fidx[group + 1] - self.fidx[group]
            imin = self.fidx[group] - self.fidx[0]

            # randomly assign frequencies to events
            if self.randomize_frequency:
                rdm = sample(range(nfreq), nfreq)
            
            else:
                rdm = list(range(nfreq))

            for ievt, event in enumerate(events):
                # add to super source
                lines = catalogdir.readlines(f'events/{event}')

                if event not in fslots:
                    fslots[event] = []

                for ifreq in range(ievt, nfreq, len(events)):
                    # save local indices to fslots
                    idx = imin + rdm[ifreq]
                    fslots[event].append(idx)
                    f0 = freq[idx]
                    lines[2] = 'time shift:           0.0000'
                    lines[3] = f'half duration:{" " * (9 - len(str(int(1 / f0))))}{1/f0:.4f}'

                    # reference moment tensor
                    if self.normalize_source:
                        mref = 1e25
                        mmax = max(abs(float(lines[i].split()[-1])) for i in range(7, 13))
                        
                        # amplification factor of forward source
                        for j in range(7, 13):
                            line = lines[j].split()
                            line[-1] = f'{(float(line[-1]) * mref / mmax):.6e}'
                            lines[j] = '           '.join(line)

                    cmt += '\n'.join(lines)

                    if cmt[-1] != '\n':
                        cmt += '\n'
        
        # amplify high frequency component
        if self.compensate_attenuation:
            self.famp = (freq / freq[0]) ** self.compensate_attenuation
        
        self.write(cmt, 'SUPERSOURCE')
    
    def _load_weightings(self):
        nf = len(self.freq)

        if self.event_weighting:
            self.gamp = np.zeros(nf)
        
        if self.station_weighting:
            self.samp = {}
        
        event_weightings = catalogdir.load(f'{self.ampstr}/event.pickle')
        
        for event in self.fslots:
            if self.event_weighting:
                for idx in self.fslots[event]:
                    self.gamp[idx] = event_weightings[event]

            if self.station_weighting:
                station_weightings = catalogdir.load(f'{self.ampstr}/station.{event}.pickle')

                for idx in self.fslots[event]:
                    for station in station_weightings:
                        if station not in self.samp:
                            self.samp[station] = np.zeros(nf)
                        
                        self.samp[station][idx] = station_weightings[station]

    async def _encode_observed(self):
        """Prepare observed frequencies."""
        import cmath
        from pyasdf import ASDFDataSet

        # load catalog
        catalog = get_catalog()
        components = ['R', 'T', 'Z'] if is_rotated() else ['N', 'E', 'Z']

        # time array for STF
        nt = self.kf * self.nt_se
        t = np.linspace(0, (nt - 1) * self.dt, nt)

        # encoded traces
        encoded = {}
        freq = self.freq

        for event, slots in self.fslots.items():
            # add empty traces to fill
            for station in catalog[event]:
                for cmp in components:
                    sta = station.replace('.', '_') + '_MX' + cmp

                    if sta not in encoded:
                        encoded[sta] = np.full(len(freq), np.nan, dtype=complex)
            
            with ASDFDataSet(catalogdir.abs(f'{self.freqstr}/{event}.ft.h5'), mode='r', mpi=False) as ds:
                # start time of seismogram relative to event origin time
                traces = ds.auxiliary_data.FT.list()
                hdur = ds.events[0].focal_mechanisms[0].moment_tensor.source_time_function.duration / 2
                tshift = 1.5 * hdur

                # source time function of observed data and its frequency component
                stf = np.exp(-((t - tshift) / (hdur / 1.628)) ** 2) / np.sqrt(np.pi * (hdur / 1.628) ** 2)
                sff = self._ft_obs(stf)

                # record frequency components
                for idx in slots:
                    # get the group of current frequency
                    for group in range(len(self.fidx) - 1):
                        imin = self.fidx[group] - self.fidx[0]
                        imax = self.fidx[group + 1] - self.fidx[0]

                        if imin <= idx < imax:
                            break

                    f0 = freq[idx]

                    # phase shift due to the measurement of observed data
                    pshift = cmath.exp(2 * np.pi * 1j * f0 * (self.nt_ts * self.dt - tshift)) / sff[idx]

                    # loop over stations
                    for station in catalog[event]:
                        for cmp in catalog[event][station][group]: # type:ignore
                            sta = station.replace('.', '_') + '_MX' + cmp

                            if sta not in traces:
                                continue

                            if not np.isnan(encoded[sta][idx]):
                                raise RuntimeError(f'duplicate frequency slot {idx}')

                            encoded[sta][idx] = ds.auxiliary_data.FT[sta].data[idx] * pshift

        # save encoded data
        with ASDFDataSet(self.abs('observed.ft.h5'), mode='w', mpi=False) as ds:
            for sta, data in encoded.items():
                ds.add_auxiliary_data(data, 'FT', sta, {'df': self.df, 'fmin': freq[0], 'fmax': freq[-1]})
    
    def _ft_syn(self, data: np.ndarray):
        return fft(data[self.nt_ts: self.nt_ts + self.nt_se])[self.fidx[0]: self.fidx[-1]]
    
    def _ft_obs(self, data: np.ndarray):
        if (nt := self.kf * self.nt_se) > len(data):
            # expand observed data with zeros
            data = np.concatenate([data, np.zeros(nt - len(data))]) # type: ignore
        
        else:
            data = data[:nt]
        
        return fft(data)[::self.kf][self.fidx[0]: self.fidx[-1]]
    
    def _ft(self, event: Optional[str], acc: ASDFAccessor):
        output = {}
        station = acc.station

        # ignore stations that are not selected
        if event and station and not has_station(event, station):
            print('skipped', event, station)
            return

        # save the stats of original trace
        stats = acc.stream[0].stats # type: ignore
        npts = stats.npts
        delta = stats.delta

        # process stream
        stream = process(acc,
            duration=self.transient_duration if event else None,
            nt=self.nt_ts if event else None,
            dt=self.dt,
            taper=self.taper if event else None,
            remove_response=event is not None and self.remove_response,
            rotate=event is not None and is_rotated()
        )

        if stream is None:
            args = ['failed to process stream', station]
            
            if event:
                args.append(get_catalog()[event][station])

            print(*args)
            return

        # Time and frequency parameters
        params = {
            'npts': npts,
            'delta': delta,
            'nt': len(stream[0].data), # type: ignore
            'dt': self.dt,
            'nt_ts': self.nt_ts,
            'nt_se': self.nt_se,
            'fidx': self.fidx
        }

        # FFT
        if event is None and is_rotated():
            if (inv := acc.inventory) is None or station is None:
                return
            
            output_nez = {}

            for trace in stream:
                output_nez[trace.stats.component] = self._ft_syn(trace.data)

            # rotate frequencies
            output_rtz = rotate_frequencies(output_nez, self.fslots, params, station, inv)
            output = {}

            for cmp, data in output_rtz.items():
                output[f'MX{cmp}'] = data, params
        
        else:
            for trace in stream:
                data = self._ft_obs(trace.data) if event else self._ft_syn(trace.data)
                output[f'MX{trace.stats.component}'] = data, params

        return output
