from __future__ import annotations

from sys import stderr
from typing import Optional, List, Dict, Tuple, TYPE_CHECKING

from obspy import Stream, Trace, Inventory
from asdfy import ASDFAccessor
from scipy.fftpack import fft, ifft
from pytomo3d.signal.process import flex_cut_stream, rotate_stream

if TYPE_CHECKING:
    import numpy as np


def detrend(stream: Stream, taper: Optional[float] = None):
    """Detrend and taper."""
    stream.detrend('linear')
    stream.detrend('demean')

    if taper:
        stream.taper(max_percentage=None, max_length=taper*60)


def select(acc: ASDFAccessor, duration: Optional[float] = None):
    """Select 3 components from Stream."""
    stream = acc.stream

    # cut stream based on event time
    if duration:
        starttime = acc.origin.time
        endtime = starttime + duration * 60
        stream = flex_cut_stream(stream, starttime, endtime, 10)

    # select 3 components from the stream
    for trace_z in stream.select(component='Z'):
        for cmps in [['N', 'E'], ['1', '2']]:
            traces = [trace_z]

            for cmp in cmps:
                if len(substream := stream.select(component=cmp, location=trace_z.stats.location)):
                    traces.append(substream[0])
            
            if len(traces) == 3:
                return Stream(traces)


def rotate_frequencies(group: Dict[str, np.ndarray], fslots: Dict[str, List[int]], parameters: dict, station: str, inv: Inventory):
    import numpy as np
    
    from pypers.fwi.catalog import locate_events

    # unpack parameters
    nt_se = parameters['nt_se']
    fidx = parameters['fidx']
    nf = fidx[-1] - fidx[0]
    net, sta = station.split('.')

    # read event locations
    event_loc = locate_events()

    # output rotated frequencies
    group_rotated: Dict[str, np.ndarray] = {}

    if 'R' in group or 'T' in group:
        mode = 'RT->NE'
        cmps = 'N', 'E', 'Z'

    else:
        mode = 'NE->RT'
        cmps = 'R', 'T', 'Z'
    
    for cmp in cmps:
        group_rotated[cmp] = np.zeros(nf, dtype=complex)

    for event, slots in fslots.items():
        if len(slots) == 0:
            continue
        
        lat, lon = event_loc[event]
        
        # time domain traces of current event
        traces = []

        for cmp, data in group.items():
            data_partial = np.zeros(len(data), dtype=complex)
            data_partial[slots] = data[slots]

            data_full = np.zeros(nt_se, dtype=complex)
            data_full[fidx[0]: fidx[-1]] = data_partial
            data_full[-fidx[0]: -fidx[-1]: -1] = np.conj(data_partial)

            traces.append(Trace(ifft(data_full).real, {
                'component': cmp, 'channel': f'MX{cmp}', 'delta': parameters['dt'], 'network': net, 'station': sta, 'location': 'S3'
            }))

        # rotate in time domain
        stream = rotate_stream(Stream(traces), lat, lon, inv, mode=mode)

        # transform back to frequency domain
        for cmp in cmps:
            data = stream.select(component=cmp)[0].data # type: ignore
            data = fft(data)[fidx[0]: fidx[-1]]
            group_rotated[cmp][slots] = data[slots]
    
    return group_rotated


def process(acc: ASDFAccessor,
    duration: Optional[float] = None, nt: Optional[int] = None, dt: Optional[float] = None,
    taper: Optional[float] = None, remove_response: bool = False, rotate: bool = False):
    """Process observed stream."""
    try:
        if (stream := select(acc, duration)) is None:
            return
        
        origin = acc.origin

        # remove instrument response
        if remove_response:
            detrend(stream, taper)
            stream.attach_response(acc.inventory)
            stream.remove_response(output="DISP", zero_mean=False, taper=False)

        # resample and align
        if dt:
            if nt:
                stream.interpolate(1/dt, starttime=origin.time)
            
            else:
                stream.resample(sampling_rate=1/dt)
        
        # detrend and apply taper
        detrend(stream, taper)
        
        # rotate
        if rotate:
            stream = rotate_stream(stream, origin.latitude, origin.longitude, acc.inventory)

            if len(stream) != 3:
                return
            
            for cmp in ['R', 'T', 'Z']:
                if len(stream.select(component=cmp)) != 1:
                    return

        return stream
        
    except Exception as e:
        print(e, file=stderr)


def get_distance(acc: ASDFAccessor):
    """Get the distance between event and station."""
    from obspy.geodetics import locations2degrees

    o = acc.origin
    s = acc.inventory[0][0]

    return locations2degrees(o.latitude, o.longitude, s.latitude, s.longitude)
