from functools import partial
import json

import numpy as np
from asdfy import ASDFAccessor
from scipy.fftpack import fft, fftfreq

from pypers import Workspace, getpath, getcfg, basedir as d
from pypers.utils.asdf import asdf_task

from .process import process_observed, process_synthetic


dt = getcfg('workspace', 'dt')
period_range = getcfg('workspace', 'period_range')


def pick(event: str, obs_acc: ASDFAccessor, syn_acc: ASDFAccessor):
    obs_stream = process_observed(obs_acc)
    syn_stream = process_synthetic(syn_acc)
    
    if obs_stream and syn_stream:
        selected = []

        for cmp in ['R', 'T', 'Z']:
            obs = obs_stream.select(component=cmp)[0]
            syn = syn_stream.select(component=cmp)[0]

            freq = fftfreq(len(obs), dt)
            fwin = np.where((freq < 1 / period_range[0]) & (freq > 1 / period_range[-1]))

            ft_obs = fft(obs.data)[fwin]
            ft_syn = fft(syn.data)[fwin]

            if np.std(np.angle(ft_obs / ft_syn)) < 0.5:
                selected.append(cmp)
        
        if len(selected):
            with open(f'picked/{event}/{obs_acc.key[2]}.json', 'w') as f:
                json.dump(selected, f)


def gather():
    p = d.subdir('picked')
    catalog = {}
    
    for event in p.ls():
        s = p.subdir(event)

        if len(stations := s.ls()) > 10:
            catalog[event] = {}

            for station in stations:
                with open(s.abs(station), 'r') as f:
                    catalog[event][station.split('.json')[0]] = [json.load(f)]
            
    d.dump(catalog, 'catalog.toml')


def pick_catalog():
    """Process observed traces in catalog."""
    ws = Workspace()
    ws.rm('picked')

    obs = getpath('obs')
    syn = getpath('syn')

    if obs and syn:
        obsdir = ws.subdir(obs)
        syndir = ws.subdir(syn)

        for trace in obsdir.ls():
            event = trace.split('.')[0]
            
            ws.mkdir(f'picked/{event}')

            ws.add(asdf_task(
                (obsdir.abs(trace), syndir.abs(syndir.ls(grep=f'{event}.*')[0])), None, partial(pick, event),
                input_type='stream', accessor=True, name=event
            ))

    return ws
