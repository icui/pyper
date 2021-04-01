from functools import partial
from typing import Optional

import numpy as np

from pypers import Workspace
from pypers.fwi.catalog import get_events, get_stations, locate_events, locate_stations


def _save_locations(ws: Workspace):
    """Save the locations of events and stations for weight computation."""
    event_loc = locate_events()
    station_loc = locate_stations()
    
    ws.dump(event_loc, 'locations/event.pickle')

    for event in get_events():
        loc = {}

        for station in get_stations(event):
            loc[station] = station_loc[station]
        
        ws.dump(loc, f'locations/station.{event}.pickle')

    ws.mkdir('weightings')


def _compute_weightings(ws: Workspace, target: str, percentage: float):
    from obspy.geodetics import locations2degrees

    locations = ws.load(f'locations/{target}.pickle')
    npts = len(locations)
    dists = np.zeros([npts, npts])

    # compute weight in current rank
    for i, l1 in enumerate(locations.values()):
        for j, l2 in enumerate(locations.values()):
            if i != j:
                dists[i, j] = dists[j, i] = locations2degrees(l1[0], l1[1], l2[0], l2[1])
    
    # search for optimal condition number
    ref_dists = np.linspace(1, 100, 100)
    conds = []
    arrs = []

    for ref_dist in ref_dists:
        dists_exp = np.exp(-(dists / ref_dist) ** 2)
        arr = 1 / np.sum(dists_exp, axis=1)
        arr /= np.sum(arr) / len(arr)
        
        conds.append(cond := arr.max() / arr.min())
        arrs.append(arr)

        if cond <= 0.8 * max(conds):
            break
    
    for i in range(len(conds)):
        if conds[i] >= percentage * max(conds):
            print(target, conds[i], min(arrs[i]), max(arrs[i]))

            weightings = {}

            for j, station in enumerate(locations.keys()):
                weightings[station] = arrs[i][j]
            
            ws.dump(weightings, f'weightings/{target}.pickle')
            return
    
    raise RuntimeError(f'failed to obtain condition number for {target} {conds}')


def compute_weightings(event_weighting: Optional[float], station_weighting: Optional[float], dst: str):
    """Compute geographical weightings."""
    ws = Workspace('compute_weightings')

    # save location
    ws.add(partial(_save_locations, ws))

    # compute event weightings
    if event_weighting:
        func = partial(_compute_weightings, ws, 'event', event_weighting)
        ws.add(partial(ws.mpiexec, func, walltime='compute_weightings'), 'event_weightings')

    # compute station weightings
    ws.add(subws := Workspace('station_weightings', concurrent=True))

    for event in get_events():
        func = partial(_compute_weightings, ws, f'station.{event}', station_weighting)
        subws.add(partial(ws.mpiexec, func, walltime='compute_weightings'), event)

    # move results to catalog director
    ws.add(partial(ws.mv, 'weightings', dst), 'export_result')

    return ws
