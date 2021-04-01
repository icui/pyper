from typing import Optional, Dict, Tuple

from pypers import Directory, getpath, cache


# directory containing catalog files
catalogdir = Directory(getpath('catalog'))


def get_catalog() -> dict:
    """Load catalog based on config.toml."""
    if 'catalog' not in cache:
        cache['catalog'] = catalogdir.load('catalog.toml')
    
    return cache['catalog']


def get_stations(event: str, group: Optional[int] = None):
    """Get available stations in current frequency group."""
    catalog = get_catalog()

    if group is None:
        return list(catalog[event].keys())

    stations = []

    for station, traces in catalog[event].items():
        if len(traces[group]) > 0:
            stations.append(station)
    
    return stations


def get_events(group: Optional[int] = None):
    """Get available events in current frequency group."""
    catalog = get_catalog()
    events = []

    for event in catalog:
        if isinstance(catalog[event], dict):
            if group is None:
                events.append(event)
            
            else:
                for traces in catalog[event].values():
                    if len(traces[group]) > 0:
                        events.append(event)
                        break
    
    return events


def has_station(event: str, station: str):
    """Determines whether a station is used."""
    return station in get_catalog()[event]


def locate_events() -> Dict[str, Tuple[float, float]]:
    """Get event latitudes and longitides."""
    if 'event_locations' in cache:
        event_loc = cache['event_locations']
    
    else:
        event_loc = cache['event_locations'] = {}

        for event in get_events():
            lines = catalogdir.readlines(f'events/{event}')
            
            lat = float(lines[4].split()[-1])
            lon = float(lines[5].split()[-1])

            event_loc[event] = lat, lon
    
    return event_loc


def locate_stations() -> Dict[str, Tuple[float, float]]:
    """Get station latitudes and longitides."""
    if 'station_locations' in cache:
        station_loc = cache['station_locations']
    
    else:
        station_loc = cache['station_locations'] = {}

        for event in get_events():
            for line in catalogdir.readlines(f'stations/STATIONS.{event}'):
                if len(ll := line.split()) == 6:
                    station = ll[1] + '.' + ll[0]

                    if station not in station_loc:
                        lat = float(ll[2])
                        lon = float(ll[3])

                        station_loc[station] = lat, lon

    return station_loc


def is_rotated():
    """Determines whether trace rotation is RTZ or NEZ."""
    catalog = get_catalog()

    for event in catalog:
        if isinstance(catalog[event], dict):
            for traces in catalog[event].values():
                for components in traces:
                    for cmp in components:
                        if cmp == 'R' or cmp == 'T':
                            return True
                        
                        if cmp == 'N' or cmp == 'E':
                            return False

    return False
