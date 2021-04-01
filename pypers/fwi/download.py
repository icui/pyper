from functools import partial
from os import environ

from obspy import read, read_inventory, read_events
from obspy.clients.fdsn.mass_downloader import GlobalDomain, Restrictions, MassDownloader
from pyasdf import ASDFDataSet

from pypers import Workspace, basedir as d


def download_event(event: str):
    d.mkdir(mseed := f'mseed/{event}')
    d.mkdir(xml := f'xml/{event}')
    
    # Event object
    e = read_events(f'events/{event}')[0]
    time = e.preferred_origin().time

    # call mass downloader
    restrictions = Restrictions(
        starttime=time - 600,
        endtime=time + 7800,
        reject_channels_with_gaps=True,
        minimum_length=0.95,
        channel_priorities=['BH[ZNE12]', 'HH[ZNE12]'],
        location_priorities=['', '00', '10']
    )

    MassDownloader().download(
        GlobalDomain(), restrictions,
        mseed_storage=d.abs(mseed),
        stationxml_storage=d.abs(xml)
    )


def convert(event: str):
    dst = f'raw_obs/{event}.raw_obs.h5'
    tmp = f'tmp/{event}.raw_obs.h5'

    if d.has(dst):
        return

    e = read_events(f'events/{event}')[0]

    # convert to ASDF
    with ASDFDataSet(d.abs(tmp), mode='w', mpi=False, compression=None) as ds:
        ds.add_quakeml(e)
        
        stations = set()
        
        for sta in d.ls(f'mseed/{event}'):

                # try:
                #     stations.add(station)
                #     ds.add_stationxml(read_inventory(d.abs(f'xml/{event}/{sta}.xml')))
                
                # except Exception as e:
                #     print('failed to add station', event, f'<<<{station}>>>')
                #     continue

            try:
                ds.add_waveforms(read(d.abs(f'mseed/{event}/{sta}')), 'raw_obs')
            
            except Exception as e:
                print(e)
            
            station = '.'.join(sta.split('.')[:2])

            if station not in stations:
                stations.add(station)
                ds.add_stationxml(read_inventory(d.abs(f'xml/{event}/{sta}.xml')))
    
    d.mv(tmp, dst)


def download():
    if 'LSB_JOBINDEX' in environ:
        n = int(environ['LSB_JOBINDEX']) - 1
        events = sorted(d.ls('events'))
        print(events[n])
        download_event(events[n])

        # for event in events:
        #     download_event(event)
        #     print(event, 'done')
        
        # d.write('', f'done_{n}')
    
    else:
        for event in d.ls('events'):
            if d.has(f'mseed/{event}') or d.has(f'tmp/{event}'):
                continue
            
            print(event)
            download_event(event)
            print('done')
            return


def download_catalog():
    ws = Workspace()
    ws.mkdir('mseed')
    ws.mkdir('xml')
    ws.mkdir('stations')
    ws.mkdir('raw_obs')
    ws.mkdir('tmp')

    ws.add(ws1 := Workspace('download'))
    ws.add(ws2 := Workspace('convert'))

    for event in d.ls('events'):
        ws1.add(partial(download_event, event), event)
        ws2.add(partial(convert, event), event)
    
    return ws


if __name__ == '__main__':
    download()
