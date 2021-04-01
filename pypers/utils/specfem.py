from typing import TypedDict, Optional, List, cast

from pypers import Directory, cache, getpath


class Par_file(TypedDict, total=False):
    # 0 for forward simulation, 3 for adjoint simulation
    SIMULATION_TYPE: int

    # save forward wavefield
    SAVE_FORWARD: bool

    # use monochromatic source time function
    USE_MONOCHROMATIC_CMT_SOURCE: bool

    # simulation duration
    RECORD_LENGTH_IN_MINUTES: float

    # model name
    MODEL: str

    # use high order time scheme
    USE_LDDRK: bool

    # number of processors in XI direction
    NPROC_XI: int

    # number of processors in ETA direction
    NPROC_ETA: int

    # number of chunks
    NCHUNKS: int

    # compute steady state kernel for source encoded FWI
    STEADY_STATE_KERNEL: bool

    # steady state duration for source encoded FWI
    STEADY_STATE_LENGTH_IN_MINUTES: float


def probe_mesher(d: Directory) -> float:
    """Prober of mesher progress."""
    ntotal = 0
    nl = 0

    if not d.has(out_file := 'OUTPUT_FILES/output_mesher.txt'):
        return 0.0
    
    lines = d.readlines(out_file)

    for line in lines:
        if ' out of ' in line:
            if ntotal == 0:
                ntotal = int(line.split()[-1]) * 2

            if nl < ntotal:
                nl += 1

        if 'End of mesh generation' in line:
            return 1.0

    if ntotal == 0:
        return 0.0

    return (nl - 1) / ntotal


def probe_solver(d: Directory) -> float:
    """Prober of solver progress."""
    from math import ceil

    if not d.has(out := 'OUTPUT_FILES/output_solver.txt'):
        return 0.0
    
    lines = d.readlines(out)
    lines.reverse()

    for line in lines:
        if 'End of the simulation' in line:
            return 1.0

        if 'We have done' in line:
            words = line.split()
            done = False

            for word in words:
                if word == 'done':
                    done = True

                elif word and done:
                    return ceil(float(word)) / 100

    return 0.0


def probe_smoother(d: Directory, hess: bool):
    """Prober of smoother progress."""
    kind = 'smooth_' + ('hess' if hess else 'kl')
    ntotal = cache.get(kind)

    if ntotal and d.has(out := f'OUTPUT_FILES/{kind}.txt'):
        n = 0

        lines = d.readlines(out)
        niter = '0'

        for line in lines:
            if 'Initial residual:' in line:
                n += 1
            
            elif 'Iterations' in line:
                niter = line.split()[1]
        
        n = max(1, n)

        return f'{n}/{ntotal*2} iter{niter}'


def getpars(d: Optional[Directory] = None) -> Par_file:
    """Get entries in Par_file."""
    if d is None:
        d = Directory(getpath('specfem'))

    pars: dict = {}

    for line in d.readlines('DATA/Par_file'):
        if '=' in line:
            keysec, valsec = line.split('=')[:2]
            key = keysec.split()[0]
            val = valsec.split('#')[0].split()[0]

            if val == '.true':
                pars[key] = True
            
            elif val == '.false.':
                pars[key] = False
            
            elif val.isnumeric():
                pars[key] = int(val)
            
            else:
                try:
                    pars[key] = float(val.replace('D', 'E').replace('d', 'e'))
                
                except:
                    pars[key] = val
    
    return cast(Par_file, pars)

def setpars(d: Directory, pars: Par_file):
    """Set entries in Par_file."""
    lines = d.readlines('DATA/Par_file')

    # update lines from par
    for i, line in enumerate(lines):
        if '=' in line:
            keysec = line.split('=')[0]
            key = keysec.split()[0]

            if key in pars and pars[key] is not None:
                # convert float or bool to str
                val = pars[key]

                if isinstance(val, bool):
                    val = f'.{str(val).lower()}.'

                elif isinstance(val, float):
                    if len('%f' % val) < len(f'{val}'):
                        val = '%fd0' % val

                    else:
                        val = f'{val}d0'

                lines[i] = f'{keysec}= {val}'

    d.writelines(lines, 'DATA/Par_file')


def getsize(d: Optional[Directory] = None):
    """Number of processors to run the solver."""
    pars = getpars(d)

    if 'NPROC_XI' in pars and 'NPROC_ETA' in pars and 'NCHUNKS' in pars:
        return pars['NPROC_XI'] * pars['NPROC_ETA'] * pars['NCHUNKS']
    
    raise RuntimeError('not dimension in Par_file')

def _format_station(lines: dict, ll: List[str]):
    """Format a line in STATIONS file."""
    # location of dots for floating point numbers
    dots = 28, 41, 55, 62

    # line with station name
    line = ll[0].ljust(13) + ll[1].ljust(5)

    # add numbers with correct indentation
    for i in range(4):
        num = ll[i + 2]

        if '.' in num:
            nint, _ = num.split('.')
        
        else:
            nint = num

        while len(line) + len(nint) < dots[i]:
            line += ' '
        
        line += num
    
    lines['.'.join(ll[:2])] = line


def merge_stations(d: Directory, dst: str, catalog: Optional[dict] = None):
    """Merge multiple stations into one."""
    lines = {}

    for src in d.ls():
        event = src.split('.')[1]

        if catalog and event not in catalog:
            continue

        for line in d.readlines(src):
            if len(ll := line.split()) == 6:
                station = ll[1] + '.' + ll[0]

                if catalog and station not in catalog[event]:
                    continue

                _format_station(lines, ll)
    
    d.writelines(lines.values(), dst)


def extract_stations(d: Directory, dst: str):
    """Extract STATIONS from ASDFDataSet."""
    from os.path import join

    from pyasdf import ASDFDataSet

    for src in d.ls():
        event = src.split('.')[0]
        lines = {}
        out = join(dst, f'STATIONS.{event}')

        if d.has(out):
            continue

        with ASDFDataSet(src, mode='r', mpi=False) as ds:
            for station in ds.waveforms.list():
                if not hasattr(ds.waveforms[station], 'StationXML'):
                    print('  ' + station)
                    continue

                sta = ds.waveforms[station].StationXML.networks[0].stations[0] # type: ignore

                ll = station.split('.')
                ll.reverse()
                ll.append(f'{sta.latitude:.4f}')
                ll.append(f'{sta.longitude:.4f}')
                ll.append(f'{sta.elevation:.1f}')
                ll.append(f'{sta.channels[0].depth:.1f}')

                _format_station(lines, ll)
        
        d.writelines(lines.values(), join(dst, f'STATIONS.{event}'))
