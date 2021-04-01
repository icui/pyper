from typing import Optional

import numpy as np

from pypers import Workspace, Directory, field
from pypers.fwi.workspace import create, FWIKwargs


class Misfit(Workspace):
    """Trace misfit and adjoint source computation."""
    # observed traces
    path_observed: str = field(required=True)

    # synthetic traces
    path_synthetic: str = field(required=True)

    # taper adjoint source
    taper: Optional[float] = field()

    # compute only misfit
    misfit_only: bool = field(False)

    def apply_taper(self, adstf: np.ndarray, dt: float):
        """Apply taper to adjoint source."""
        if self.taper:
            ntaper = int(self.taper * 60 / dt)
            adstf[-ntaper:] *= np.hanning(2 * ntaper)[ntaper:]


def read_misfit(src: str) -> float:
    """Get output misfit value."""
    from pyasdf import ASDFDataSet

    mf = 0.0

    with ASDFDataSet(Directory(src).abs('adjoint.h5'), mode='r', mpi=False) as ds:
        group = ds.auxiliary_data.AdjointSources
        
        for sta in group.list():
            mf += group[sta].parameters['misfit']
    
    return mf


def create_misfit(cwd: str = '.', kwargs: FWIKwargs = {}) -> Misfit:
    return create('misfit', cwd, kwargs)
