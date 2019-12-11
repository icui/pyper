Quick start
=================================

Dependencies
----------------
Pyper is still in early development stage, which means that it is pretty picky about the environment. Future versions of pyper will provide alternative choices to parallel hdf5, pyasdf, cupy and specfem3d_globe.

- Python 3.5 or later
- `Obspy <https://github.com/obspy/obspy/>`_
- `Parallel h5py <http://docs.h5py.org/en/stable/build.html#building-against-parallel-hdf5/>`_
- `Pyasdf <https://github.com/SeismicData/pyasdf/>`_
- `CuPy <https://cupy.chainer.org/>`_
- `Specfem3D Globe <https://github.com/geodynamics/specfem3d_globe/>`_

Installation
----------------

1. Clone from github::

	git clone https://github.com/icui/pyper.git

2. Add pyper to $PYTHONPATH::

	export PYTHONPATH=$PYTHONPATH:<path-to-pyper>

Running examples
----------------
Examples can be found in the examples directory. To run an example, you need to first modify config.json to choose the cluster system you are using, then run::

	python run_example.py