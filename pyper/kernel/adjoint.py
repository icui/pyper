import numpy as np
import pyasdf
from scipy.signal import resample
from obspy.core.trace import Trace
from pyper.main import modules
from pyper.kernel.base import base
from pyper.tools.shell import cp, mkdir, ln
from pyper.pipeline import *

solver = modules['solver']

class adjoint(base):
	def setup(self):
		""" get event list for adjoint calculation
		"""
		# create output directory
		mkdir('output/proc_syn')
		mkdir('output/proc_obs')
		mkdir('output/adjoint')
		mkdir('output/kernel')

		# link raw observed traces
		ln('traces', 'output/raw_obs')

	def run(self):
		""" compute the kernel of each source then sum kernels
		"""
		# process events
		for src in solver.events: self.process(src, 'obs')
		
		# run forward
		for src in solver.events: solver.forward(src, True)
		
		# process synthetics
		for src in solver.events: self.process(src, 'syn')

		# run adjoint
		for src in solver.events: solver.adjoint(src)

		# sum kernels
		solver.sum_kernels(solver.events)
	
	@stage()
	def compare(self, src, adjoint_source):
		""" compute misfit and optionally compute adjoint source
		"""
		# synthetic, observed trace dataset
		syn_ds = pyasdf.ASDFDataSet('output/proc_syn/%s.proc_syn.h5' % src, mode='r')
		obs_ds = pyasdf.ASDFDataSet('output/proc_obs/%s.proc_obs.h5' % src, mode='r')

		# compare waveforms of each stattion
		results = syn_ds.process_two_files_without_parallel_output(obs_ds, self.compare_waveform)

		# save adjoint sources from master node
		if adjoint_source and syn_ds.mpi.rank == 0:
			self.set('adjoint_data', results)
		
		# required by parallel ASDF
		del syn_ds
		del obs_ds
	
	def compare_waveform(self, syn_wav, obs_wav):
		""" compute the misfit and adjoint source of a certain station
			called by self.compare_waveforms()
		"""
		# array of misfits and adjoint sources from all components
		result = []
		
		# loop over components
		for cmp in solver.components:
			# select component
			syn = syn_wav[syn_wav.get_waveform_tags()[0]].select(component=cmp)
			obs = obs_wav[obs_wav.get_waveform_tags()[0]].select(component=cmp)

			if len(syn) and len(obs):
				# compute misfit and adjoint source
				mf, adstf = self.compare_trace(syn[0], obs[0])
				stats = syn[0].stats

				# adjoint source parameters
				parameters = syn_wav.coordinates.copy()
				parameters['dt'] = stats.delta
				parameters['misfit_value'] = mf
				parameters['adjoint_source_type'] = self.misfit
				parameters['min_period'] = self.period_min
				parameters['max_period'] = self.period_max
				parameters['station_id'] = stats['network'] + '_' + stats['station']
				parameters['channel'] = stats['channel']
				parameters['component'] = cmp
				parameters['units'] = 'm'

				# save result of current trace
				result.append((parameters, adstf))
	
		return result
	
	def compare_trace(self, syn, obs):
		""" compute misfit and adjoint source time function
		"""
		# use the same timestep as forward simulation for adjoint source time function
		nt = self.nt = syn.stats.npts
		dt = self.dt = syn.stats.delta

		# trim observed data to the same record length as synthetic data
		nt_obs = int(round(obs.stats.npts * (syn.stats.npts * syn.stats.delta) / (obs.stats.npts * obs.stats.delta)))

		# resample observed data
		syn_data = syn.data
		obs_data = resample(obs.data[0: nt_obs], num=nt)

		# compute misfit and adjont source
		return self.compute_misfit(syn_data, obs_data)
	
	@stage
	def write_adjoint(self, src):
		""" export saved adjoint source data to ASDF format in head node
		"""
		# read adjoint sources
		results = self.adjoint_data
		self.set('adjoint_data', None)
		
		# save adjoint source
		with pyasdf.ASDFDataSet('output/adjoint/%s.adjoint.h5' % src, mode='w') as adj_ds:
			for sta in results:
				for (parameters, adstf) in results[sta]:
					adj_ds.add_auxiliary_data(
						data = adstf,
						data_type = 'AdjointSources',
						path = parameters['station_id'] + '_' + parameters['channel'],
						parameters = parameters
					)