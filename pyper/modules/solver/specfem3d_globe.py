import csv
import numpy as np
from os.path import isdir
from glob import glob
from pyper.modules.solver.solver import Solver
from pyper.system import stage, task, mpirun
from pyper.shell import call, mkdir, cp, rm, mv, write, exists, ln
from typing import List

class Specfem3D_Globe(Solver):
	""" Specfem3D_Globe solver
	"""
	def _setpar(self, key, val, src='base'):
		""" Modify DATA/Par_file entries in specfem3d_globe.
		
		Arguments:
			key {[type]} -- name of the entry
			val {[type]} -- value of the entry
		
		Keyword Arguments:
			src {str} -- modfiy Par_file for the workspace of a specefic event (default: {'base'})
		"""
		def split(str, sep):
			n = str.find(sep)
			if n >= 0:
				return str[:n], str[n + len(sep):]
			else:
				return str, ''
		
		val = str(val)
		par_file = 'scratch/solver/%s/DATA/Par_file' % src
		with open(par_file, 'r') as f:
			lines = []

			for line in f.readlines():
				if line.find(key) == 0:
					key, _ = split(line, '=')
					_, comment = split(line, '#')
					n = len(line) - len(key) - len(val) - len(comment) - 1
					if comment:
						line = ''.join([key, '= ', val, ' '*n, '#', comment])
					else:
						line = ''.join([key, '= ', val + '\n'])
			
				lines.append(line)

			write(par_file, ''.join(lines))
	
	def _check_binary(self, bin_file: str):
		""" Ensure required binary exists,
			if not, exit and wait for compilation.
		
		Arguments:
			bin_file {str} -- name of the required binary
		"""
		if not exists('scratch/solver/base/bin/' + bin_file):
			print('waiting for %s compilation' % bin_file)

			# backup solver Par_file
			bak_file = self['directory'] + '/DATA/Par_file.pyper_bak'
			if not exists(bak_file):
				mv(self['directory'] + '/DATA/Par_file', bak_file, sudo=True)

			# copy Par_file for compilation
			cp('scratch/solver/base/DATA/Par_file', self['directory'] + '/DATA', sudo=True)

			# clear current pipeline
			rm('scratch')
			
			exit()
	
	def _create_workspace(self, src: str):
		""" Create a workspace for the simulation of an event.
		
		Arguments:
			src {str} -- event name
		"""
		rm('scratch/solver/%s' % src)
		cp('scratch/solver/base', 'scratch/solver/%s' % src)
		cp('scratch/events/%s' % src, 'scratch/solver/%s/DATA/CMTSOLUTION' % src)
	
	@stage()
	def _mesh(self):
		""" Run mesher.
		"""
		# call meshfem
		mpirun(('scratch/solver/base', './bin/xmeshfem3D', self['ntasks']))

		# move mesh to solver directory for future runs
		rm(self['directory'] + '/DATABASES_MPI/*', sudo=True)
		mv('scratch/solver/base/DATABASES_MPI/*', self['directory'] + '/DATABASES_MPI', sudo=True)

		# copy addressing.txt and link mesh files
		cp('scratch/solver/base/OUTPUT_FILES/addressing.txt', self['directory'] + '/OUTPUT_FILES', sudo=True)
		ln(self['directory'] + '/DATABASES_MPI/*', 'scratch/solver/base/DATABASES_MPI')
	
	def setup(self):
		""" Create solver workspace and link binaries.
		"""
		# create base specfem directory
		mkdir('scratch/solver/base')
		mkdir('scratch/solver/base/DATABASES_MPI')
		mkdir('scratch/solver/base/OUTPUT_FILES')
		mkdir('scratch/solver/base/SEM')
		mkdir('scratch/solver/base/DATA')

		# link binaries
		ln(self['directory'] + '/bin', 'scratch/solver/base')
		
		# event and station list
		events: List[str] = []
		stations: List[str] = []

		# link event directory and get event list
		mkdir('scratch/events')
		for entry in glob('events/*'):
			ln(entry, 'scratch/events')
			events.append(entry[7:])

		# get station list
		with open('STATIONS', 'r') as f:
			for sta in csv.reader(f, delimiter=' ', skipinitialspace=True):
				if len(sta) == 6:
					stations.append(sta[1] + '.' + sta[0])

		self.update({'events': events, 'stations': stations})

		# link models
		for model_dir in glob(self['directory'] + '/DATA/*'):
			if isdir(model_dir):
				ln(model_dir, 'scratch/solver/base/DATA')
		
		# copy config files
		cp('Par_file', 'scratch/solver/base/DATA')
		cp('STATIONS', 'scratch/solver/base/DATA')
		cp('STATIONS', 'scratch/solver/base/DATA/STATIONS_ADJOINT')
		cp('scratch/events/%s' % self['events'][0], 'scratch/solver/base/DATA/CMTSOLUTION')
		
		# set dimension
		nproc = int(np.sqrt(self['ntasks'] / 6))
		self._setpar('NPROC_XI', nproc)
		self._setpar('NPROC_ETA', nproc)

		# set parameters
		self._setpar('RECORD_LENGTH_IN_MINUTES', str(self['duration']) + 'd0')
		self._setpar('STEADY_STATE_IN_MINUTES', '-1.d0')
		self._setpar('USE_MONOCHRONIC_TIME_FUNCTION', '.false.')
		self._setpar('ADIOS_ENABLED', '.true.' if self['adios'] else '.false.')
		self._setpar('SAVE_FORWARD', '.false.')

		# create output directory
		mkdir('output/raw_syn')
		mkdir('output/stf')

		# restore original solver Par_file
		bak_file: str = self['directory'] + '/DATA/Par_file.pyper_bak'
		if exists(bak_file):
			mv(bak_file, bak_file[0:-10], sudo=True)

		# check solver binary
		self._check_binary('xspecfem3D')
		
		# check tomo binaries
		if self['adios']:
			self._check_binary('xsum_kernels_adios')
			self._check_binary('xcombine_vol_data_vtk_adios')
		else:
			self._check_binary('xsum_kernels')
			self._check_binary('xcombine_vol_data_vtk')
		
		# call mesher if necessary
		if not exists('scratch/solver/base/OUTPUT_FILES/addressing.txt'):
			addr_file: str = self['directory'] + '/OUTPUT_FILES/addressing.txt'
			if exists(addr_file):
				# use existing mesh
				ln(addr_file, 'scratch/solver/base/OUTPUT_FILES')
				ln(self['directory'] + '/DATABASES_MPI/*', 'scratch/solver/base/DATABASES_MPI')
			
			else:
				# check and call mesher
				self._check_binary('xmeshfem3D')
				self._mesh()
		
		# link values_from_mesher.h
		if not exists('scratch/solver/base/OUTPUT_FILES/values_from_mesher.h'):
			mesher_file = self['directory'] + '/OUTPUT_FILES/values_from_mesher.h'
			if exists(mesher_file):
				ln(mesher_file, 'scratch/solver/base/OUTPUT_FILES')
	
	@task()
	def forward(self, src, save_forward=False):
		# create working directory
		self._create_workspace(src)
		
		# change paramaters
		self._setpar('SIMULATION_TYPE', 1, src)
		self._setpar('SAVE_FORWARD', '.true.' if save_forward else '.false.', src)
		
		# call specfem
		mpirun(('scratch/solver/%s' % src, './bin/xspecfem3D', self['ntasks']))
		
		# export traces in forward mode
		mv('scratch/solver/%s/OUTPUT_FILES/synthetic.h5' % src, 'output/raw_syn/%s.raw_syn.h5' % src)
		
		# export source time function files
		for stf_file in glob('scratch/solver/%s/OUTPUT_FILES/plot_source_time_function*.txt' % src):
			stf_idx = stf_file[53:]
			mv(stf_file, 'output/stf/%s.stf%s' % (src, stf_idx))
	
	@task()
	def adjoint(self, src):
		# copy adjoint sources
		cp('output/adjoint/%s.adjoint.h5' % src, 'scratch/solver/%s/SEM/adjoint.h5' % src)
		
		# set par_file to adjoint
		self._setpar('SIMULATION_TYPE', 3, src)
		self._setpar('SAVE_FORWARD', '.false.', src)
		
		# call specfem
		mpirun(('scratch/solver/%s' % src, './bin/xspecfem3D', self['ntasks']))

		# export kernels
		mv('scratch/solver/%s/OUTPUT_FILES/kernels.bp' % src, 'output/kernel/%s.bp' % src)

	@stage()
	def sum_kernels(self, events):
		# create working directory
		self._create_workspace('kernels_sum')

		# prepare kernel list for summing kernels
		write('scratch/solver/kernels_sum/kernels_list.txt', '\n'.join(events))
		
		# prepare slice list for combining kernels
		slices = '\n'.join(str(i) for i in range(self['ntasks']))
		write('scratch/solver/kernels_sum/slices.txt', slices)

		if len(events) > 1:
			# clear directories
			mkdir('scratch/solver/kernels_sum/INPUT_KERNELS')
			mkdir('scratch/solver/kernels_sum/OUTPUT_SUM')

			# link event kernels
			for src in events:
				mkdir('scratch/solver/kernels_sum/INPUT_KERNELS/' + src)
				ln('output/kernel/%s.bp' % src, 'scratch/solver/kernels_sum/INPUT_KERNELS/%s/kernels.bp' % src)

			# run xsum_kernels
			mpirun(('scratch/solver/kernels_sum', './bin/xsum_kernels_adios', self['ntasks']))

			# move to output directory
			mv('scratch/solver/kernels_sum/OUTPUT_SUM/kernels_sum.bp', 'output/kernel')
		
		elif len(events) == 1:
			# skip sum for 1 event
			cp('output/kernel/%s.bp' % events[0], 'output/kernel/kernels_sum.bp')

	@task()
	def export_kernel(self, kernel, src='kernels_sum'):
		# temp location for exporting
		kernel_file = 'scratch/solver/%s/kernel_vtk.bp' % src
		rm(kernel_file)
		ln('output/kernel/%s.bp' % src, kernel_file)

		# combine data from different nodes and export to vtk
		call('cd scratch/solver/%s && ./bin/xcombine_vol_data_vtk_adios slices.txt ' % src + kernel + \
				'_kl kernel_vtk.bp DATABASES_MPI/solver_data.bp . %d 1' % self['vtk_quality'])
		
		# move to output directory
		mv('scratch/solver/%s/reg_1_%s_kl.vtk' % (src, kernel), 'output/kernel/%s_%s.vtk' % (src, kernel))
	
	def set_duration(self, src, duration):
		self._setpar('RECORD_LENGTH_IN_MINUTES', '%fd0' % (duration / 60), src)
	
	def set_monochronic(self, src, steady_state=None):
		if steady_state:
			self._setpar('STEADY_STATE_IN_MINUTES', '%fd0' % (steady_state / 60), src)
			self._setpar('USE_MONOCHRONIC_TIME_FUNCTION', '.true.', src)
		
		else:
			self._setpar('STEADY_STATE_IN_MINUTES', '-1.d0', src)
			self._setpar('USE_MONOCHRONIC_TIME_FUNCTION', '.false.', src)