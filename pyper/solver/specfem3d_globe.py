import csv
from os.path import isdir
from math import sqrt
from glob import glob
from pyper.main import modules
from pyper.solver.base import base
from pyper.tools.shell import call, mkdir, cp, rm, mv, write, exists, ln, echo
from pyper.pipeline import *

system = modules['system']

class specfem3d_globe(base):
	def setup(self):
		""" link specfem3d_globe subdirectories to scratch/solver
		"""
		# create specfem directories
		mkdir('scratch/solver/specfem3d_globe')
		mkdir('scratch/solver/specfem3d_globe/DATABASES_MPI')
		mkdir('scratch/solver/specfem3d_globe/OUTPUT_FILES')
		mkdir('scratch/solver/specfem3d_globe/SEM')
		mkdir('scratch/solver/specfem3d_globe/DATA')

		# link binaries
		ln(self.directory + '/bin', 'scratch/solver/specfem3d_globe')
		
		# event and station list
		events = []
		stations = []

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

		self.set({'events': events, 'stations': stations})

		# link models
		for model_dir in glob(self.directory + '/DATA/*'):
			if isdir(model_dir):
				ln(model_dir, 'scratch/solver/specfem3d_globe/DATA')
		
		# copy config files
		cp('Par_file', 'scratch/solver/specfem3d_globe/DATA')
		cp('STATIONS', 'scratch/solver/specfem3d_globe/DATA')
		cp('STATIONS', 'scratch/solver/specfem3d_globe/DATA/STATIONS_ADJOINT')
		cp('scratch/events/%s' % self.events[0], 'scratch/solver/specfem3d_globe/DATA/CMTSOLUTION')
		
		# set dimension
		nproc = int(sqrt(system.ntasks / 6))
		self.setpar('NPROC_XI', nproc)
		self.setpar('NPROC_ETA', nproc)

		# set parameters
		self.setpar('RECORD_LENGTH_IN_MINUTES', str(self.duration) + 'd0')
		self.setpar('STEADY_STATE_IN_MINUTES', '-1.d0')
		self.setpar('USE_MONOCHRONIC_TIME_FUNCTION', '.false.')
		self.setpar('ADIOS_ENABLED', '.true.' if self.adios else '.false.')
		self.setpar('SAVE_FORWARD', '.false.')

		# create output directory
		mkdir('output/raw_syn')
		mkdir('output/stf')

		# restore original solver Par_file
		bak_file = self.directory + '/DATA/Par_file.pyper_bak'
		if exists(bak_file):
			mv(bak_file, bak_file[0:-10], sudo=True)

		# check solver binary
		self.check_binary('xspecfem3D')
		
		# check tomo binaries
		if self.adios:
			self.check_binary('xsum_kernels_adios')
			self.check_binary('xcombine_vol_data_vtk_adios')
		else:
			self.check_binary('xsum_kernels')
			self.check_binary('xcombine_vol_data_vtk')
		
		# call mesher if necessary
		if not exists('scratch/solver/specfem3d_globe/OUTPUT_FILES/addressing.txt'):
			addr_file = self.directory + '/OUTPUT_FILES/addressing.txt'
			if exists(addr_file):
				# use existing mesh
				ln(addr_file, 'scratch/solver/specfem3d_globe/OUTPUT_FILES')
				ln(self.directory + '/DATABASES_MPI/*', 'scratch/solver/specfem3d_globe/DATABASES_MPI')
			
			else:
				# check and call mesher
				self.check_binary('xmeshfem3D')
				self.mesh()
		
		# link values_from_mesher.h
		if not exists('scratch/solver/specfem3d_globe/OUTPUT_FILES/values_from_mesher.h'):
			mesher_file = self.directory + '/OUTPUT_FILES/values_from_mesher.h'
			if exists(mesher_file):
				ln(mesher_file, 'scratch/solver/specfem3d_globe/OUTPUT_FILES')

	def setpar(self, key, val, src='specfem3d_globe'):
		""" modify DATA/Par_file in specfem3d_globe
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
	
	def check_binary(self, bin_file):
		""" ensure required binary exists
			if not, exit and wait for compilation
		"""
		if not exists('scratch/solver/specfem3d_globe/bin/' + bin_file):
			print('waiting for %s compilation' % bin_file)

			# backup solver Par_file
			bak_file = self.directory + '/DATA/Par_file.pyper_bak'
			if not exists(bak_file):
				mv(self.directory + '/DATA/Par_file', bak_file, sudo=True)

			# copy Par_file for compilation
			cp('scratch/solver/specfem3d_globe/DATA/Par_file', self.directory + '/DATA', sudo=True)

			# clear current pipeline
			rm('scratch')
			
			exit()
	
	def create_directory(self, src):
		""" create a workspace for simulation
		"""
		rm('scratch/solver/%s' % src)
		cp('scratch/solver/specfem3d_globe', 'scratch/solver/%s' % src)
		cp('scratch/events/%s' % src, 'scratch/solver/%s/DATA/CMTSOLUTION' % src)
	
	@task
	def forward(self, src, save_forward=False):
		""" run forward simulation
		"""
		# create working directory
		self.create_directory(src)
		
		# change paramaters
		self.setpar('SIMULATION_TYPE', 1, src)
		self.setpar('SAVE_FORWARD', '.true.' if save_forward else '.false.', src)
		
		# call specfem
		system.mpirun(('scratch/solver/%s' % src, './bin/xspecfem3D', self.ntasks))
		
		# export traces in forward mode
		mv('scratch/solver/%s/OUTPUT_FILES/synthetic.h5' % src, 'output/raw_syn/%s.raw_syn.h5' % src)
		
		# export source time function files
		for stf_file in glob('scratch/solver/%s/OUTPUT_FILES/plot_source_time_function*.txt' % src):
			stf_idx = stf_file[53:]
			mv(stf_file, 'output/stf/%s.stf%s' % (src, stf_idx))
	
	@task
	def adjoint(self, src):
		""" run adjoint simulation
		"""
		# copy adjoint sources
		cp('output/adjoint/%s.adjoint.h5' % src, 'scratch/solver/%s/SEM/adjoint.h5' % src)
		
		# set par_file to adjoint
		self.setpar('SIMULATION_TYPE', 3, src)
		self.setpar('SAVE_FORWARD', '.false.', src)
		
		# call specfem
		system.mpirun(('scratch/solver/%s' % src, './bin/xspecfem3D', self.ntasks))

		# export kernels
		mv('scratch/solver/%s/OUTPUT_FILES/kernels.bp' % src, 'output/kernel/%s.bp' % src)

	@stage
	def mesh(self):
		""" export solver output
		"""
		# call meshfem
		system.mpirun(('scratch/solver/specfem3d_globe', './bin/xmeshfem3D', self.ntasks))

		# move mesh to solver directory for future runs
		rm(self.directory + '/DATABASES_MPI/*', sudo=True)
		mv('scratch/solver/specfem3d_globe/DATABASES_MPI/*', self.directory + '/DATABASES_MPI', sudo=True)

		# copy addressing.txt and link mesh files
		cp('scratch/solver/specfem3d_globe/OUTPUT_FILES/addressing.txt', self.directory + '/OUTPUT_FILES', sudo=True)
		ln(self.directory + '/DATABASES_MPI/*', 'scratch/solver/specfem3d_globe/DATABASES_MPI')

	@stage
	def sum_kernels(self, events):
		""" prepare files needed for xsum_kernels
		"""
		# create working directory
		self.create_directory('kernels_sum')

		# prepare kernel list for summing kernels
		write('scratch/solver/kernels_sum/kernels_list.txt', '\n'.join(events))
		
		# prepare slice list for combining kernels
		slices = '\n'.join(str(i) for i in range(system.ntasks))
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
			system.mpirun(('scratch/solver/kernels_sum', './bin/xsum_kernels_adios', self.ntasks))

			# move to output directory
			mv('scratch/solver/kernels_sum/OUTPUT_SUM/kernels_sum.bp', 'output/kernel')
		
		elif len(events) == 1:
			# skip sum for 1 event
			cp('output/kernel/%s.bp' % events[0], 'output/kernel/kernels_sum.bp')

	@task
	def export_kernel(self, kernel, src='kernels_sum'):
		""" export kernel to vtk
		"""
		# temp location for exporting
		kernel_file = 'scratch/solver/%s/kernel_vtk.bp' % src
		rm(kernel_file)
		ln('output/kernel/%s.bp' % src, kernel_file)

		# combine data from different nodes and export to vtk
		call('cd scratch/solver/%s && ./bin/xcombine_vol_data_vtk_adios slices.txt '+ kernel + \
				'_kl kernel_vtk.bp DATABASES_MPI/solver_data.bp . %d 1' % (src, self.vtk_quality))
		
		# move to output directory
		mv('scratch/solver/%s/reg_1_%s_kl.vtk' % (src, kernel), 'output/kernel/%s_%s.vtk' % (src, kernel))
	
	@stage
	def set_duration(self, src, duration):
		""" set time duration of simulation
		"""
		self.setpar('RECORD_LENGTH_IN_MINUTES', '%fd0' % (duration / 60), src)
	
	@stage
	def set_monochronic(self, src, steady_state=None):
		""" tell specfem to use monochronic source time function for source encoding
			and set length of steady state
		"""
		if steady_state:
			self.setpar('STEADY_STATE_IN_MINUTES', '%fd0' % (steady_state / 60), src)
			self.setpar('USE_MONOCHRONIC_TIME_FUNCTION', '.true.', src)
		
		else:
			self.setpar('STEADY_STATE_IN_MINUTES', '-1.d0', src)
			self.setpar('USE_MONOCHRONIC_TIME_FUNCTION', '.false.', src)