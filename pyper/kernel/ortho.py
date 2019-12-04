import numpy as np
import pyasdf
import random
import cmath
from scipy.special import erf
from scipy.signal import resample
from scipy.fftpack import fftfreq
from obspy.core.trace import Trace
from pyper.tools.cuda import fft
from pyper.kernel.adjoint import adjoint
from pyper.tools.shell import mkdir, rm, echo
from pyper.main import modules
from pyper.pipeline import stage

solver = modules['solver']

class ortho(adjoint):
	def run(self):
		""" compute the kernel of each source then sum kernels
		"""
		# get time step and frequency data
		self.get_frequencies()

		# process events
		for evt in solver.events:
			self.process(evt, 'obs')
		
		# encode observed frequencies
		self.encode_frequencies()

		# compute kernels
		solver.forward('SUPERSOURCE')
		self.process('SUPERSOURCE', 'syn')
		# self.run_event('SUPERSOURCE')

	@stage
	def get_frequencies(self):
		""" get frequencies used for encoding
		"""
		# length of timestep
		if not hasattr(self, 'dt'):
			with pyasdf.ASDFDataSet('output/raw_obs/%s.raw_obs.h5' % solver.events[0], mode='r') as ds:
				wav = next(ds.ifilter())
				stats = wav[wav.get_waveform_tags()[0]][0].stats
				self.set('dt', stats['delta'])
		
		dt = self.dt

		# number of timesteps to reach steady state
		nt_ss = int(self.steady_state * 60 / dt)

		# number of timesteps in integration period
		nt_tau = int(self.integrate_duration * 60 / dt)

		# frequency step
		df = 1 / nt_tau / dt

		# frequencies to be encoded
		freq = fftfreq(nt_tau, dt)
		if_tau = np.squeeze(np.where(
			(freq > 1 / self.period_max - df / 100) &
			(freq < 1 / self.period_min + df / 100)
		))
		freq = freq[if_tau]

		# get length and frequency indices of observed data used for measurement
		k = int(np.ceil(nt_ss / nt_tau))
		nt_ktau = k * nt_tau
		if_ktau = if_tau * k

		# make solver compute monochronic source
		solver.set_duration((nt_ss * 2 + nt_tau) * dt)
		solver.set_monochronic(nt_ss * dt)

		# print output
		echo(' - number of events: %d' % len(solver.events))
		echo(' - number of frequencies: %d' % len(freq))
		echo(' - period range: %.2fs - %.2fs' % (1 / freq[-1], 1 / freq[0]))
		echo(' - record length for measurement: %.2fmin' % (nt_ktau * dt / 60))
		echo(' - record length in total: %.2fmin' % solver.duration)

		# save result
		self.set({
			'freq': freq, 'dt': dt,
			'if_tau': if_tau, 'if_ktau': if_ktau,
			'nt_ss': nt_ss, 'nt_tau': nt_tau, 'nt_ktau': nt_ktau
		})
	
	def process_trace(self, trace, tag):
		# resample trace if necessary
		if trace.stats['delta'] != self.dt:
			trace.resample(1 / self.dt)

		if tag == 'obs':
			# FT of observed data
			ft_data = fft(trace.data[:self.nt_ktau])[self.if_ktau]
			
		elif tag == 'syn':
			# FT of synthetic data
			ft_data = fft(trace.data[self.nt_ss: self.nt_ss + self.nt_tau])[self.if_tau]
		
		# save as real
		trace.data = np.concatenate([ft_data.real, ft_data.imag])
		return trace

	def compare_trace(self, syn, obs):
		""" compute misfit and adjoint source time function
		"""
		mf, adstf_obs = self.compute_misfit(syn.data, obs.data)

		# resample adjoint source
		if self.nt_syn != self.nt_obs:
			adstf_obs = resample(adstf_obs, num=self.nt_syn)
		
		adstf = np.zeros(self.nt_adj)
		adstf[:self.nt_syn] = adstf_obs
		return mf, adstf
	
	@stage
	def encode_frequencies(self):
		""" get frequency component of source time function
		"""
		# timesteps
		nt_ss = self.nt_ss
		nt_tau = self.nt_tau
		nt_ktau = self.nt_ktau
		nt = nt_ss + nt_tau
		dt = self.dt
		t = np.linspace(0, (nt - 1) * dt, nt)

		# frequencies
		freq = self.freq
		nfreq = len(freq)
		if_tau = self.if_ktau
		if_ktau = self.if_ktau

		# events and stations
		nevt = len(solver.events)
		nsta = len(solver.stations)
		ncmp = len(solver.components)
		nfpe = int(np.ceil(nfreq / nevt))

		# array to store encoded observed data
		obs_se = np.zeros([2 * nfreq, nsta, ncmp])

		# content of super cmt solution
		cmt_str = ''

		# dict storing trace info from dataset
		stats = {}

		# get the order frequencies assigned to frequency slots
		if self.random_frequency == True:
			# assign frequencies randomly
			if_rdm = random.sample(range(0, nfpe * nevt), nfpe * nevt)

		else:
			# assign frequencies in order
			if_rdm = list(range(0, nfpe * nevt))

		# loop over events
		for ievt in range(nevt):
			# read observed frequency dataset
			with pyasdf.ASDFDataSet('output/proc_obs/%s.proc_obs.h5' % solver.events[ievt], mode='r') as ds:
				# get frequency of source time function (source decay = 1.628)
				hdur = ds.events[0].focal_mechanisms[0].moment_tensor.source_time_function.duration / 2
				stf_obs = 0.5 * (1.0 + erf(1.628 * (t - 1.5 * hdur) / hdur))
				sff_obs = fft(stf_obs[:nt_ktau])[if_ktau]
				
				stf_syn = np.zeros(nt)
				for ifpe in range(nfpe):
					ifreq = if_rdm[ievt * nfpe + ifpe]
					if ifreq >= nfreq: continue
					f0 = self.freq[ifreq]
					stf_syn += np.sin(2 * np.pi * f0 * t)
				sff_syn = fft(stf_syn[nt_ss:])[if_tau]

				# read event cmt solution
				with open('events/%s' % solver.events[ievt], 'r') as cmt:
					lines = cmt.readlines()
				
				# loop over frequency slots of current event
				for ifpe in range(nfpe):
					# random frequency assigned to current event
					ifreq = if_rdm[ievt * nfpe + ifpe]
					# ifreq = if_rdm[ifpe * nevt + ievt]
					if ifreq >= nfreq: continue
					f0 = self.freq[ifreq]
					
					# source time function for synthetic waveforms
					stf_syn2 = np.sin(2 * np.pi * f0 * t)
					sff_syn2 = fft(stf_syn[nt_ss:])[if_tau]

					# compute the phase shift caused by different source time functions
					pshift = sff_syn[ifreq] / sff_obs[ifreq]
					echo('pshift:', cmath.phase(pshift), cmath.phase(sff_syn[ifreq]), cmath.phase(sff_obs[ifreq]))

					# replace the half duration of event cmt solution with 1 / f0 and write to super source
					lines[3] = 'half duration:%s%f\n' % (' ' * (9 - len(str(int(1 / f0)))), 1 / f0)
					cmt_str += ''.join(lines)

					# loop over stations
					for ista in range(nsta):
						wav = ds.waveforms[solver.stations[ista]]
						stream = wav[wav.get_waveform_tags()[0]]

						# save trace stats
						if not ista in stats:
							stats[ista] = {}
						
						# loop over components
						for icmp in range(ncmp):
							trace = stream.select(component=solver.components[icmp])
							if not len(trace): continue
							trace = trace[0]

							# save trace stats
							if not icmp in stats[ista]:
								stats[ista][icmp] = trace.stats

							# save real and imaginary part of data
							ft_obs = (trace.data[ifreq] + trace.data[ifreq + nfreq] * 1j) * pshift
							obs_se[ifreq, ista, icmp] = ft_obs.real
							obs_se[ifreq + nfreq, ista, icmp] = ft_obs.imag
		
		# write cmt solution of super source
		with open('scratch/events/SUPERSOURCE', 'w') as cmt:
			cmt.write(cmt_str)
		
		# save encoded data in ASDF format
		with pyasdf.ASDFDataSet('output/proc_obs/SUPERSOURCE.proc_obs.h5', mode='w') as ds:
			for ista in stats:
				for icmp in stats[ista]:
					trace = Trace(header = stats[ista][icmp])
					trace.data = obs_se[:, ista, icmp]
					ds.add_waveforms(trace, tag='proc_obs')