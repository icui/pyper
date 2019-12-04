import numpy as np
import pyasdf
import random
import cmath
from scipy.special import erf
from scipy.signal import resample
from scipy.fftpack import fftfreq
from obspy.core.trace import Trace
from pyper.tools.cuda import fft
from pyper.main import modules
from pyper.kernel.ortho import ortho
from pyper.tools.shell import mkdir, rm, exists, echo
from pyper.pipeline import *
import matplotlib.pyplot as plt

solver = modules['solver']

class ortho2(ortho):
	def run(self):
		""" compute the kernel of each source then sum kernels
		"""
		# solver = modules['solver']
		# self.read_timestep(solver.events[0], 'obs')
		# self.get_frequencies()
		# solver.forward('S2')
		# solver.set_monochronic()
		# solver.forward('C2')

		self.compare()
		exit()
	
	def compare(self):
		modules['system'].set('stages', [])
		solver = modules['solver']
		phase = np.vectorize(cmath.phase)
		self.get_frequencies()
		dt = self.dt
		nt_ss = self.nt_ss
		nt_tau = self.nt_tau
		nt = nt_ss + nt_tau
		nt_ktau = self.nt_ktau
		nfreq = len(self.freq)
		
		with pyasdf.ASDFDataSet('oq/proc_syn/SUPERSOURCE.proc_syn.h5', mode='r') as ds:
			tr1 = ds.waveforms['AD_EQA'].proc_syn.select(component='E')[0].data[:nt]

		with pyasdf.ASDFDataSet('oq/proc_obs/C201610282002A.proc_obs.h5', mode='r') as ds:
			tr2 = ds.waveforms['AD_EQA'].proc_obs.select(component='E')[0].data[:nt]
		
		hdur = 2.0
		t = np.linspace(0, (nt - 1) * dt, nt)
		stf1 = np.zeros(nt)
		stf2 = 0.5 * (1.0 + erf(1.628 * (t - 1.5 * hdur) / hdur))
		for f0 in self.freq:
			stf1 += np.sin(2 * np.pi * f0 * t)
		
		# f1 = fft(tr1[-nt_tau:])[self.if_tau]
		# f2 = fft(tr2[:nt_ktau])[self.if_ktau]
		
		f1 = tr1[:nfreq] + tr1[nfreq:] * 1j
		f2 = tr2[:nfreq] + tr2[nfreq:] * 1j

		s1 = fft(stf1[-nt_tau:])[self.if_tau]
		s2 = fft(stf2[:nt_ktau])[self.if_ktau]
		f1 *= s2 / s1
		
		plt.plot(phase(f1))
		plt.plot(phase(f2))
		plt.plot(phase(f1 / f2))

		plt.show()
			