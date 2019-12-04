import numpy as np
import cmath
from scipy.fftpack import fft, ifft, fftfreq

def phase_se(self, syn, obs):
	""" phase difference of synthetic and observed data
		syn[:nf], obs[:nf]: phase
		syn[nf:], obs[nf:]: amplitude
	"""
	# convert to complex array
	nfreq = len(self.freq)
	ft_syn = syn[:nfreq] + syn[nfreq:] * 1j
	ft_obs = obs[:nfreq] + obs[nfreq:] * 1j

	# fill empty slots
	nan = np.squeeze(np.where(np.isclose(ft_obs, 0)))
	ft_syn[nan] = ft_obs[nan] = 1

	# timestep and frequency indices
	nt = self.nt_obs
	ntpss = self.ntpss
	freq_idx = self.freq_idx

	# phase differencet
	phase_diff = np.vectorize(cmath.phase)(ft_syn / ft_obs)
	mf = (phase_diff**2).sum()

	# fourier transform of adjoint source time function
	ft_adstf = np.zeros(ntpss, dtype=complex)
	ft_adstf[freq_idx] = phase_diff * (-ft_syn.imag + ft_syn.real * 1j) / abs(ft_syn)

	# periodic adjoint source time function
	adstf = np.tile(ifft(ft_adstf).real, int(np.ceil(nt / ntpss)))[-nt:]

	return mf, adstf

def waveform(self, syn, obs):
	adstf = syn - obs
	mf = np.sqrt(np.sum(adstf * adstf * self.dt))
	
	return mf, adstf

def traveltime(self, syn, obs):
	cc = abs(np.convolve(obs, np.flipud(syn)))
	mf = (np.argmax(cc) - self.nt + 1) * self.dt

	adstf = np.zeros(self.nt)
	adstf[1: -1] = (syn[2:] - syn[0: -2]) / (2 * self.dt)
	adstf *= 1 / (sum(adstf * adstf) * self.dt)
	adstf *= mf

	return mf, adstf

def phase(self, syn, obs):
	nt = self.nt

	# frequencies
	freq = fftfreq(nt, self.dt)
	freq_min = 1 / self.period_max
	freq_max = 1 / self.period_min

	# index of frequencies within the frequency band
	m = np.squeeze(np.where((freq >= freq_min) & (freq < freq_max)))

	# fourier transform of traces
	ft_syn = fft(syn)[m]
	ft_obs = fft(obs)[m]

	# phase difference
	diff = ft_syn / ft_obs
	phase = np.vectorize(cmath.phase)
	phase_diff = phase(diff)
	mf = (phase_diff**2).sum()

	# fourier transform of adjoint source time function
	ft_adstf = np.zeros(nt, dtype=complex)

	# compute adjoint source time function
	phase_syn = phase(ft_syn)
	ft_adstf[m] = phase_diff * np.vectorize(np.complex)(-np.sin(phase_syn), np.cos(phase_syn))
	adstf = np.zeros(len(syn))
	adstf = -ifft(ft_adstf).real

	return mf, -adstf