import cupy as cp

def fft(a, n=None):
	""" Fourier transform using CuPy
	"""
	# copy array to new device if device is changed
	a_gpu = cp.asarray(a)
	
	# make sure fft function uses the right GPU device
	with cp.cuda.Device(cp.cuda.device.get_device_id()):
		# sometimes there will be a device busy error without this step
		a_gpu = cp.asarray(a)

		# Fourier transform on GPU
		f_gpu = cp.fft.fft(a_gpu, n=n)

		return f_gpu.get()
	