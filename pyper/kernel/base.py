import pyasdf
from importlib import import_module
from obspy.core.stream import Stream
from pyper.tools.module import module
from pyper.main import modules
from pyper.pipeline import *

class base(module):
	def run(self):
		""" compute kernel
		"""
		raise NotImplementedError

	@stage()
	def process(self, src, tag):
		""" process event in compute node using ASDFDataSet.process
		"""
		# input and output filenames
		input_file = 'output/raw_%s/%s.raw_%s.h5' % (tag, src, tag)
		output_file = 'output/proc_%s/%s.proc_%s.h5' % (tag, src, tag)
		
		with pyasdf.ASDFDataSet(input_file, mode='r') as ds:
			# input and output waveform tags
			input_tag = next(ds.ifilter()).get_waveform_tags()[0]
			output_tag = 'proc_' + tag
			
			# process waveforms
			ds.process(lambda stream, inv: self.process_waveform(stream, inv, tag), output_file, {input_tag: output_tag})
	
	def process_waveform(self, stream, inv, tag):
		""" process traces from a certain station
			called by self.process_waveforms()
		"""
		traces = []
		for trace in stream:
			traces.append(self.process_trace(trace, tag))
		
		stream = Stream(traces)
		stream.attach_response(inv)

		return stream
	
	def process_trace(self, trace, tag):
		""" process a single trace
			called by self.process_waveform()
		"""
		trace.filter('bandpass', freqmin=1/self.period_max, freqmax=1/self.period_min, zerophase=True)
		return trace
	
	def compute_misfit(self, *args):
		""" compute misfit and adjoint source time function
		"""
		if not hasattr(self, '_misfit'):
			self._misfit = getattr(import_module('pyper.kernel.misfit'), self.misfit)
		
		return self._misfit(self, *args)