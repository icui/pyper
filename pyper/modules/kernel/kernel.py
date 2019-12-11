import pyasdf
from obspy.core.stream import Stream
from obspy.core.inventory import Inventory
from importlib import import_module
from pyper.module import Module
from abc import abstractmethod
from typing import Any

class Kernel(Module):
	""" Base class for kernel computation
	"""
	def __init__(self, section: str):
		""" load components
		
		Arguments:
			section {str} -- 'kernel' passed to Model class
		"""
		super().__init__(section)
		self._preprocess: Any = import_module('pyper.modules.preprocess.%s' % self['preprocess'])
		self._postprocess: Any = import_module('pyper.modules.postprocess.%s' % self['postprocess'])
		self._misfit: Any = import_module('pyper.modules.misfit.%s' % self['misfit'])

	@abstractmethod
	def compute(self, src: str):
		""" Compute the kernel of a given event
		
		Arguments:
			src {str} -- event name
		"""
		pass
	
	def process(self, src: str, tag: str):
		""" Process all seismograms of an event
		
		Arguments:
			src {str} -- event name
			tag {str} -- trace tag ('syn' or 'obs')
		"""
		# input and output filenames
		input_file = 'output/raw_%s/%s.raw_%s.h5' % (tag, src, tag)
		output_file = 'output/proc_%s/%s.proc_%s.h5' % (tag, src, tag)

		def process(stream: Stream, inv: Inventory):
			""" Processing function for ASDF library.
			
			Arguments:
				stream {Stream} -- trace stream to be processed
				inv {Inventory} -- trace stream information
			
			Returns:
				[Stream] -- processed trace stream
			"""
			traces = []
			for trace in stream:
				traces.append(self._preprocess.process_synthetic(trace))
			
			stream = Stream(traces)
			stream.attach_response(inv)

			return stream
		
		with pyasdf.ASDFDataSet(input_file, mode='r') as ds:
			# input and output waveform tags
			input_tag = next(ds.ifilter()).get_waveform_tags()[0]
			output_tag = 'proc_' + tag
			
			# process waveforms
			ds.process(process, output_file, {input_tag: output_tag})
	