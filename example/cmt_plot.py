from obspy.core.event import read_events, Catalog
from glob import glob

events = []
files = glob('events.300/*')

for file in files:
	events.append(read_events(file).events[0])

catalog = Catalog(events)
catalog.plot()
