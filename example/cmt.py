#!/usr/bin/env python
from pyper.tools.shell import mkdir, cp, rm
from glob import glob
import random

rm('events.300', sudo=True)
mkdir('events.300', sudo=True)
files = glob('/ccs/proj/geo111/wenjie/AdjointTomography/M26/forward_simulation/CMT/*')
rdm_idx = random.sample(range(0, len(files)), len(files))

nx = 30
ny = 10
min_depth = 150

events = {}
for ix in range(nx):
	events[ix] = {}
	for iy in range(ny):
		events[ix][iy] = []


for idx in rdm_idx:
	file = files[idx]
	src = file.split('/')[-1]

	with open(file, 'r') as f:
		lines = f.readlines()
		latitude = float(lines[4][9:])
		longitude = float(lines[5][10:])
		depth = float(lines[6][6:])
		iy = int((latitude + 90) / 180 * ny)
		ix = int((longitude + 180) / 360 * nx)

		if depth > min_depth and len(events[ix][iy]) < nx * ny:
			events[ix][iy].append(file)

copied = 0
while True:
	for ix in events:
		for iy in events[ix]:
			if len(events[ix][iy]):
				cp(events[ix][iy][0], 'events.300', sudo=True)
				del events[ix][iy][0]
				copied += 1
				print(copied)
				# if copied >= 300: exit()