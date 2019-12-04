# Pyper

### Introduction

Pyper is a package for global full waveform inversion. It features a flexible workflow manager and integration with (EnTK)[https://radicalentk.readthedocs.io/en/latest/].

The workflow of pyper consists of 2 levels: stages and tasks. Stages are executed in their creation order. A stage may contain one or more tasks, which are executed in parallel.

![Pipeline](https://raw.githubusercontent.com/icui/pyper/master/doc/img/pipeline.png)

Currently there are two options to execute the pipeline: build-in pipeline tool for small testing tasks and EnTK for large tasks.

### Prerequisites

* Python 3.7 or later
* (Obspy)[https://github.com/obspy/obspy]
* (Pyasdf)[https://github.com/SeismicData/pyasdf]
* (CuPy)[https://cupy.chainer.org]
* (Specfem3D Globe)[https://github.com/geodynamics/specfem3d_globe]

### Running examples

Before running, you need to add pyper to PATH and PYTHONPATH

````
export PATH=$PATH:<path-to-pyper>/scripts
export PYTHONPATH=$PYTHONPATH:<path-to-pyper>
````

##### On TigerGPU

````
cd example
ln -s config.tiger.json config.json
pyprun
````

##### On Summit

````
cd example
ln -s config.summit.json config.json
pyprun
````

### Design

#### Pipeline
A pipeline gathers of all the tasks that will be executed then execute them via job script or external pipeline tools

##### add_stage
create a new stage which contains one task stages are executed in sequence

##### add_task
create a new stage which contains multiple tasks and add task to this stage later executed add_task() will directly add task to this stage until another add_stage() is called

##### submit
execute pipeline