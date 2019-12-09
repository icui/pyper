.. pyper documentation master file, created by
   sphinx-quickstart on Mon Dec  9 10:01:10 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to pyper's documentation!
=================================

.. toctree::
   :maxdepth: 8
   
   quick-start.rst
   modules/contents.rst

Overview
============

Pyper is a flexible and free toolbox for performing global full waveform inversion and related tasks on HPC clusters. It includes tools for signal processing, window selection, adjoint source construction, gradient based optimization, and workflow management. Its workflow consists of two stages: creating a list of tasks that will be executed and executing the tasks either in serial or in parallel. Using this strategy, it is much easier to optimize computational resources and restore previous stages if any task fails compared to traditional workflows. Pyper utilizes modern software and libraries to make workflows more robust and efficient, like ASDF for seismic data, CUDA(R) for trace processing, and EnTK for workflow management.