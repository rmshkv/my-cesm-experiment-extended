#!/bin/bash

set -e

casename=gcp-cases

remote_mach=thorodin.cgd.ucar.edu
remote_dir=/project/webshare/projects/ocean-diagnostics-books/${USER}/${casename}

source activate cesm-exp

conda info --envs

jupyter-book clean _computed-notebooks
jupyter-book build _computed-notebooks/${casename} --all

ssh ${USER}@${remote_mach} "mkdir -p ${remote_dir}"
scp -r _computed-notebooks/${casename}/_build/html/* ${remote_mach}:${remote_dir}
