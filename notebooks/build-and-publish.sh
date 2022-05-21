#!/bin/bash

set -e

remote_mach=thorodin.cgd.ucar.edu
remote_dir=/web/web-data/staff/mclong/my-cesm-experiment


jupyter-book clean
jupyter-book build notebooks --all

scp -r notebooks/_build/html/* ${remote_mach}:${remote_dir}