
This packages is designed to be installed in a Python virtual environment.

To create a new virtualenv :

==> path-to-root-python\bin\virtualenv name-of-virtual-env-dir
==> cd name-of-virtual-env-dir/
==> source bin/activate

To create a new instance of NRCC_GRID in the virtualenv :
==> git clone git@github.com:rcc-acis/nrcc_grid.git
==> cd nrcc_grid

To check that all dependencies are installed in an activated virtualenv:
==> python required.py

To build the package in an activated virtualenv:
==> python setup.py build

To install the package as an egg in site-packages in an activated virtualenv:
==> python setup.py install

To configure package to run from cron in an activated virtualenv:
==> cd ../lib/pythonX.X/site-packages/nrcc-X.X-egg-name/nrcc
==> python setup.py
This copies the cron scripts from the egg to path-to-virtual-env-dir/cron
so that the entire virtual envirnoment is available to them in cron. It
also updates path to the Python executable in each cron script after it
is copied.


COMPAG
======
See the README.txt file in the nrcc.compag directory for documentation on
running the COMPAG data downloads and builds.


CONFIGURATION
=============

The directory nrcc/config contains configuration files for the various
sub-packages.

Most of the configuration will work on any machine, however, the 
following directories must be set for each install :

     COMPAG.working_dir must be set to the directory where you want
                        the final merged DEM 5K files to be created.
     STATIONS.working_dir must be set to the directory where the
                          temporary station data cache files will
                          be created.
     HRAP.working_dir must be set to the directory where the daily
                      HRAP build directories will be created.
     RAP.rap13k.working_dir must be set to the directory where the
                            daily RAP 13K build directories will be
                            created.
     RUC.ruc13k.working_dir must be set to the directory where the
                            daily RUC 13K build directories will be
                            created.
     RUC.ruc40k.working_dir must be set to the directory where the
                            daily RUC 40K build directories will be
                            created.
     IMPORTANT : The individual working directories MUST BE DIFFERENT
                 or the related apps will stomp on each other.

You may also set overrides for the location of the various static grid
files used by the grid construction applications :
     DEM5K.static_filepath = path to the static file containing the DEM
                             5km lon, lat, elev and mask grids
     HRAP.static_filepath = path to the static file containing the HRAP
                            lon, lat, elev and mask grids
     RAP.rap13k.static_filepath = path to the static file containing the
                                  RAP 13Km lon, lat, elev and mask grids
     RUC.ruc13k.static_filepath = path to the static file containing the
                                  RUC 13Km lon, lat, elev and mask grids
     RUC.ruc40k.static_filepath = path to the static file containing the
                                  RUC 40Km lon, lat, elev and mask grids

Rather than change the config files in the source directory, you can
make changes in an override file somewhere else and set the environment
variable NRCC_GRID_CONFIG_PY to the path to that file.

If you choose to use an override file, you may also set the path to a
single directory that contains all static data files such as :
      "nrcc_static_data_dir" : "/Users/Shared/nrcc_static"

The override file must contain a single valid python dictionary with each
config parameter path as a separate key. See the config.py file in this
directory for a complete example.

