#0 - Prepare Environment.R

# First, edit:
# renv.lock
#
# set cpp11 to 0.1.0
# set nlopt to 2.0.3

install.packages("remotes")

library(remotes)
remotes::install_version("renv", version = "0.13.2", repos = "http://cran.us.r-project.org")
library(renv)
Sys.setenv(PATH = paste0(Sys.getenv("PATH"), ";C:\\rtools40\\usr\\bin"))

renv::activate()
renv::restore()

# Run Build
