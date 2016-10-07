# CSTBox framework
#
# Makefile for building the Debian distribution package containing the
# PERFORMER extensions.
#
# author = Eric PASCUAL - CSTB (eric.pascual@cstb.fr)

# name of the CSTBox module
MODULE_NAME=ext-performer

include $(CSTBOX_DEVEL_HOME)/lib/makefile-dist.mk

copy_files: \
	copy_bin_files \
	copy_python_files \
	copy_temp_whl_files

copy_temp_whl_files:
	cp -ar ./res/tmp/ $(BUILD_DIR)
