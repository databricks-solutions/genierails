# aws/Makefile — thin cloud-specific wrapper
# This file's own dir, recorded before anything else is read: MAKEFILES can
# prepend makefiles, but lastword is still this file here, and override keeps
# a command-line value from replacing it. Makefile.shared only deletes inside
# $(GENIERAILS_CLOUD_DIR)/envs.
override GENIERAILS_CLOUD_DIR := $(patsubst %/,%,$(dir $(abspath $(lastword $(MAKEFILE_LIST)))))
CLOUD      := aws
# Default to this file's dir rather than the cwd, so make -f <path>/Makefile
# works from anywhere; a command-line CLOUD_ROOT or SHARED_ROOT still wins.
CLOUD_ROOT := $(GENIERAILS_CLOUD_DIR)
SHARED_ROOT := $(CLOUD_ROOT)/../shared
# Recipes re-run $(MAKE), which reads ./Makefile; from another dir, point it
# back at this file. Run from here, MAKE is unchanged.
ifneq ($(CURDIR),$(GENIERAILS_CLOUD_DIR))
MAKE := $(MAKE) -f $(GENIERAILS_CLOUD_DIR)/Makefile
endif

include $(SHARED_ROOT)/Makefile.shared
