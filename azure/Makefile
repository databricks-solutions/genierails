# azure/Makefile — thin cloud-specific wrapper
# This file's own dir, recorded before anything else is read: MAKEFILES can
# prepend makefiles, but lastword is still this file here, and override keeps
# a command-line value from replacing it. Makefile.shared only deletes inside
# $(GENIERAILS_CLOUD_DIR)/envs.
override GENIERAILS_CLOUD_DIR := $(patsubst %/,%,$(dir $(abspath $(lastword $(MAKEFILE_LIST)))))
CLOUD      := azure
CLOUD_ROOT := $(shell pwd)
SHARED_ROOT := $(CLOUD_ROOT)/../shared

include $(SHARED_ROOT)/Makefile.shared
