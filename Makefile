REBAR = $(shell pwd)/rebar3
APP=snarl

.PHONY: rel stagedevrel package version all tree

all: version compile

include fifo.mk

version:
	@git describe > snarl.version

version_header: version
	@echo "-define(VERSION, <<\"$(shell cat snarl.version)\">>)." > apps/snarl/include/snarl_version.hrl

clean:
	$(REBAR) clean
	make -C rel/pkg clean

long-test:
	$(REBAR) as eqc,long eunit

rel: update
	$(REBAR) as prod compile
	sh generate_zabbix_template.sh
	$(REBAR) as prod release

package: rel
	make -C rel/pkg package
