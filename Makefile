DIST_DIR=./dist
DIRS=b conf connections couchbase_utils lib platform_utils pytests scripts src
FILES=Makefile README.md TestInput.py
SLEEP_TIME=3
VERBOSE=0
DEBUG=0
TESTNAME=conf/sanity.conf

.PHONY: clean TAF test

TAF:
	mkdir -p $(DIST_DIR)/TAF
	tar -cvf $(DIST_DIR)/TAF.tar --exclude='*.pyc' $(DIRS) $(FILES)
	tar -C $(DIST_DIR)/TAF -xvf $(DIST_DIR)/TAF.tar
	rm -f $(DIST_DIR)/TAF.tar
	tar -C $(DIST_DIR) -czvf $(DIST_DIR)/TAF.tar.gz TAF

clean:
	rm -rf $(DIST_DIR)

test:
	scripts/start_cluster_and_run_tests.sh b/resources/dev-4-nodes.ini $(TESTNAME)

simple-test-serverless:
	scripts/start_cluster_and_run_tests.sh b/resources/dev-3-nodes.ini conf/serverless_sanity.conf "" 1

# specify number of nodes and testcase
any-test:
	scripts/start_cluster_and_run_tests.sh $(NODES) $(TEST)

# specify number of nodes and test conf
any-suite:
	scripts/start_cluster_and_run_tests.sh $(NODES) $(SUITE)

revision:
	19