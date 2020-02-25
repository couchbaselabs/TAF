DIST_DIR=./dist
DIRS=b conf connections couchbase_utils lib platform_utils pytests scripts src
FILES=Makefile README TestInput.py
SLEEP_TIME=3
VERBOSE=0
DEBUG=0
TESTNAME=conf/py-all-dev.conf

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
	scripts/start_cluster_and_run_tests.sh b/resources/dev.ini $(TESTNAME)

