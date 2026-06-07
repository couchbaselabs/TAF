"""
Sample Lighthouse sanity test to validate LighthouseBase setup.

Single cluster run:
    python testrunner.py -i node.ini -t bucket_collections.lighthouse_sanity.LighthouseSanity.test_setup

Multi cluster run:
    python testrunner.py -i node.ini -t bucket_collections.lighthouse_sanity.LighthouseSanity.test_setup,nodes_init=2|2,num_of_clusters=2

Multi cluster + LH Portal (requires [LHPortal] in ini):
    python testrunner.py -i node.ini -t bucket_collections.lighthouse_sanity.LighthouseSanity.test_setup,nodes_init=2|2,num_of_clusters=2
"""

from bucket_collections.lighthouse_base import LighthouseBase


class LighthouseSanity(LighthouseBase):

    def setUp(self):
        super(LighthouseSanity, self).setUp()

    def tearDown(self):
        super(LighthouseSanity, self).tearDown()

    def test_setup(self):
        """If setUp passed, the setup is valid"""
        pass

