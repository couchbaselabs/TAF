from dcp_base import DCPBase
from dcp_new.constants import *
from dcp_bin_client import *
from mc_bin_client import MemcachedClient as McdClient
from memcached.helper.data_helper import MemcachedClientHelper
import unittest
from memcacheConstants import *
from threading import Thread
from remote.remote_util import RemoteMachineShellConnection
from datetime import datetime


class DcpTestCase(DCPBase):
    def setUp(self):
        super(DCPBase, self).setUp()
        self.remote_shell = RemoteMachineShellConnection(self.cluster.master)
        # Create default bucket and add rbac user
        self.bucket_util.create_default_bucket()
        bucket_name = "default"
        self.bucket_util.add_rbac_user()
        self.sleep(30)

        self.dcp_client = DcpClient(host=self.cluster.master.ip)
        self.dcp_client.sasl_auth_plain("cbadminbucket","password")
        self.dcp_client.bucket_select(bucket_name)

        self.mcd_client = MemcachedClientHelper.direct_client(self.cluster.master, self.cluster.buckets[0])

    def tearDown(self):
        self.dcp_client.close()
        self.mcd_client.close()

    """Basic dcp open consumer connection test

    Verifies that when the open dcp consumer command is used there is a
    connection instance that is created on the server and that when the
    tcp connection is closed the connection is remove from the server"""

    def test_open_consumer_connection_command(self):
        response = self.dcp_client.open_consumer("mystream")
        print("response: {0}".format(response))
        assert response['status'] == SUCCESS

        response = self.mcd_client.stats('dcp')
        print("response: {0}".format(response))
        assert response['eq_dcpq:mystream:type'] == 'consumer'

        self.dcp_client.close()
        self.sleep(1)
        response = self.mcd_client.stats('dcp')
        print("response: {0}".format(response))

        assert 'eq_dcpq:mystream:type' not in response

    """Basic dcp open producer connection test

    Verifies that when the open dcp producer command is used there is a
    connection instance that is created on the server and that when the
    tcp connection is closed the connection is remove from the server"""

    def test_open_producer_connection_command(self):

        response = self.dcp_client.open_producer("mystream")
        print("response: {0}".format(response))
        assert response['status'] == SUCCESS

        response = self.mcd_client.stats('dcp')
        print("response: {0}".format(response))
        assert response['eq_dcpq:mystream:type'] == 'producer'

        self.dcp_client.close()
        self.sleep(1)
        response = self.mcd_client.stats('dcp')
        print("response: {0}".format(response))

        assert 'eq_dcpq:mystream:type' not in response

    def test_open_notifier_connection_command(self):
        """Basic dcp open notifier connection test

        Verifies that when the open dcp noifier command is used there is a
        connection instance that is created on the server and that when the
        tcp connection is closed the connection is remove from the server"""

        response = self.dcp_client.open_notifier("notifier")
        print("response: {0}".format(response))
        assert response['status'] == SUCCESS

        response = self.mcd_client.stats('dcp')
        print("response: {0}".format(response))
        assert response['eq_dcpq:notifier:type'] == 'notifier'

        self.dcp_client.close()
        self.sleep(1)

        response = self.mcd_client.stats('dcp')
        print("response: {0}".format(response))
        assert 'eq_dcpq:mystream:type' not in response

    """Open consumer connection same key

    Verifies a single consumer connection can be opened.  Then opens a
    second consumer connection with the same key as the original.  Expects
    that the first consumer connection is closed.  Stats should reflect 1
    consumer connected
    """

    def test_open_consumer_connection_same_key(self):
        stream = "mystream"
        self.dcp_client.open_consumer(stream)

        c1_stats = self.mcd_client.stats('dcp')
        assert c1_stats['eq_dcpq:' + stream + ':type'] == 'consumer'

        self.sleep(2)
        c2_stats = None
        for i in range(10):
            self.dcp_client = DcpClient(self.cluster.master.ip, self.cluster.master.port)
            response = self.dcp_client.open_consumer(stream)
            assert response['status'] == SUCCESS

        c2_stats = self.mcd_client.stats('dcp')
        assert c2_stats is not None
        assert c2_stats['eq_dcpq:' + stream + ':type'] == 'consumer'
        assert c2_stats['ep_dcp_count'] == '1'

        assert c1_stats['eq_dcpq:' + stream + ':created'] < \
               c2_stats['eq_dcpq:' + stream + ':created']

    """Open producer same key

    Verifies a single producer connection can be opened.  Then opens a
    second consumer connection with the same key as the original.  Expects
    that the first producer connection is closed.  Stats should reflect 1
    producer connected.
    """

    @unittest.skip("Needs Debug")
    def test_open_producer_connection_same_key(self):
        stream = "mystream"
        self.dcp_client.open_producer(stream)

        c1_stats = self.mcd_client.stats('dcp')
        assert c1_stats['eq_dcpq:' + stream + ':type'] == 'producer'

        self.sleep(2)
        c2_stats = None
        for i in range(10):
            conn = DcpClient(self.cluster.master.ip, self.cluster.master.port)
            response = conn.open_producer(stream)
            assert response['status'] == SUCCESS

        c2_stats = self.mcd_client.stats('dcp')

        assert c2_stats['eq_dcpq:' + stream + ':type'] == 'producer'

        # CBQE-3410 1 or 2 is ok
        assert c2_stats['ep_dcp_count'] == '1' or c2_stats['ep_dcp_count'] == '2'

        assert c1_stats['eq_dcpq:' + stream + ':created'] < \
               c2_stats['eq_dcpq:' + stream + ':created']

    """ Open consumer empty name

    Tries to open a consumer connection with empty string as name.  Expects
    to recieve a client error.
    """

    def test_open_consumer_no_name(self):
        response = self.dcp_client.open_consumer("")
        assert response['status'] == ERR_EINVAL

    """ Open producer empty name

    Tries to open a producer connection with empty string as name.  Expects
    to recieve a client error.
    """

    def test_open_producer_no_name(self):
        response = self.dcp_client.open_producer("")
        assert response['status'] == ERR_EINVAL

    """ Open n producers and consumers

    Open n consumer and n producer connections.  Check dcp stats and verify number
    of open connections = 2n with corresponding values for each conenction type.
    Expects each open connection response return true.
    """

    def test_open_n_consumer_producers(self):
        n = 16
        conns = [DcpClient(self.cluster.master.ip,  self.cluster.master.port) for i in xrange(2 * n)]
        ops = []
        for i in xrange(n):
            op = conns[i].open_consumer("consumer{0}".format(i))
            ops.append(op)
            op = conns[n + i].open_producer("producer{0}".format(n + i))
            ops.append(op)

        for op in ops:
            assert op['status'] == SUCCESS

        stats = self.mcd_client.stats('dcp')
        assert stats['ep_dcp_count'] == str(n * 2)

    def test_open_notifier(self):
        response = self.dcp_client.open_notifier("notifier")
        assert response['status'] == SUCCESS

    def test_open_notifier_no_name(self):
        response = self.dcp_client.open_notifier("")
        assert response['status'] == ERR_EINVAL

    """Basic add stream test

    This test verifies a simple add stream command. It expects that a stream
    request message will be sent to the producer before a response for the
    add stream command is returned."""

    @unittest.skip("invalid: MB-11890")
    def test_add_stream_command(self):

        response = self.dcp_client.open_consumer("mystream")
        assert response['status'] == SUCCESS
        response = self.dcp_client.add_stream(0, 0)
        assert response['status'] == SUCCESS

    @unittest.skip("invalid: MB-11890")
    def test_add_stream_reopen_connection(self):

        for i in range(10):
            response = self.dcp_client.open_consumer("mystream")
            assert response['status'] == SUCCESS

            response = self.dcp_client.add_stream(0, 0)
            assert response['status'] == SUCCESS

            self.dcp_client.reconnect()

    """Add stream to producer

    Attempt to add stream to a producer connection. Expects to recieve
    client error response."""

    @unittest.skip("invalid: MB-11890")
    def test_add_stream_to_producer(self):

        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        response = self.dcp_client.add_stream(0, 0)
        assert response['status'] == ERR_ECLIENT

    """Add stream test without open connection

    This test attempts to add a stream without idnetifying the
    client as a consumer or producer.  Excepts request
    to throw client error"""

    @unittest.skip("invalid: MB-11890")
    def test_add_stream_without_connection(self):
        response = self.dcp_client.add_stream(0, 0)
        assert response['status'] == ERR_ECLIENT

    """Add stream command with no consumer vbucket

    Attempts to add a stream when no vbucket exists on the consumer. The
    client shoudl expect a not my vbucket response immediately"""

    @unittest.skip("invalid: MB-11890")
    def test_add_stream_not_my_vbucket(self):
        response = self.dcp_client.open_consumer("mystream")
        assert response['status'] == SUCCESS

        response = self.dcp_client.add_stream(1025, 0)
        assert response['status'] == ERR_NOT_MY_VBUCKET

    """Add stream when stream exists

    Creates a stream and then attempts to create another stream for the
    same vbucket. Expects to fail with an exists error."""

    @unittest.skip("invalid: MB-11890")
    def test_add_stream_exists(self):
        response = self.dcp_client.open_consumer("mystream")
        assert response['status'] == SUCCESS

        response = self.dcp_client.add_stream(0, 0)
        assert response['status'] == SUCCESS

        response = self.dcp_client.add_stream(0, 0)
        assert response['status'] == ERR_KEY_EEXISTS

    """Add stream to new consumer

    Creates two clients each with consumers using the same key.
    Attempts to add stream to first consumer and second consumer.
    Expects that adding stream to second consumer passes"""

    @unittest.skip("invalid: MB-11890")
    def test_add_stream_to_duplicate_consumer(self):

        response = self.dcp_client.open_consumer("mystream")
        assert response['status'] == SUCCESS

        dcp_client2 = DcpClient(self.cluster.master.ip,  self.cluster.master.port)
        response = dcp_client2.open_consumer("mystream")
        assert response['status'] == SUCCESS

        response = self.dcp_client.add_stream(0, 0)
        assert response['status'] == ERR_ECLIENT

        response = dcp_client2.add_stream(0, 0)
        assert response['status'] == SUCCESS

        dcp_client2.close()

    """
    Add a stream to consumer with the takeover flag set = 1.  Expects add stream
    command to return successfully.
    """

    @unittest.skip("invalid: MB-11890")
    def test_add_stream_takeover(self):

        response = self.dcp_client.open_consumer("mystream")
        assert response['status'] == SUCCESS

        response = self.dcp_client.add_stream(0, 1)
        assert response['status'] == SUCCESS

    """
        Open n consumer connection.  Add one stream to each consumer for the same
        vbucket.  Expects every add stream request to succeed.
    """

    @unittest.skip("invalid: MB-11890")
    def test_add_stream_n_consumers_1_stream(self):

        n = 16
        self.verification_seqno = n

        conns = [DcpClient(self.cluster.master.ip,  self.cluster.master.port) for i in xrange(n)]
        for i in xrange(n):
            response = self.mcd_client.set('key' + str(i), 0, 0, 'value', 0)

            stream = "mystream{0}".format(i)
            response = conns[i].open_consumer(stream)
            assert response['status'] == SUCCESS

            response = conns[i].add_stream(0, 1)
            assert response['status'] == SUCCESS

        stats = self.mcd_client.stats('dcp')
        assert stats['ep_dcp_count'] == str(n)

        self.wait_for_persistence(self.mcd_client)

    """
        Open n consumer connection.  Add n streams to each consumer for unique vbucket
        per connection. Expects every add stream request to succeed.
    """

    @unittest.skip("invalid: MB-11890")
    def test_add_stream_n_consumers_n_streams(self):
        n = 8
        self.verification_seqno = n

        vb_ids = self.all_vbucket_ids()
        conns = [DcpClient(self.cluster.master.ip,  self.cluster.master.port) for i in xrange(n)]
        for i in xrange(n):
            self.mcd_client.set('key' + str(i), 0, 0, 'value', 0)

            stream = "mystream{0}".format(i)
            response = conns[i].open_consumer(stream)
            assert response['status'] == SUCCESS

            for vb in vb_ids[0:n]:
                response = conns[i].add_stream(vb, 0)
                assert response['status'] == SUCCESS

        stats = self.mcd_client.stats('dcp')
        assert stats['ep_dcp_count'] == str(n)

    """
        Open a single consumer and add stream for all active vbuckets with the
        takeover flag set in the request.  Expects every add stream request to succeed.
    """

    @unittest.skip("invalid: MB-11890")
    def test_add_stream_takeover_all_vbuckets(self):

        response = self.dcp_client.open_consumer("mystream")
        assert response['status'] == SUCCESS

        # parsing keys: 'vb_1', 'vb_0',...
        vb_ids = self.all_vbucket_ids()
        for i in vb_ids:
            response = self.dcp_client.add_stream(i, 1)
            assert response['status'] == SUCCESS

    @unittest.skip("invalid: MB-11890")
    def test_add_stream_various_ops(self):
        """ verify consumer can receive mutations created by various mcd ops """

        response = self.dcp_client.open_consumer("mystream")
        assert response['status'] == SUCCESS

        val = 'base-'
        self.mcd_client.set('key', 0, 0, val, 0)

        for i in range(100):
            # append + prepend
            self.mcd_client.append('key', str(i), 0, 0)
            val += str(i)
            self.mcd_client.prepend('key', str(i), 0, 0)
            val = str(i) + val

        self.mcd_client.incr('key2', init=0, vbucket=0)
        for i in range(100):
            self.mcd_client.incr('key2', amt=2, vbucket=0)
        for i in range(100):
            self.mcd_client.decr('key2', amt=2, vbucket=0)

        response = self.dcp_client.add_stream(0, 0)
        assert response['status'] == SUCCESS
        stats = self.mcd_client.stats('dcp')
        mutations = stats['eq_dcpq:mystream:stream_0_start_seqno']
        assert mutations == '402'

        self.verification_seqno = 402

    def test_stream_request_deduped_items(self):
        """ request a duplicate mutation """
        response = self.dcp_client.open_producer("mystream")

        # get vb uuid
        response = self.mcd_client.stats('failovers')
        vb_uuid = long(response['vb_0:0:id'])

        self.mcd_client.set('snap1', 0, 0, 'value1', 0)
        self.mcd_client.set('snap1', 0, 0, 'value2', 0)
        self.mcd_client.set('snap1', 0, 0, 'value3', 0)

        # attempt to request mutations 1 and 2
        start_seqno = 1
        end_seqno = 2
        stream = self.dcp_client.stream_req(0, 0,
                                            start_seqno,
                                            end_seqno,
                                            vb_uuid)

        assert stream.status is SUCCESS
        stream.run()
        assert stream.last_by_seqno == 3

        self.verification_seqno == 3

    def test_stream_request_dupe_backfilled_items(self):
        """ request mutations across memory/backfill mutations"""
        self.dcp_client.open_producer("mystream")

        def load(i):
            """ load 3 and persist """
            set_ops = [self.mcd_client.set('key%s' % i, 0, 0, 'value', 0) \
                       for x in range(3)]
            self.wait_for_persistence(self.mcd_client)

        def stream(end, vb_uuid):
            backfilled = False

            # send a stream request mutations from 1st snapshot
            stream = self.dcp_client.stream_req(0, 0, 0, end, vb_uuid)

            # check if items were backfilled before streaming
            stats = self.mcd_client.stats('dcp')
            num_backfilled = \
                int(stats['eq_dcpq:mystream:stream_0_backfill_sent'])

            if num_backfilled > 0:
                backfilled = True

            stream.run()  # exaust stream
            assert stream.has_response() == False

            self.dcp_client.close_stream(0)
            return backfilled

        # get vb uuid
        resp = self.mcd_client.stats('failovers')
        vb_uuid = long(resp['vb_0:0:id'])

        # load stream snapshot 1
        load('a')
        stream(3, vb_uuid)

        # load some more items
        load('b')

        # attempt to stream until request contains backfilled items
        tries = 10
        backfilled = stream(4, vb_uuid)
        while not backfilled and tries > 0:
            tries -= 1
            self.sleep(2)
            backfilled = stream(4, vb_uuid)

        assert backfilled, "ERROR: no back filled items were streamed"

        self.verification_seqno = 6

    def test_backfill_from_default_vb_uuid(self):
        """ attempt a backfill stream request using vb_uuid = 0 """

        def disk_stream():
            stream = self.dcp_client.stream_req(0, 0, 0, 1, 0)
            last_by_seqno = 0
            persisted = False

            assert stream.status is SUCCESS
            snap = stream.next_response()
            if snap['flag'].find('disk') == 0:
                persisted = True

            return persisted

        self.dcp_client.open_producer("mystream")
        self.mcd_client.set('key', 0, 0, 'value', 0)

        tries = 20
        while tries > 0 and not disk_stream():
            tries -= 1
            self.sleep(1)

        assert tries > 0, "Items never persisted to disk"

    """Close stream that has not been initialized.
    Expects client error."""

    @unittest.skip("Needs Debug")
    def test_close_stream_command(self):
        response = self.dcp_client.close_stream(0)
        assert response['status'] == ERR_ECLIENT

    """Close a consumer stream. Expects close operation to
    return a success."""

    @unittest.skip("invalid: MB-11890")
    def test_close_consumer_stream(self):

        response = self.dcp_client.open_consumer("mystream")
        assert response['status'] == SUCCESS

        response = self.dcp_client.add_stream(0, 0)
        assert response['status'] == SUCCESS

        response = self.dcp_client.close_stream(0)
        assert response['status'] == SUCCESS

    """
        Open a consumer connection.  Add stream for a selected vbucket.  Then close stream.
        Immediately after closing stream send a request to add stream again.  Expects that
        stream can be added after closed.
    """

    @unittest.skip("invalid: MB-11890")
    def test_close_stream_reopen(self):
        response = self.dcp_client.open_consumer("mystream")
        assert response['status'] == SUCCESS

        response = self.dcp_client.add_stream(0, 0)
        assert response['status'] == SUCCESS

        response = self.dcp_client.add_stream(0, 0)
        assert response['status'] == ERR_KEY_EEXISTS

        response = self.dcp_client.close_stream(0)
        assert response['status'] == SUCCESS

        response = self.dcp_client.add_stream(0, 0)
        assert response['status'] == SUCCESS

    """
        open and close stream as a consumer then takeover
        stream as producer and attempt to reopen stream
        from same vbucket
    """

    @unittest.skip("invalid scenario: MB-11785")
    def test_close_stream_reopen_as_producer(self):
        response = self.dcp_client.open_consumer("mystream")
        assert response['status'] == SUCCESS

        response = self.dcp_client.add_stream(0, 0)
        assert response['status'] == SUCCESS

        response = self.dcp_client.close_stream(0)
        assert response['status'] == SUCCESS

        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        response = self.dcp_client.stream_req(0, 0, 0, 0, 0, 0)
        assert response.status == SUCCESS

        response = self.dcp_client.open_consumer("mystream")
        assert response['status'] == SUCCESS

        response = self.dcp_client.close_stream(0)
        assert response['status'] == ERR_KEY_ENOENT

    """
        Add stream to a consumer connection for a selected vbucket.  Start sending ops to node.
        Send close stream command to selected vbucket.  Expects that consumer has not recieved any
        subsequent mutations after producer recieved the close request.
    """

    def test_close_stream_with_ops(self):

        stream_closed = False

        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        doc_count = 1000
        for i in range(doc_count):
            self.mcd_client.set('key%s' % i, 0, 0, 'value', 0)

        stream = self.dcp_client.stream_req(0, 0, 0, doc_count, 0)
        while stream.has_response():

            response = stream.next_response()
            if not stream_closed:
                response = self.dcp_client.close_stream(0)
                assert response['status'] == SUCCESS, response
                stream_closed = True

            if response is None:
                break

        assert stream.last_by_seqno < doc_count, \
            "Error: recieved all mutations on closed stream"

        self.verification_seqno = doc_count

    """
        Sets up a consumer connection.  Adds stream and then sends 2 close stream requests.  Expects
        second request to close stream returns noent

    """

    def test_close_stream_twice(self):

        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        response = self.dcp_client.stream_req(0, 0, 0, 1000, 0)
        assert response.status == SUCCESS

        response = self.dcp_client.close_stream(0)
        assert response['status'] == SUCCESS

        response = self.dcp_client.close_stream(0)
        assert response['status'] == ERR_KEY_ENOENT

    """
        Test verifies that if multiple consumers are streaming from a vbucket
        that if one of the consumer closes then the producer doesn't stop
        sending changes to other consumers
    """

    @unittest.skip("invalid: MB-11890")
    def test_close_stream_n_consumers(self):

        n = 16
        for i in xrange(100):
            self.mcd_client.set('key' + str(i), 0, 0, 'value', 0)
        self.wait_for_persistence(self.mcd_client)

        # add stream to be close by different client
        client2 = DcpClient(self.cluster.master.ip,  self.cluster.master.port)
        closestream = "closestream"
        client2.open_consumer(closestream)
        client2.add_stream(0, 0)

        conns = [DcpClient(self.cluster.master.ip,  self.cluster.master.port) for i in xrange(n)]

        for i in xrange(n):

            stream = "mystream{0}".format(i)
            conns[i].open_consumer(stream)
            conns[i].add_stream(0, 1)
            if i == int(n / 2):
                # close stream
                response = client2.close_stream(0)
                assert response['status'] == SUCCESS

        self.sleep(2)
        stats = self.mcd_client.stats('dcp')
        key = "eq_dcpq:{0}:stream_0_state".format(closestream)
        assert stats[key] == 'dead'

        for i in xrange(n):
            key = "eq_dcpq:mystream{0}:stream_0_state".format(i)
            assert stats[key] in ('reading', 'pending')

        client2.close()

        self.verification_seqno = 100

    """Request failover log without connection

    attempts to retrieve failover log without establishing a connection to
    a producer.  Expects operation is not supported"""

    @unittest.skip("Needs Debug")
    def test_get_failover_log_command(self):
        response = self.dcp_client.get_failover_log(0)
        assert response['status'] == ERR_ECLIENT

    """Request failover log from consumer

    attempts to retrieve failover log from a consumer.  Expects
    operation is not supported."""

    def test_get_failover_log_consumer(self):

        response = self.dcp_client.open_consumer("mystream")
        assert response['status'] == SUCCESS

        response = self.dcp_client.get_failover_log(0)
        assert response['status'] == ERR_ECLIENT

    """Request failover log from producer

    retrieve failover log from a producer. Expects to successfully recieve
    failover log and for it to match dcp stats."""

    def test_get_failover_log_producer(self):

        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        response = self.dcp_client.get_failover_log(0)
        assert response['status'] == SUCCESS

        response = self.mcd_client.stats('failovers')
        assert response['vb_0:0:seq'] == '0'

    """Request failover log from invalid vbucket

    retrieve failover log from invalid vbucket. Expects to not_my_vbucket from producer."""

    def test_get_failover_invalid_vbucket(self):

        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        response = self.dcp_client.get_failover_log(1025)
        assert response['status'] == ERR_NOT_MY_VBUCKET

    """Failover log during stream request

    Open a producer connection and send and add_stream request with high end_seqno.
    While waiting for end_seqno to be reached send request for failover log
    and Expects that producer is still able to return failover log
    while consumer has an open add_stream request.
    """

    def test_failover_log_during_stream_request(self):

        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        stream = self.dcp_client.stream_req(0, 0, 0, 100, 0)
        seqno = stream.failover_log[0][1]
        response = self.dcp_client.get_failover_log(0)

        assert response['status'] == SUCCESS
        assert response['value'][0][1] == seqno

    """Failover log with ops

    Open a producer connection to a vbucket and start loading data to node.
    After expected number of items have been created send request for failover
    log and expect seqno to match number
    """

    @unittest.skip("needs debug")
    def test_failover_log_with_ops(self):

        stream = "mystream"
        response = self.dcp_client.open_producer(stream)
        assert response['status'] == SUCCESS

        stream = self.dcp_client.stream_req(0, 0, 0, 100, 0)
        assert stream.status == SUCCESS
        seqno = stream.failover_log[0][1]

        for i in range(100):
            self.mcd_client.set('key' + str(i), 0, 0, 'value', 0)
            resp = stream.next_response()
            assert resp

            if (i % 10) == 0:
                fail_response = self.dcp_client.get_failover_log(0)
                assert fail_response['status'] == SUCCESS
                assert fail_response['value'][0][1] == seqno

        self.verification_seqno = 100

    """Request failover from n producers from n vbuckets

    Open n producers and attempt to fetch failover log for n vbuckets on each producer.
    Expects expects all requests for failover log to succeed and that the log for
    similar buckets match.
    """

    def test_failover_log_n_producers_n_vbuckets(self):

        n = 2
        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        vb_ids = self.all_vbucket_ids()
        expected_seqnos = {}
        for id_ in vb_ids:
            # print 'id', id_
            response = self.dcp_client.get_failover_log(id_)
            expected_seqnos[id_] = response['value'][0][0]

            # open n producers for this vbucket
            for i in range(n):
                stream = "mystream{0}".format(i)
                conn = DcpClient(self.cluster.master.ip,  self.cluster.master.port)
                # print 'conn', conn
                response = conn.open_producer(stream)
                vbucket_id = id_
                # print 'vbucket_id',vbucket_id
                response = self.dcp_client.get_failover_log(vbucket_id)
                assert response['value'][0][0] == expected_seqnos[vbucket_id]

    """Basic dcp stream request

    Opens a producer connection and sends a stream request command for
    vbucket 0. Since no items exist in the server we should accept the
    stream request and then send back a stream end message."""

    def test_stream_request_command(self):
        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        stream = self.dcp_client.stream_req(0, 0, 0, 0, 0, 0)
        assert stream.opcode == CMD_STREAM_REQ
        end = stream.next_response()
        assert end and end['opcode'] == CMD_STREAM_END

    """Stream request with invalid vbucket

    Opens a producer connection and then tries to create a stream with an
    invalid VBucket. Should get a not my vbucket error."""

    def test_stream_request_invalid_vbucket(self):
        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        response = self.dcp_client.stream_req(1025, 0, 0, MAX_SEQNO, 0, 0)
        assert response.status == ERR_NOT_MY_VBUCKET

        response = self.mcd_client.stats('dcp')
        assert 'eq_dcpq:mystream:stream_0_opaque' not in response
        assert response['eq_dcpq:mystream:type'] == 'producer'

    """Stream request for invalid connection

    Try to create a stream over a non-dcp connection. The server should
    disconnect from the client"""

    @unittest.skip("Needs Debug")
    def test_stream_request_invalid_connection(self):

        response = self.dcp_client.stream_req(0, 0, 0, MAX_SEQNO, 0, 0)
        assert response.status == ERR_ECLIENT

        response = self.mcd_client.stats('dcp')
        assert 'eq_dcpq:mystream:type' not in response

    """Stream request for consumer connection

    Try to create a stream on a consumer connection. The server should
    disconnect from the client"""

    def test_stream_request_consumer_connection(self):
        response = self.dcp_client.open_consumer("mystream")
        assert response['status'] == SUCCESS

        response = self.dcp_client.stream_req(0, 0, 0, MAX_SEQNO, 0)
        assert response.status == ERR_ECLIENT

        response = self.mcd_client.stats('dcp')
        assert 'eq_dcpq:mystream:type' not in response

    """Stream request with start seqno bigger than end seqno

    Opens a producer connection and then tries to create a stream with a start
    seqno that is bigger than the end seqno. The stream should be closed with an
    range error. Now we are getting a client - still correct"""

    def test_stream_request_start_seqno_bigger_than_end_seqno(self):
        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        response = self.dcp_client.stream_req(0, 0, MAX_SEQNO, MAX_SEQNO / 2, 0, 0)
        assert response.status == ERR_ECLIENT or response.status == ERR_ERANGE

        response = self.mcd_client.stats('dcp')
        assert 'eq_dcpq:mystream:stream_0_opaque' not in response

        # dontassert response['eq_dcpq:mystream:type'] == 'producer'

    """Stream requests from the same vbucket

    Opens a stream request for a vbucket to read up to seq 100. Then sends another
    stream request for the same vbucket.  Expect a EXISTS error and dcp stats
    should refer to initial created stream."""

    def test_stream_from_same_vbucket(self):

        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        response = self.dcp_client.stream_req(0, 0, 0, MAX_SEQNO, 0)
        assert response.status == SUCCESS

        response = self.mcd_client.stats('dcp')
        assert response['eq_dcpq:mystream:type'] == 'producer'
        created = response['eq_dcpq:mystream:created']
        assert created >= 0

        response = self.dcp_client.stream_req(0, 0, 0, 100, 0)
        assert response.status == ERR_KEY_EEXISTS

        response = self.mcd_client.stats('dcp')
        assert response['eq_dcpq:mystream:created'] == created

    """Basic dcp stream request (Receives mutations)

    Stores 10 items into vbucket 0 and then creates an dcp stream to
    retrieve those items in order of sequence number.
    """

    def test_stream_request_with_ops(self):

        # self.mcd_client.stop_persistence()

        doc_count = snap_end_seqno = 10

        for i in range(doc_count):
            self.mcd_client.set('key' + str(i), 0, 0, 'value', 0)

        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        mutations = 0
        last_by_seqno = 0
        stream = self.dcp_client.stream_req(0, 0, 0, doc_count, 0, 0)
        assert stream.status == SUCCESS
        stream.run()

        self.wait_for_persistence(self.mcd_client)

        assert stream.last_by_seqno == doc_count

        self.verification_seqno = doc_count

    """Receive mutation from dcp stream from a later sequence

    Stores 10 items into vbucket 0 and then creates an dcp stream to
    retrieve items from sequence number 7 to 10 on (4 items).
    """

    def test_stream_request_with_ops_start_sequence(self):
        # self.mcd_client.stop_persistence()

        for i in range(10):
            self.mcd_client.set('key' + str(i), 0, 0, 'value', 0)

        resp = self.mcd_client.stats('vbucket-seqno')
        end_seqno = int(resp['vb_0:high_seqno'])

        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        resp = self.mcd_client.stats('failovers')
        vb_uuid = long(resp['vb_0:0:id'])
        high_seqno = long(resp['vb_0:0:seq'])

        start_seqno = 7
        stream = self.dcp_client.stream_req(
            0, 0, start_seqno, end_seqno, vb_uuid)

        assert stream.status == SUCCESS

        responses = stream.run()
        mutations = \
            len(filter(lambda r: r['opcode'] == CMD_MUTATION, responses))

        assert stream.last_by_seqno == 10
        assert mutations == 3

        self.verification_seqno = 10

    def set_keys_with_timestamp(self, count):

        for i in range(count):
            self.mcd_client.set('key' + str(i), 0, 0, str(time.time()), 0)
            self.sleep(0.010)

    """ Concurrent set keys and stream them. Verify that the time between new arrivals
    is not greater than 10 seconds
    """

    def test_mutate_stream_request_concurrent_with_ops(self):  # ******

        doc_count = snap_end_seqno = 10000
        t = Thread(target=self.set_keys_with_timestamp, args=(doc_count,))
        t.start()

        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        mutations = 0
        last_by_seqno = 0
        stream = self.dcp_client.stream_req(0, 0, 0, doc_count, 0, 0)
        assert stream.status == SUCCESS
        results = stream.run()

        # remove response like this
        # {'snap_end_seqno': 30, 'arrival_time': 1423699992.195518, 'flag': 'memory', 'opcode': 86, 'snap_start_seqno': 30, 'vbucket': 0}
        resultsWithoutSnap = [x for x in results if 'key' in x]

        pauses = []
        i = 1
        while i < len(resultsWithoutSnap):
            if resultsWithoutSnap[i]['arrival_time'] - resultsWithoutSnap[i - 1]['arrival_time'] > 10:
                pauses.append('Key {0} set at {1} was streamed {2:.2f} seconds after the previous key was received. '.
                              format(resultsWithoutSnap[i]['key'],
                                     datetime.fromtimestamp(
                                         float(resultsWithoutSnap[i - 1]['value'])).strftime('%H:%M:%S'),
                                     resultsWithoutSnap[i]['arrival_time'] - resultsWithoutSnap[i - 1]['arrival_time'],
                                     datetime.fromtimestamp(
                                         float(resultsWithoutSnap[i - 1]['value'])).strftime('%H:%M:%S')))
            i = i + 1

        # print 'Number of pause delays:', len(pauses)
        if len(pauses) > 0:
            if len(pauses) < 20:  # keep the output manageable
                for i in pauses:
                    print(i)
            else:
                for i in range(20):
                    print(pauses[i])

            assert False, 'There were pauses greater than 10 seconds in receiving stream contents'

        assert stream.last_by_seqno == doc_count

    """Basic dcp stream request (Receives mutations/deletions)

    Stores 10 items into vbucket 0 and then deletes 5 of thos items. After
    the items have been inserted/deleted from the server we create an dcp
    stream to retrieve those items in order of sequence number.
    """

    def test_stream_request_with_deletes(self):
        # self.mcd_client.stop_persistence()

        for i in range(10):
            self.mcd_client.set('key' + str(i), 0, 0, 'value', 0)

        for i in range(5):
            self.mcd_client.delete('key' + str(i), 0, 0)

        resp = self.mcd_client.stats('vbucket-seqno')
        end_seqno = int(resp['vb_0:high_seqno'])

        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        last_by_seqno = 0
        stream = self.dcp_client.stream_req(0, 0, 0, end_seqno, 0)
        assert stream.status == SUCCESS
        responses = stream.run()

        mutations = \
            len(filter(lambda r: r['opcode'] == CMD_MUTATION, responses))
        deletions = \
            len(filter(lambda r: r['opcode'] == CMD_DELETION, responses))

        assert mutations == 5
        assert deletions == 5
        assert stream.last_by_seqno == 15

        self.verification_seqno = 15

    """
    MB-13386 - delete and compaction
    Stores 10 items into vbucket 0 and then deletes 5 of those items. After
    the items have been inserted/deleted from the server we create an dcp
    stream to retrieve those items in order of sequence number.
    """

    def test_stream_request_with_deletes_and_compaction(self):

        for i in range(1, 4):
            self.mcd_client.set('key' + str(i), 0, 0, 'value', 0)

        self.sleep(2)
        for i in range(2, 4):
            self.mcd_client.delete('key' + str(i), 0, 0)

        self.sleep(2)

        resp = self.mcd_client.stats('vbucket-seqno')
        end_seqno = int(resp['vb_0:high_seqno'])

        self.wait_for_persistence(self.mcd_client)

        # drop deletes is important for this scenario
        self.mcd_client.compact_db('', 0, 2, 5, 1)  # key, bucket,  purge_before_ts, purge_before_seq, drop_deletes

        # wait for compaction to end - if this were a rest call then we could use active tasks but
        # as this an mc bin client call the only way known (to me) is to sleep
        self.sleep(20)

        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        last_by_seqno = 0
        self.sleep(5)
        stream = self.dcp_client.stream_req(0, 0, 0, end_seqno, 0)
        assert stream.status == SUCCESS
        responses = stream.run()

        print('responses', responses)

        mutations = \
            len(filter(lambda r: r['opcode'] == CMD_MUTATION, responses))
        deletions = \
            len(filter(lambda r: r['opcode'] == CMD_DELETION, responses))

        assert deletions == 1, 'Deletion mismatch, expect {0}, actual {1}'.format(2, deletions)
        assert mutations == 1, 'Mutation mismatch, expect {0}, actual {1}'.format(1, mutations)

        assert stream.last_by_seqno == 5

    """
    MB-13479 - dedup and compaction
    Set some keys
    Consumer consumes them
    Delete one of the set keys
    Compaction - dedup occurs
    Request more of the stream - there should be a rollback so the the consumer does not bridge the dedup

    """

    def test_stream_request_with_dedup_and_compaction(self):

        KEY_BASE = 'key'
        for i in range(1, 4):
            self.mcd_client.set(KEY_BASE + str(i), 0, 0, 'value', 0)

        self.sleep(2)

        resp = self.mcd_client.stats('vbucket-seqno')

        end_seqno = int(resp['vb_0:high_seqno'])

        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        # consume the first 3 keys
        stream = self.dcp_client.stream_req(0, 0, 0, 3, 0)
        assert stream.status == SUCCESS
        responses = stream.run()

        # and delete one from the original batch
        for i in range(2, 4):
            self.mcd_client.delete(KEY_BASE + str(i), 0, 0)

        # set a couple more keys

        self.mcd_client.set(KEY_BASE + str(5), 0, 0, 'value', 0)
        self.mcd_client.set(KEY_BASE + str(5), 0, 0, 'value', 0)

        self.mcd_client.compact_db('', 0, 3, 5, 1)  # key, bucket, ...
        self.sleep(10)

        # and now get the stream
        #     def stream_req(self, vbucket, takeover, start_seqno, end_seqno,
        #               vb_uuid, snap_start = None, snap_end = None):
        stream = self.dcp_client.stream_req(0, 0, 3, 6, 0)
        assert stream.status == ERR_ROLLBACK

    @unittest.skip("Broken: needs debugging")
    def test_stream_request_backfill_deleted(self):
        """ verify deleted mutations can be streamed after backfill
            task has occured """

        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        resp = self.mcd_client.stats('failovers')
        vb_uuid = long(resp['vb_0:0:id'])

        # set 3 items and delete delete first 2
        self.mcd_client.set('key1', 0, 0, 'value', 0)
        self.mcd_client.set('key2', 0, 0, 'value', 0)
        self.mcd_client.set('key3', 0, 0, 'value', 0)
        self.mcd_client.set('key4', 0, 0, 'value', 0)
        self.mcd_client.set('key5', 0, 0, 'value', 0)
        self.mcd_client.set('key6', 0, 0, 'value', 0)
        self.wait_for_persistence(self.mcd_client)
        self.mcd_client.delete('key1', 0, 0)
        self.mcd_client.delete('key2', 0, 0)

        backfilling = False
        tries = 10
        while not backfilling and tries > 0:
            # stream request until backfilling occurs
            self.dcp_client.stream_req(0, 0, 0, 5,
                                       vb_uuid)
            stats = self.mcd_client.stats('dcp')
            num_backfilled = \
                int(stats['eq_dcpq:mystream:stream_0_backfilled'])
            backfilling = num_backfilled > 0
            tries -= 1
            self.sleep(2)

        assert backfilling, "ERROR: backfill task did not start"

        # attempt to stream deleted mutations
        stream = self.dcp_client.stream_req(0, 0, 0, 3, vb_uuid)
        response = stream.next_response()

    """ Stream request with incremental mutations

    Insert some ops and then create a stream that wants to get more mutations
    then there are ops. The stream should pause after it gets the first set.
    Then add some more ops and wait from them to be streamed out. We will insert
    the exact amount of items that the should be streamed out."""

    def test_stream_request_incremental(self):

        for i in range(10):
            self.mcd_client.set('key' + str(i), 0, 0, 'value', 0)

        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        stream = self.dcp_client.stream_req(0, 0, 0, 20, 0)
        assert stream.status == SUCCESS
        stream.run(10)
        assert stream.last_by_seqno == 10

        for i in range(10):
            self.mcd_client.set('key' + str(i + 10), 0, 0, 'value', 0)

        # read remaining mutations
        stream.run()
        assert stream.last_by_seqno == 20

        self.verification_seqno = 20

    """Send stream requests for multiple

    Put some operations into four different vbucket. Then get the end sequence
    number for each vbucket and create a stream to it. Read all of the mutations
    from the streams and make sure they are all sent."""

    def test_stream_request_multiple_vbuckets(self):
        num_vbs = 4
        num_ops = 10
        for vb in range(num_vbs):
            for i in range(num_ops):
                self.mcd_client.set('key' + str(i), 0, 0, 'value', vb)

        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        streams = {}
        stats = self.mcd_client.stats('vbucket-seqno')
        for vb in range(4):
            en = int(stats['vb_%d:high_seqno' % vb])
            stream = self.dcp_client.stream_req(vb, 0, 0, en, 0)
            streams[vb] = {'stream': stream,
                           'mutations': 0,
                           'last_seqno': 0}

        while len(streams) > 0:
            for vb in streams.keys():
                if streams[vb]['stream'].has_response():
                    response = streams[vb]['stream'].next_response()
                    if response['opcode'] == 87:
                        assert response['by_seqno'] > streams[vb]['last_seqno']
                        streams[vb]['last_seqno'] = response['by_seqno']
                        streams[vb]['mutations'] = streams[vb]['mutations'] + 1
                else:
                    assert streams[vb]['mutations'] == num_ops
                    del streams[vb]

    """
        Sends a stream request with start seqno greater than seqno of vbucket.  Expects
        to receive a rollback response with seqno to roll back to
    """

    @unittest.skip("Needs Debug")
    def test_stream_request_rollback(self):
        response = self.dcp_client.open_producer("rollback")
        assert response['status'] == SUCCESS

        self.mcd_client.set('key1', 0, 0, 'value', 0)
        self.mcd_client.set('key2', 0, 0, 'value', 0)

        vb_id = 'vb_0'
        vb_stats = self.mcd_client.stats('vbucket-seqno')
        fl_stats = self.mcd_client.stats('failovers')
        fail_seqno = long(fl_stats[vb_id + ':0:seq'])
        vb_uuid = long(vb_stats[vb_id + ':uuid'])
        rollback = long(vb_stats[vb_id + ':high_seqno'])

        start_seqno = end_seqno = 3
        stream = self.dcp_client.stream_req(0, 0, start_seqno, end_seqno, vb_uuid)

        assert stream.status == ERR_ROLLBACK
        assert stream.rollback == rollback
        assert stream.rollback_seqno == fail_seqno

        start_seqno = end_seqno = rollback
        stream = self.dcp_client.stream_req(0, 0, start_seqno - 1, end_seqno, vb_uuid)
        stream.run()

        assert end_seqno == stream.last_by_seqno

        self.verification_seqno = end_seqno

    """
        Sends a stream request with start seqno greater than seqno of vbucket.  Expects
        to receive a rollback response with seqno to roll back to.  Instead of rolling back
        resend stream request n times each with high seqno's and expect rollback for each attempt.
    """

    @unittest.skip("Needs Debug")
    def test_stream_request_n_rollbacks(self):
        response = self.dcp_client.open_producer("rollback")
        assert response['status'] == SUCCESS

        vb_stats = self.mcd_client.stats('vbucket-seqno')
        vb_uuid = long(vb_stats['vb_0:uuid'])

        for n in range(1000):
            self.mcd_client.set('key1', 0, 0, 'value', 0)

            by_seqno = n + 1
            stream = self.dcp_client.stream_req(0, 0, by_seqno + 1, by_seqno + 2, vb_uuid)
            assert stream.status == ERR_ROLLBACK
            assert stream.rollback_seqno == 0

    """
        Send stream request command from n producers for the same vbucket.  Expect each request
        to succeed for each producer and verify that expected number of mutations are received
        for each request.
    """

    def test_stream_request_n_producers(self):
        clients = []

        for n in range(10):
            client = DcpClient(self.cluster.master.ip,  self.cluster.master.port)
            op = client.open_producer("producer:%s" % n)
            assert op['status'] == SUCCESS
            clients.append(client)

        for n in range(1, 10):
            self.mcd_client.set('key%s' % n, 0, 0, 'value', 0)

            for client in clients:
                stream = client.stream_req(0, 0, 0, n, 0)

                # should never get rollback
                assert stream.status == SUCCESS, stream.status
                stream.run()

                # stream changes and we should reach last seqno
                assert stream.last_by_seqno == n, \
                    "%s != %s" % (stream.last_by_seqno, n)
                self.verification_seqno = stream.last_by_seqno

        [client.close() for client in clients]

    def test_stream_request_needs_rollback(self):

        # load docs
        self.mcd_client.set('key1', 0, 0, 'value', 0)
        self.mcd_client.set('key2', 0, 0, 'value', 0)
        self.mcd_client.set('key3', 0, 0, 'value', 0)

        # failover uuid
        resp = self.mcd_client.stats('failovers')
        vb_uuid = long(resp['vb_0:0:id'])

        # vb_uuid does not exist
        self.dcp_client.open_producer("rollback")
        resp = self.dcp_client.stream_req(0, 0, 1, 3, 0, 1, 1)
        assert resp and resp.status == ERR_ROLLBACK
        assert resp and resp.rollback == 0

        # snap_end > by_seqno
        resp = self.dcp_client.stream_req(0, 0, 1, 3, vb_uuid, 1, 4)
        assert resp and resp.status == SUCCESS, resp.status

        # snap_start > by_seqno
        resp = self.dcp_client.stream_req(0, 0, 4, 4, vb_uuid, 4, 4)
        assert resp and resp.status == ERR_ROLLBACK, resp.status
        assert resp and resp.rollback == 3, resp.rollback

        # fallthrough
        resp = self.dcp_client.stream_req(0, 0, 7, 7, vb_uuid, 2, 7)
        assert resp and resp.status == ERR_ROLLBACK, resp.status
        assert resp and resp.rollback == 3, resp.rollback

    def test_stream_request_after_close(self):
        """
        Load items from producer then close producer and attempt to resume stream request
        """

        doc_count = 100
        self.dcp_client.open_producer("mystream")

        for i in xrange(doc_count):
            self.mcd_client.set('key' + str(i), 0, 0, 'value', 0)
        self.sleep(2)
        self.wait_for_persistence(self.mcd_client)

        resp = self.mcd_client.stats('failovers')
        vb_uuid = long(resp['vb_0:0:id'])

        stream = self.dcp_client.stream_req(0, 0, 0, doc_count,
                                            vb_uuid)

        stream.run(doc_count / 2)
        self.dcp_client.close()

        self.dcp_client = DcpClient(self.cluster.master.ip,  self.cluster.master.port)
        self.dcp_client.open_producer("mystream")
        stream = self.dcp_client.stream_req(0, 0, stream.last_by_seqno,
                                            doc_count, vb_uuid)
        while stream.has_response():
            response = stream.next_response()
            if response['opcode'] == CMD_MUTATION:
                # first mutation should be at location we left off
                assert response['key'] == 'key' + str(doc_count / 2)
                break

    def test_stream_request_notifier(self):
        """Open a notifier consumer and verify mutations are ready
        to be streamed"""

        doc_count = 100
        response = self.dcp_client.open_notifier("notifier")
        assert response['status'] == SUCCESS

        resp = self.mcd_client.stats('failovers')
        vb_uuid = long(resp['vb_0:0:id'])

        notifier_stream = \
            self.dcp_client.stream_req(0, 0, doc_count - 1, 0, vb_uuid)

        for i in range(doc_count):
            self.mcd_client.set('key' + str(i), 0, 0, 'value', 0)

        response = notifier_stream.next_response()
        assert response['opcode'] == CMD_STREAM_END

        self.dcp_client = DcpClient(self.cluster.master.ip,  self.cluster.master.port)
        response = self.dcp_client.open_producer("producer")
        assert response['status'] == SUCCESS

        stream = self.dcp_client.stream_req(0, 0, 0, doc_count, 0)
        assert stream.status == SUCCESS
        stream.run()
        assert stream.last_by_seqno == doc_count
        self.verification_seqno = doc_count

    def test_stream_request_notifier_bad_uuid(self):
        """Wait for mutations from missing vb_uuid"""

        response = self.dcp_client.open_notifier("notifier")
        assert response['status'] == SUCCESS

        # set 1
        self.mcd_client.set('key', 0, 0, 'value', 0)

        # create notifier stream with vb_uuid that doesn't exist
        # expect rollback since this value can never be reached
        vb_uuid = 0
        stream = self.dcp_client.stream_req(0, 0, 1, 0, 0)
        assert stream.status == ERR_ROLLBACK, \
            "ERROR: response expected = %s, received = %s" % \
            (ERR_ROLLBACK, stream.status)

    def test_stream_request_append(self):
        """ stream appended mutations """
        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        val = 'base-'
        self.mcd_client.set('key', 0, 0, val, 0)

        for i in range(100):
            self.mcd_client.append('key', str(i), 0, 0)
            val += str(i)

        self.sleep(1)
        stream = self.dcp_client.stream_req(0, 0, 0, 100, 0)
        assert stream.status == SUCCESS

        responses = stream.run()
        assert stream.last_by_seqno == 101
        assert responses[1]['value'] == val

        self.verification_seqno = 101

    def test_stream_request_prepend(self):
        """ stream prepended mutations """
        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        val = 'base-'
        self.mcd_client.set('key', 0, 0, val, 0)

        for i in range(100):
            self.mcd_client.prepend('key', str(i), 0, 0)
            val = str(i) + val

        self.sleep(1)
        stream = self.dcp_client.stream_req(0, 0, 0, 100, 0)
        assert stream.status == SUCCESS

        responses = stream.run()
        assert stream.last_by_seqno == 101
        assert responses[1]['value'] == val

        self.verification_seqno = 101

    def test_stream_request_incr(self):
        """ stream mutations created by incr command """
        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        val = 'base-'
        self.mcd_client.incr('key', init=0, vbucket=0)

        for i in range(100):
            self.mcd_client.incr('key', amt=2, vbucket=0)

        self.sleep(1)
        stream = self.dcp_client.stream_req(0, 0, 0, 100, 0)
        assert stream.status == SUCCESS

        responses = stream.run()
        assert stream.last_by_seqno == 101
        assert responses[1]['value'] == '200'

        self.verification_seqno = 101

    def test_stream_request_decr(self):
        """ stream mutations created by decr command """
        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        val = 'base-'
        self.mcd_client.decr('key', init=200, vbucket=0)

        for i in range(100):
            self.mcd_client.decr('key', amt=2, vbucket=0)

        self.sleep(1)
        stream = self.dcp_client.stream_req(0, 0, 0, 100, 0)
        assert stream.status == SUCCESS

        responses = stream.run()
        assert stream.last_by_seqno == 101
        assert responses[1]['value'] == '0'

        self.verification_seqno = 101

    def test_stream_request_replace(self):
        """ stream mutations created by replace command """
        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        val = 'base-'
        self.mcd_client.set('key', 0, 0, 'value', 0)

        for i in range(100):
            self.mcd_client.replace('key', 0, 0, 'value' + str(i), 0)

        self.sleep(1)
        stream = self.dcp_client.stream_req(0, 0, 0, 100, 0)
        assert stream.status == SUCCESS

        responses = stream.run()
        assert stream.last_by_seqno == 101
        assert responses[1]['value'] == 'value99'

        self.verification_seqno = 101

    @unittest.skip("needs debug")
    def test_stream_request_touch(self):
        """ stream mutations created by touch command """

        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        val = 'base-'
        self.mcd_client.set('key', 100, 0, 'value', 0)
        self.mcd_client.touch('key', 1, 0)

        stream = self.dcp_client.stream_req(0, 0, 0, 2, 0)
        assert stream.status == SUCCESS

        responses = stream.run()
        assert stream.last_by_seqno == 2
        assert int(responses[1]['expiration']) > 0

        self.wait_for_persistence(self.mcd_client)

        stats = self.mcd_client.stats()
        num_expired = stats['vb_active_expired']
        if num_expired == 0:
            self.verification_seqno = 2
        else:
            assert num_expired == 1
            self.verification_seqno = 3

    def test_stream_request_gat(self):
        """ stream mutations created by get-and-touch command """

        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        val = 'base-'
        self.mcd_client.set('key', 100, 0, 'value', 0)
        self.mcd_client.gat('key', 1, 0)

        stream = self.dcp_client.stream_req(0, 0, 0, 2, 0)
        assert stream.status == SUCCESS

        responses = stream.run()
        assert stream.last_by_seqno == 2
        assert int(responses[1]['expiration']) > 0

        self.wait_for_persistence(self.mcd_client)
        stats = self.mcd_client.stats()
        num_expired = int(stats['vb_active_expired'])
        if num_expired == 0:
            self.verification_seqno = 2
        else:
            assert num_expired == 1
            self.verification_seqno = 3

    def test_stream_request_client_per_vb(self):
        """ stream request muataions from each vbucket with a new client """

        for vb in xrange(8):
            for i in range(1000):
                self.mcd_client.set('key' + str(i), 0, 0, 'value', vb)

        num_vbs = len(self.all_vbucket_ids())
        for vb in xrange(8):

            dcp_client = DcpClient(self.cluster.master.ip,  self.cluster.master.port)
            dcp_client.open_producer("producerstream")
            stream = dcp_client.stream_req(
                vb, 0, 0, 1000, 0)

            mutations = stream.run()
            try:
                assert stream.last_by_seqno == 1000, stream.last_by_seqno
                self.verification_seqno = 1000
            finally:
                dcp_client.close()

    def test_stream_request_mutation_with_flags(self):
        self.dcp_client.open_producer("mystream")
        self.mcd_client.set('key', 0, 2, 'value', 0)
        stream = self.dcp_client.stream_req(0, 0, 0, 1, 0)
        snap = stream.next_response()
        res = stream.next_response()
        item = self.mcd_client.get('key', 0)
        assert res['flags'] == 2
        assert item[0] == 2

    @unittest.skip("Needs Debug")
    def test_flow_control(self):
        """ verify flow control of a 128 byte buffer stream """

        response = self.dcp_client.open_producer("flowctl")
        assert response['status'] == SUCCESS

        buffsize = 128
        response = self.dcp_client.flow_control(buffsize)
        assert response['status'] == SUCCESS

        for i in range(5):
            self.mcd_client.set('key' + str(i), 0, 0, 'value', 0)

        stream = self.dcp_client.stream_req(0, 0, 0, 5, 0)
        required_ack = False

        while stream.has_response():
            resp = stream.next_response()
            if resp is None:
                ack = self.dcp_client.ack(buffsize)
                assert ack is None, ack['error']
                required_ack = True

        assert stream.last_by_seqno == 5
        assert required_ack, "received non flow-controlled stream"

        self.verification_seqno = 5

    " MB-15213 buffer size of zero means no flow control"

    def test_flow_control_buffer_size_zero(self):
        """ verify no flow control for a 0 byte buffer stream """

        response = self.dcp_client.open_producer("flowctl")
        assert response['status'] == SUCCESS

        buffsize = 0
        response = self.dcp_client.flow_control(buffsize)
        assert response['status'] == SUCCESS

        for i in range(5):
            self.mcd_client.set('key' + str(i), 0, 0, 'value', 0)

        stream = self.dcp_client.stream_req(0, 0, 0, 5, 0)
        required_ack = False

        # consume the stream
        while stream.has_response():
            resp = stream.next_response()

        assert stream.last_by_seqno == 5

    @unittest.skip("Needs Debug")
    def test_flow_control_stats(self):
        """ verify flow control stats """

        buffsize = 128
        self.dcp_client.open_producer("flowctl")
        self.dcp_client.flow_control(buffsize)
        self.mcd_client.set('key1', 0, 0, 'valuevaluevalue', 0)
        self.mcd_client.set('key2', 0, 0, 'valuevaluevalue', 0)
        self.mcd_client.set('key3', 0, 0, 'valuevaluevalue', 0)

        def info():
            stats = self.mcd_client.stats('dcp')
            acked = stats['eq_dcpq:flowctl:total_acked_bytes']
            unacked = stats['eq_dcpq:flowctl:unacked_bytes']
            sent = stats['eq_dcpq:flowctl:total_bytes_sent']

            return int(acked), int(sent), int(unacked)

        # all stats 0
        assert all(map(lambda x: x == 0, info()))

        stream = self.dcp_client.stream_req(0, 0, 0, 3, 0)
        self.sleep(10)  # give time for the stats to settle
        acked, sent, unacked = info()
        assert acked == 0

        if unacked != sent:
            print("test_flow_control_stats unacked %d sent %d" % (unacked, sent))
            self.log.info("test_flow_control_stats unacked %d sent %d" % (unacked, sent))

        assert unacked == sent

        # ack received bytes
        last_acked = acked
        while unacked > 0:
            ack = self.dcp_client.ack(buffsize)
            acked, sent, unacked = info()
            assert acked == last_acked + buffsize
            last_acked = acked

        stream.run()
        assert stream.last_by_seqno == 3

        self.verification_seqno = 3

    @unittest.skip("Needs Debug")
    def test_flow_control_stream_closed(self):
        """ close and reopen stream during with flow controlled client"""

        response = self.dcp_client.open_producer("flowctl")
        assert response['status'] == SUCCESS

        buffsize = 128
        response = self.dcp_client.flow_control(buffsize)
        assert response['status'] == SUCCESS

        end_seqno = 5
        for i in range(end_seqno):
            self.mcd_client.set('key' + str(i), 0, 0, 'value', 0)

        resp = self.mcd_client.stats('failovers')
        vb_uuid = long(resp['vb_0:0:id'])

        stream = self.dcp_client.stream_req(0, 0, 0, end_seqno, vb_uuid)
        max_timeouts = 10
        required_ack = False
        last_seqno = 0
        while stream.has_response() and max_timeouts > 0:
            resp = stream.next_response()

            if resp is None:

                # close
                self.dcp_client.close_stream(0)

                # ack
                ack = self.dcp_client.ack(buffsize)
                assert ack is None, ack['error']
                required_ack = True

                # new stream
                stream = self.dcp_client.stream_req(0, 0, last_seqno,
                                                    end_seqno, vb_uuid)
                assert stream.status == SUCCESS, \
                    "Re-open Stream failed"

                max_timeouts -= 1

            elif resp['opcode'] == CMD_MUTATION:
                last_seqno += 1

        # verify stream closed
        assert last_seqno == end_seqno, "Got %s" % last_seqno
        assert required_ack, "received non flow-controlled stream"

        self.verification_seqno = end_seqno

    def test_flow_control_reset_producer(self):
        """ recreate producer with various values max_buffer bytes """
        sizes = [64, 29, 64, 777, 32, 128, 16, 24, 29, 64]

        for buffsize in sizes:
            self.dcp_client = DcpClient(self.cluster.master.ip,  self.cluster.master.port)
            response = self.dcp_client.open_producer("flowctl")
            assert response['status'] == SUCCESS

            response = self.dcp_client.flow_control(buffsize)
            assert response['status'] == SUCCESS

            stats = self.mcd_client.stats('dcp')
            key = 'eq_dcpq:flowctl:max_buffer_bytes'
            conn_bsize = int(stats[key])
            assert conn_bsize == buffsize, \
                '%s != %s' % (conn_bsize, buffsize)

    def test_flow_control_set_buffer_bytes_per_producer(self):
        """ use various buffer sizes between producer connections """

        def max_buffer_bytes(connection):
            stats = self.mcd_client.stats('dcp')
            key = 'eq_dcpq:%s:max_buffer_bytes' % connection
            return int(stats[key])

        def verify(connection, buffsize):
            self.dcp_client = DcpClient(self.cluster.master.ip,  self.cluster.master.port)
            response = self.dcp_client.open_producer(connection)
            assert response['status'] == SUCCESS
            response = self.dcp_client.flow_control(buffsize)
            assert response['status'] == SUCCESS
            producer_bsize = max_buffer_bytes(connection)
            assert producer_bsize == buffsize, \
                "%s != %s" % (producer_bsize, buffsize)

        producers = [("flowctl1", 64), ("flowctl2", 29), ("flowctl3", 128)]

        for producer in producers:
            connection, buffsize = producer
            verify(connection, buffsize)

    @unittest.skip("Needs Debug")
    def test_flow_control_notifier_stream(self):
        """ verifies flow control still works with notifier streams """
        mutations = 100

        # create notifier
        response = self.dcp_client.open_notifier('flowctl')
        assert response['status'] == SUCCESS
        self.dcp_client.flow_control(16)

        # vb uuid
        resp = self.mcd_client.stats('failovers')
        vb_uuid = long(resp['vb_0:0:id'])

        # set to notify when seqno endseqno reached
        notifier_stream = self.dcp_client.stream_req(0, 0, mutations + 1, 0, vb_uuid)

        # persist mutations
        for i in range(mutations):
            self.mcd_client.set('key' + str(i), 0, 0, 'value', 0)
        self.wait_for_persistence(self.mcd_client)

        tries = 10
        while tries > 0:
            resp = notifier_stream.next_response()
            if resp is None:
                self.mcd_client.set('key' + str(i), 0, 0, 'value', 0)
            else:
                if resp['opcode'] == CMD_STREAM_END:
                    break
            tries -= 1

        assert tries > 0, 'notifier never received end stream'

    def test_flow_control_ack_n_vbuckets(self):

        self.dcp_client.open_producer("flowctl")

        mutations = 2
        num_vbs = 8
        buffsize = 64 * num_vbs
        self.dcp_client.flow_control(buffsize)

        for vb in range(num_vbs):
            self.mcd_client.set('key1', 0, 0, 'value', vb)
            self.mcd_client.set('key2', 0, 0, 'value', vb)

        # request mutations
        resp = self.mcd_client.stats('failovers')
        vb_uuid = long(resp['vb_0:0:id'])
        for vb in range(num_vbs):
            self.dcp_client.stream_req(vb, 0, 0, mutations, vb_uuid)

        # ack until all mutations sent
        stats = self.mcd_client.stats('dcp')
        unacked = int(stats['eq_dcpq:flowctl:unacked_bytes'])
        start_t = time.time()
        while unacked > 0:
            ack = self.dcp_client.ack(unacked)
            assert ack is None, ack['error']
            stats = self.mcd_client.stats('dcp')
            unacked = int(stats['eq_dcpq:flowctl:unacked_bytes'])

            assert time.time() - start_t < 150, \
                "timed out waiting for seqno on all vbuckets"

        stats = self.mcd_client.stats('dcp')

        for vb in range(num_vbs):
            key = 'eq_dcpq:flowctl:stream_%s_last_sent_seqno' % vb
            seqno = int(stats[key])
            assert seqno == mutations, \
                "%s != %s" % (seqno, mutations)

            self.wait_for_persistence(self.mcd_client)
            assert self.get_persisted_seq_no(vb) == seqno

    def test_consumer_producer_same_vbucket(self):

        # producer stream request
        response = self.dcp_client.open_producer("producer")
        assert response['status'] == SUCCESS
        stream = self.dcp_client.stream_req(0, 0, 0, 1000, 0)
        assert stream.status is SUCCESS

        # reopen conenction as consumer
        dcp_client2 = DcpClient(self.cluster.master.ip,  self.cluster.master.port)
        response = dcp_client2.open_consumer("consumer")
        assert response['status'] == SUCCESS
        # response = dcp_client2.add_stream(0, 0)
        # assert response['status'] == SUCCESS

        for i in xrange(1000):
            self.mcd_client.set('key%s' % i, 0, 0, 'value', 0)

        stream.run()
        assert stream.last_by_seqno == 1000

        self.verification_seqno = 1000
        dcp_client2.close()

    def test_stream_request_cas(self):

        n = 5
        response = self.dcp_client.open_producer("producer")
        for i in xrange(n):
            self.mcd_client.set('key' + str(i), 0, 0, 'value', 0)

        for i in range(n):
            key = 'key' + str(i)
            rv, cas, _ = self.mcd_client.get(key, 0)
            assert rv == SUCCESS
            self.mcd_client.cas(key, 0, 0, cas, 'new-value', 0)

        self.sleep(2)
        stream = self.dcp_client.stream_req(0, 0, 0, n, 0)
        responses = stream.run()
        mutations = \
            filter(lambda r: r['opcode'] == CMD_MUTATION, responses)
        assert len(mutations) == n
        assert stream.last_by_seqno == 2 * n

        self.verification_seqno = 2 * n

        for doc in mutations:
            assert doc['value'] == 'new-value'

    def test_get_all_seq_no(self):

        res = self.mcd_client.get_vbucket_all_vbucket_seqnos()

        for i in range(1024):
            bucket, seqno = struct.unpack(">HQ", res[2][i * 10:(i + 1) * 10])
            assert bucket == i

    # Check the scenario where time is not synced but we still request extended metadata. There should be no
    # adjusted time but the mutations should appear. This test currently fails - MB-13933

    def test_request_extended_meta_data_when_vbucket_not_time_synced(self):
        n = 5

        response = self.dcp_client.open_producer("producer")
        response = self.dcp_client.general_control('enable_ext_metadata', 'true')
        assert response['status'] == SUCCESS

        for i in xrange(n):
            self.mcd_client.set('key' + str(i), 0, 0, 'value', 0)

        stream = self.dcp_client.stream_req(0, 0, 0, n, 0)
        responses = stream.run()
        assert stream.last_by_seqno == n, \
            'Sequence number mismatch. Expect {0}, actual {1}'.format(n, stream.last_by_seqno)

    """ Tests the for the presence of the adjusted time and conflict resolution mode fields in the mutation and delete
        commands.
    """

    @unittest.skip("deferred from Watson")
    def test_conflict_resolution_and_adjusted_time(self):

        if self.remote_shell.info.type.lower() == 'windows':
            return  # currently not supported on Windows

        n = 5

        response = self.dcp_client.open_producer("producer")
        response = self.dcp_client.general_control('enable_ext_metadata', 'true')
        assert response['status'] == SUCCESS

        # set time synchronization
        self.mcd_client.set_time_drift_counter_state(0, 0, 1)

        for i in xrange(n):
            self.mcd_client.set('key' + str(i), 0, 0, 'value', 0)

        stream = self.dcp_client.stream_req(0, 0, 0, n, 0)
        responses = stream.run()

        assert stream.last_by_seqno == n, \
            'Sequence number mismatch. Expect {0}, actual {1}'.format(n, stream.last_by_seqno)

        for i in responses:
            if i['opcode'] == CMD_MUTATION:
                assert i['nmeta'] > 0, 'nmeta is 0'
                assert i['conflict_resolution_mode'] == 1, 'Conflict resolution mode not set'
                assert i['adjusted_time'] > 0, 'Invalid adjusted time {0}'.format(i['adjusted_time'])

    # This test will insert 100k items into a server,
    # Sets up a stream request. While streaming, will force the
    # server to crash. Reconnect stream and ensure that the
    # number of mutations received is as expected.
    def test_stream_req_with_server_crash(self):
        doc_count = 100000

        for i in range(doc_count):
            self.mcd_client.set('key' + str(i), 0, 0, 'value', 0)
        self.wait_for_persistence(self.mcd_client)
        by_seqno_list = []
        mutation_count = []

        def setup_a_stream(start, end, vb_uuid, snap_start, snap_end, by_seqnos, mutations):
            response = self.dcp_client.open_producer("mystream")
            assert response['status'] == SUCCESS

            stream = self.dcp_client.stream_req(0, 0, start, end, vb_uuid,
                                                snap_start, snap_end)
            assert stream.status == SUCCESS
            stream.run()
            by_seqnos.append(stream.last_by_seqno)
            mutations.append(stream.mutation_count)

        def kill_memcached():
            if self.remote_shell.info.type.lower()== 'windows':
                self._execute_command('taskkill /F /T /IM memcached*')
            else:
                self._execute_command('killall -9 memcached')

        response = self.mcd_client.stats('failovers')
        vb_uuid = long(response['vb_0:0:id'])

        start = 0
        end = doc_count
        proc1 = Thread(target=setup_a_stream,
                       args=(start, end, vb_uuid, start, start, by_seqno_list, mutation_count))

        proc2 = Thread(target=kill_memcached,
                       args=())

        proc1.start()
        proc2.start()
        self.sleep(2)
        proc2.join()
        proc1.join()

        # wait for server to be up
        self.wait_for_warmup(self.cluster.master.ip,  self.cluster.master.port)
        self.dcp_client = DcpClient(self.cluster.master.ip,  self.cluster.master.port)
        self.mcd_client = McdClient(self.cluster.master.ip,  self.cluster.master.port)

        response = self.mcd_client.stats('failovers')
        vb_uuid = long(response['vb_0:0:id'])

        assert len(by_seqno_list)
        start = by_seqno_list[0]
        end = doc_count
        setup_a_stream(start, end, vb_uuid, start, start, by_seqno_list, mutation_count)

        mutations_received_stage_one = mutation_count[0]
        mutations_received_stage_two = mutation_count[1]

        assert (mutations_received_stage_one + mutations_received_stage_two == doc_count)

    def test_track_mem_usage_with_repetetive_stream_req(self):
        doc_count = 100000

        for i in range(doc_count):
            self.mcd_client.set('key' + str(i), 0, 0, 'value', 0)

        self.wait_for_persistence(self.mcd_client)

        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        resp = self.mcd_client.stats()
        memUsed_before = float(resp['mem_used'])

        start = 0
        end = doc_count
        snap_start = snap_end = start

        response = self.mcd_client.stats('failovers')
        vb_uuid = long(response['vb_0:0:id'])

        for i in range(0, 500):
            stream = self.dcp_client.stream_req(0, 0, start, end, vb_uuid,
                                                snap_start, snap_end)
            assert stream.status == SUCCESS
            self.dcp_client.close_stream(0)

        self.sleep(5)

        resp = self.mcd_client.stats()
        memUsed_after = float(resp['mem_used'])

        assert (memUsed_after < ((0.1 * memUsed_before) + memUsed_before))

    """ Test for MB-11951 - streams which were opened and then closed without streaming any data caused problems
    """

    def test_unused_streams(self):

        initial_doc_count = 100

        for i in range(initial_doc_count):
            self.mcd_client.set('key1' + str(i), 0, 0, 'value', 0)

        # open the connection
        response = self.dcp_client.open_producer("mystream")
        assert response['status'] == SUCCESS

        # open the stream
        stream = self.dcp_client.stream_req(0, 0, 0, 1000, 0)

        # and without doing anything close it again
        response = self.dcp_client.close_stream(0)
        assert response['status'] == SUCCESS

        # then do some streaming for real

        doc_count = 100
        for i in range(doc_count):
            self.mcd_client.set('key2' + str(i), 0, 0, 'value', 0)

        # open the stream
        stream = self.dcp_client.stream_req(0, 0, 0, doc_count, 0)
        stream.run(doc_count)
        assert stream.last_by_seqno == doc_count, \
            'Incorrect sequence number. Expect {0}, actual {1}'.format(doc_count, stream.last_by_seqno)
