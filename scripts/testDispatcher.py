import sys
import urllib2
import urllib
import httplib2
import json
import string
import time
from optparse import OptionParser
import traceback

from couchbase import Couchbase
from couchbase.bucket import Bucket
from couchbase.exceptions import CouchbaseError
from couchbase.n1ql import N1QLQuery


# takes an ini template as input, std out is populated with the server pool
# need a descriptor as a parameter

# need a timeout param

POLL_INTERVAL = 60
SERVER_MANAGER = '172.23.105.177:8081'
TEST_SUITE_DB = '172.23.105.177'


def get_num_of_servers(ini_file):
    f = open(ini_file)
    contents = f.read()
    f.close()
    return contents.count('dynamic')


def get_num_of_addpool_servers(ini_file, add_pool_id):
    f = open(ini_file)
    contents = f.read()
    f.close()
    try:
        return contents.count(add_pool_id)
    except Exception:
        return 0


def rreplace(str_obj, pattern, num_replacements):
    return str_obj.rsplit(pattern, num_replacements)[0]


def main():
    usage = '%prog -s suitefile -v version -o OS'
    parser = OptionParser(usage)
    parser.add_option('-v', '--version', dest='version')
    # run is ambiguous but it means 12 hour or weekly
    parser.add_option('-r', '--run', dest='run')
    parser.add_option('-o', '--os', dest='os')
    parser.add_option('-n', '--noLaunch', action="store_true",
                      dest='noLaunch', default=False)
    parser.add_option('-c', '--component', dest='component', default=None)
    parser.add_option('-p', '--poolId', dest='poolId', default='12hour')
    parser.add_option('-a', '--addPoolId', dest='addPoolId', default=None)
    # use the test Jenkins
    parser.add_option('-t', '--test', dest='test', default=False,
                      action='store_true')
    parser.add_option('-s', '--subcomponent', dest='subcomponent',
                      default=None)
    parser.add_option('-e', '--extraParameters', dest='extraParameters',
                      default=None)
    # or could be Docker
    parser.add_option('-y', '--serverType', dest='serverType', default='VM')
    parser.add_option('-u', '--url', dest='url', default=None)
    parser.add_option('-j', '--jenkins', dest='jenkins', default=None)
    parser.add_option('-b', '--branch', dest='branch', default='master')

    # dashboardReportedParameters is of the form param1=abc,param2=def
    parser.add_option('-d', '--dashboardReportedParameters',
                      dest='dashboardReportedParameters', default=None)

    options, args = parser.parse_args()

    release_version = float('.'.join(options.version.split('.')[:2]))
    out_str = ""
    out_str += 'run: %s\n' % options.run
    out_str += 'version: %s\n' % options.version
    out_str += 'release version: %s\n' % release_version
    out_str += 'nolaunch: %s\n' % options.noLaunch
    out_str += 'os: %s\n' % options.os
    out_str += 'url: %s\n' % options.url
    out_str += 'reportedParameters: %s' % options.dashboardReportedParameters
    print(out_str)

    # What do we do with any reported parameters?
    # 1. Append them to the extra (testrunner) parameters
    # 2. Append the right hand of the equals sign to the sub-component
    #    to make a report descriptor

    if options.extraParameters is None:
        if options.dashboardReportedParameters is None:
            runtime_testrunner_params = None
        else:
            runtime_testrunner_params = options.dashboardReportedParameters
    else:
        runtime_testrunner_params = options.extraParameters
        if options.dashboardReportedParameters is not None:
            runtime_testrunner_params = options.extraParameters + ',' \
                                        + options.dashboardReportedParameters

    tests_to_launch = list()
    cb = Bucket('couchbase://' + TEST_SUITE_DB + '/QE-Test-Suites')

    # Start of new logic
    new_doc_format = True
    query_data = dict()
    query_data["new"] = dict()
    query_data["old"] = dict()

    query_data["new"]["select_string"] = \
        "SELECT test_suite.confFile, " \
        "test_suite.config, " \
        "comp.component, " \
        "comp.framework, " \
        "subcomp.subcomponent, " \
        "subcomp.implementedIn, " \
        "subcomp.initNodes, " \
        "subcomp.mailing_list, " \
        "subcomp.mode, " \
        "subcomp.os, " \
        "subcomp.parameters, " \
        "subcomp.slave, " \
        "subcomp.timeOut " \
        "FROM `QE-Test-Suites` test_suite " \
        "LEFT OUTER UNNEST test_suite.components AS comp " \
        "LEFT OUTER UNNEST comp.subcomponents AS subcomp "
    query_data["new"]["partOf"] = "subcomp.partOf"
    query_data["new"]["component"] = "comp.component"
    query_data["new"]["sub_component"] = "subcomp.subcomponent"

    query_data["old"]["select_string"] = "SELECT * FROM `QE-Test-Suites` "
    query_data["old"]["partOf"] = "partOf"
    query_data["old"]["component"] = "component"
    query_data["old"]["sub_component"] = "subcomponent"

    for version in ["new", "old"]:
        if options.run == "12hr_weekly":
            suite_string = "('12hour' IN {0} OR" \
                           " 'weekly' IN {0})" \
                           .format(query_data[version]["partOf"])
        else:
            suite_string = "'" + options.run + "' in subcomp.partOf"

        if options.component is None or options.component == 'None':
            query_string = query_data[version]["select_string"] \
                           + "WHERE " + suite_string + " ORDER BY " \
                           + query_data[version]["component"]
        else:
            if options.subcomponent is None or options.subcomponent == 'None':
                split_components = options.component.split(',')
                component_string = ''
                for i in range(len(split_components)):
                    component_string = component_string \
                                       + "'" + split_components[i] + "'"
                    if i < len(split_components) - 1:
                        component_string = component_string + ','

                query_string = query_data[version]["select_string"] + \
                               "WHERE %s " \
                               "AND %s IN [%s] " \
                               "ORDER BY %s ;" \
                               % (suite_string,
                                  query_data[version]["component"],
                                  component_string,
                                  query_data[version]["component"])
            else:
                # have a subcomponent, assume only 1 component
                split_subcomponents = options.subcomponent.split(',')
                subcomponent_string = ''
                for i in range(len(split_subcomponents)):
                    print('subcomponentString is', subcomponent_string)
                    subcomponent_string = subcomponent_string \
                                          + "'" + split_subcomponents[i] + "'"
                    if i < len(split_subcomponents) - 1:
                        subcomponent_string = subcomponent_string + ','
                query_string = query_data[version]["select_string"] + \
                               "WHERE %s AND %s IN ['%s'] AND " \
                               "%s IN [%s];" \
                               % (suite_string,
                                  query_data[version]["component"],
                                  options.component,
                                  query_data[version]["sub_component"],
                                  subcomponent_string)

        print('Query is:', query_string)
        _ = N1QLQuery(query_string)
        results = cb.n1ql_query(query_string)

        result_rows = list()
        for row in results:
            result_rows.append(row)
        if result_rows:
            if version == "old":
                new_doc_format = False
            break

    framework = None
    for data in result_rows:
        try:
            if not new_doc_format:
                data = data['QE-Test-Suites']
            # trailing spaces causes problems opening the files
            data['config'] = data['config'].rstrip()
            print('row', data)

            # check any os specific
            if 'os' not in data or (data['os'] == options.os) or \
                    (data['os'] == 'linux'
                     and options.os in ['centos', 'ubuntu']):

                # and also check for which release it is implemented in
                if 'implementedIn' not in data \
                        or release_version >= float(data['implementedIn']):
                    if 'jenkins' in data:
                        # then this is sort of a special case,
                        # launch the old style Jenkins job not implemented yet
                        print 'Old style Jenkins', data['jenkins']
                    else:
                        if 'initNodes' in data:
                            init_nodes = data['initNodes'].lower() == 'true'
                        else:
                            init_nodes = True
                        if 'installParameters' in data:
                            install_parameters = data['installParameters']
                        else:
                            install_parameters = 'None'
                        if 'slave' in data:
                            slave = data['slave']
                        else:
                            slave = 'P0'
                        if 'owner' in data:
                            owner = data['owner']
                        else:
                            owner = 'QE'
                        if 'mailing_list' in data:
                            mailing_list = data['mailing_list']
                        else:
                            mailing_list = 'qa@couchbase.com'
                        if 'mode' in data:
                            mode = data["mode"]
                        else:
                            mode = 'java'
                        if 'framework' in data:
                            framework = data["framework"]
                        else:
                            framework = 'testrunner'
                        # if there's an additional pool, get the number
                        # of additional servers needed from the ini
                        add_pool_server_count = get_num_of_addpool_servers(
                            data['config'],
                            options.addPoolId)

                        tests_to_launch.append({
                            'component': data['component'],
                            'subcomponent': data['subcomponent'],
                            'confFile': data['confFile'],
                            'iniFile': data['config'],
                            'serverCount': get_num_of_servers(data['config']),
                            'addPoolServerCount': add_pool_server_count,
                            'timeLimit': data['timeOut'],
                            'parameters': data['parameters'],
                            'initNodes': init_nodes,
                            'installParameters': install_parameters,
                            'slave': slave,
                            'owner': owner,
                            'mailing_list': mailing_list,
                            'mode': mode
                        })
                else:
                    print(data['component'], data['subcomponent'],
                          ' is not supported in this release')
            else:
                print('OS does not apply to', data['component'],
                      data['subcomponent'])

        except Exception as e:
            print('Exception in querying tests, possible bad record: %s' % e)

    print('Tests to launch:')
    for i in tests_to_launch:
        print(i['component'], i['subcomponent'])

    launch_string_base = 'http://qa.sc.couchbase.com/job/test_suite_executor'
    # optional add [-docker] [-Jenkins extension]
    if options.serverType.lower() == 'docker':
        launch_string_base = launch_string_base + '-docker'
    if options.test:
        launch_string_base = launch_string_base + '-test'
    if framework == "jython":
        launch_string_base = launch_string_base + '-jython'
    if framework == "TAF":
        launch_string_base = launch_string_base + '-TAF'
    elif options.jenkins is not None:
        launch_string_base = launch_string_base + '-' + options.jenkins

    # this are VM/Docker dependent - or maybe not
    launch_string = launch_string_base + '/buildWithParameters?' \
                    'token=test_dispatcher&' \
                    'version_number={0}&' \
                    'confFile={1}&' \
                    'descriptor={2}&' \
                    'component={3}&' \
                    'subcomponent={4}&'\
                    'iniFile={5}&' \
                    'parameters={6}&' \
                    'os={7}&' \
                    'initNodes={8}&' \
                    'installParameters={9}&' \
                    'branch={10}&' \
                    'slave={11}&' \
                    'owners={12}&' \
                    'mailing_list={13}&' \
                    'mode={14}&' \
                    'timeout={15}'
    if options.url is not None:
        launch_string = launch_string + '&url=' + options.url

    summary = list()
    while len(tests_to_launch) > 0:
        try:
            # this bit is Docker/VM dependent
            get_avail_url = 'http://' + SERVER_MANAGER + '/getavailablecount/'
            if options.serverType.lower() == 'docker':
                # may want to add OS at some point
                get_avail_url = get_avail_url + 'docker?os=%s&poolId=%s' \
                                            % (options.os, options.poolId)
            else:
                get_avail_url = get_avail_url + '%s?poolId=%s'\
                                                % (options.os, options.poolId)

            response, content = httplib2.Http(timeout=60) \
                                .request(get_avail_url, 'GET')
            if response.status != 200:
                print(time.asctime(time.localtime(time.time())),
                      'invalid server response', content)
                time.sleep(POLL_INTERVAL)
            elif int(content) == 0:
                print time.asctime(time.localtime(time.time())), 'no VMs'
                time.sleep(POLL_INTERVAL)
            else:
                # see if we can match a test
                server_count = int(content)
                print(time.asctime(time.localtime(time.time())),
                      'there are', server_count, ' servers available')

                have_test_to_launch = False
                i = 0
                while not have_test_to_launch and i < len(tests_to_launch):
                    if tests_to_launch[i]['serverCount'] <= server_count:
                        if tests_to_launch[i]['addPoolServerCount']:
                            get_add_pool_url = 'http://' + SERVER_MANAGER \
                                               + '/getavailablecount/'
                            if options.serverType.lower() == 'docker':
                                # may want to add OS at some point
                                get_add_pool_url = get_add_pool_url \
                                                   + 'docker?os=%s&poolId=%s' \
                                                     % (options.os,
                                                        options.addPoolId)
                            else:
                                get_add_pool_url = get_add_pool_url \
                                                   + '%s?poolId=%s' \
                                                     % (options.os,
                                                        options.addPoolId)

                            response, content = httplib2.Http(timeout=60) \
                                                .request(get_add_pool_url,
                                                         'GET')
                            curr_time = \
                                time.asctime(time.localtime(time.time()))
                            if response.status != 200:
                                print(curr_time, 'invalid server response',
                                      content)
                                time.sleep(POLL_INTERVAL)
                            elif int(content) == 0:
                                print(curr_time, 'no %s VMs at this time'
                                      % options.addPoolId)
                                i = i + 1
                            else:
                                print(curr_time,
                                      "there are %d %s servers available"
                                      % (int(content), options.addPoolId))
                                have_test_to_launch = True
                        else:
                            have_test_to_launch = True
                    else:
                        i = i + 1

                if have_test_to_launch:
                    # build the dashboard descriptor
                    dashboard_descriptor = urllib.quote(
                        tests_to_launch[i]['subcomponent'])
                    if options.dashboardReportedParameters is not None:
                        for o in options.dashboardReportedParameters.split(','):
                            dashboard_descriptor += '_' + o.split('=')[1]

                    # and this is the Jenkins descriptor
                    descriptor = urllib.quote(
                        tests_to_launch[i]['component']
                        + '-' + tests_to_launch[i]['subcomponent']
                        + '-' + time.strftime('%b-%d-%X')
                        + '-' + options.version)

                    # grab the server resources
                    # this bit is Docker/VM dependent
                    if options.serverType.lower() == 'docker':
                        get_server_url = \
                            'http://' + SERVER_MANAGER + \
                            '/getdockers/{0}?' \
                            'count={1}&' \
                            'os={2}&' \
                            'poolId={3}'\
                            .format(descriptor,
                                    tests_to_launch[i]['serverCount'],
                                    options.os,
                                    options.poolId)

                    else:
                        get_server_url = \
                            'http://' + SERVER_MANAGER + \
                            '/getservers/{0}?' \
                            'count={1}&' \
                            'expiresin={2}&' \
                            'os={3}&' \
                            'poolId={4}'\
                            .format(descriptor,
                                    tests_to_launch[i]['serverCount'],
                                    tests_to_launch[i]['timeLimit'],
                                    options.os,
                                    options.poolId)
                    print('getServerURL', get_server_url)

                    response, content = httplib2.Http(timeout=60) \
                                        .request(get_server_url, 'GET')
                    print('response.status', response, content)

                    if options.serverType.lower() != 'docker':
                        # sometimes there could be a race,
                        # before a dispatcher process acquires vms,
                        # another waiting dispatcher process could grab them,
                        # resulting in lesser vms
                        # for the second dispatcher process
                        if len(json.loads(content)) != tests_to_launch[i]['serverCount']:
                            continue

                    # get additional pool servers as needed
                    if tests_to_launch[i]['addPoolServerCount']:
                        if options.serverType.lower() == 'docker':
                            get_server_url = \
                                'http://' + SERVER_MANAGER \
                                + '/getdockers/{0}?' \
                                  'count={1}&' \
                                  'os={2}&' \
                                  'poolId={3}' \
                                  .format(descriptor,
                                          tests_to_launch[i]['addPoolServerCount'],
                                          options.os,
                                          options.addPoolId)
                        else:
                            get_server_url = \
                                'http://' + SERVER_MANAGER + \
                                '/getservers/{0}?' \
                                'count={1}&' \
                                'expiresin={2}&' \
                                'os={3}&' \
                                'poolId={4}' \
                                .format(descriptor,
                                        tests_to_launch[i]['addPoolServerCount'],
                                        tests_to_launch[i]['timeLimit'],
                                        options.os,
                                        options.addPoolId)
                        print('getServerURL', get_server_url)

                        response2, content2 = httplib2.Http(timeout=60) \
                                              .request(get_server_url, 'GET')
                        print('response2.status', response2, content2)

                    if response.status == 499 or \
                            (tests_to_launch[i]['addPoolServerCount'] and
                             response2.status == 499):
                        # some error checking here at some point
                        time.sleep(POLL_INTERVAL)
                    else:
                        # Send the request to the test executor
                        # figure out the parameters,
                        # there are test suite specific and
                        # added at dispatch time
                        if runtime_testrunner_params is None:
                            parameters = tests_to_launch[i]['parameters']
                        else:
                            if tests_to_launch[i]['parameters'] == 'None':
                                parameters = runtime_testrunner_params
                            else:
                                parameters = tests_to_launch[i]['parameters'] \
                                             + ',' + runtime_testrunner_params

                        url = launch_string.format(
                            options.version,
                            tests_to_launch[i]['confFile'],
                            descriptor,
                            tests_to_launch[i]['component'],
                            dashboard_descriptor,
                            tests_to_launch[i]['iniFile'],
                            urllib.quote(parameters),
                            options.os,
                            tests_to_launch[i]['initNodes'],
                            tests_to_launch[i]['installParameters'],
                            options.branch,
                            tests_to_launch[i]['slave'],
                            urllib.quote(tests_to_launch[i]['owner']),
                            urllib.quote(tests_to_launch[i]['mailing_list']),
                            tests_to_launch[i]['mode'],
                            tests_to_launch[i]['timeLimit'])

                        if options.serverType.lower() != 'docker':
                            r2 = json.loads(content)
                            servers = json.dumps(r2) \
                                      .replace(' ', '') \
                                      .replace('[', '', 1)
                            servers = rreplace(servers, ']', 1)
                            url = url + '&servers=' + urllib.quote(servers)

                            if tests_to_launch[i]['addPoolServerCount']:
                                add_pool_servers = content2.replace(' ', '') \
                                                   .replace('[', '', 1)
                                add_pool_servers = rreplace(add_pool_servers,
                                                            ']', 1)
                                url = url + '&addPoolServerId=' \
                                      + options.addPoolId \
                                      + '&addPoolServers=' \
                                      + urllib.quote(add_pool_servers)

                        print (time.asctime(time.localtime(time.time())),
                               'Launching:', url)

                        if options.noLaunch:
                            # free the VMs
                            time.sleep(3)
                            if options.serverType.lower() == 'docker':
                                pass # figure docker out later
                            else:
                                response, content = \
                                    httplib2.Http(timeout=60) \
                                    .request('http://' + SERVER_MANAGER +
                                             '/releaseservers/' + descriptor +
                                             '/available', 'GET')
                                print 'the release response', response, content
                        else:
                            response, content = \
                                httplib2.Http(timeout=60).request(url, 'GET')

                        tests_to_launch.pop(i)
                        summary.append({
                            'test': descriptor,
                            'time': time.asctime(time.localtime(time.time()))
                        })
                        if options.noLaunch:
                            # No sleep necessary
                            pass
                        elif options.serverType.lower() == 'docker':
                            # Due to the docker port allocation race
                            time.sleep(240)
                        else:
                            time.sleep(30)
                else:
                    print('not enough servers at this time')
                    time.sleep(POLL_INTERVAL)
            # End of if checking for servers

        except Exception as e:
            print('Exception: %s' % e)
            time.sleep(POLL_INTERVAL)
    # End of while loop

    print('\nDone, everything is launched')
    for i in summary:
        print(i['test'], 'was launched at', i['time'])


if __name__ == "__main__":
    main()
