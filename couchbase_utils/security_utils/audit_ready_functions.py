import json

from membase.api.rest_client import RestConnection
from platform_constants.os_constants import Linux
from remote.remote_util import RemoteMachineShellConnection
from global_vars import logger


class audit:
    AUDITLOGFILENAME = 'audit.log'
    AUDITCONFIGFILENAME = 'audit.json'
    AUDITDESCFILE = 'audit_events.json'
    WINLOGFILEPATH = "C:/Program Files/Couchbase/Server/var/lib/couchbase/logs"
    LINLOGFILEPATH = "/opt/couchbase/var/lib/couchbase/logs"
    MACLOGFILEPATH = "/Users/couchbase/Library/Application Support/Couchbase/var/lib/couchbase/logs"
    WINCONFIFFILEPATH = "C:/Program Files/Couchbase/Server/var/lib/couchbase/config/"
    LINCONFIGFILEPATH = "/opt/couchbase/var/lib/couchbase/config/"
    MACCONFIGFILEPATH = "/Users/couchbase/Library/Application Support/Couchbase/var/lib/couchbase/config/"
    DOWNLOADPATH = "/tmp/"

    def __init__(self,
                 eventID=None,
                 host=None,
                 method='REST'):

        rest = RestConnection(host)
        self.log = logger.get("test")
        if (rest.is_enterprise_edition()):
            self.log.info ("Enterprise Edition, Audit is part of the test")
        else:
            raise Exception(" Install is not an enterprise edition, Audit requires enterprise edition.")
        self.method = method
        self.host = host
        self.nonroot = False
        shell = RemoteMachineShellConnection(self.host)
        self.info = shell.extract_remote_info()
        if self.info.distribution_type.lower() in Linux.DISTRIBUTION_NAME \
                and host.ssh_username != "root":
            self.nonroot = True
        shell.disconnect()
        self.pathDescriptor = self.getAuditConfigElement("descriptors_path") + "/"
        self.pathLogFile = self.getAuditLogPath()
        self.defaultFields = ['id', 'name', 'description']
        if (eventID is not None):
            self.eventID = eventID
            self.eventDef = self.returnEventsDef()

    def getAuditConfigPathInitial(self):
        shell = RemoteMachineShellConnection(self.host)
        os_type = shell.extract_remote_info().distribution_type
        dist_ver = (shell.extract_remote_info().distribution_version).rstrip()
        self.log.info ("OS type is {0}".format(os_type))
        if os_type == 'windows':
            auditconfigpath = audit.WINCONFIFFILEPATH
            self.currentLogFile = audit.WINLOGFILEPATH
        elif os_type == 'Mac':
            if ('10.12' == dist_ver):
                auditconfigpath = "/Users/admin/Library/Application Support/Couchbase/var/lib/couchbase/config/"
                self.currentLogFile = "/Users/admin/Library/Application Support/Couchbase/var/lib/couchbase/logs"
            else:
                auditconfigpath = audit.MACCONFIGFILEPATH
                self.currentLogFile = audit.MACLOGFILEPATH
        else:
            if self.nonroot:
                auditconfigpath = "/home/%s%s" % (self.host.ssh_username,
                                                  audit.LINCONFIGFILEPATH)
                self.currentLogFile = "/home/%s%s" % (self.host.ssh_username,
                                                      audit.LINLOGFILEPATH)
            else:
                auditconfigpath = audit.LINCONFIGFILEPATH
                self.currentLogFile = audit.LINLOGFILEPATH
        return auditconfigpath

    '''
    setAuditConfigPath - External function to set configPATH
    Parameters:
    configPath - path to config file
    Returns : None
    '''
    def setAuditConfigPath(self, configPath):
        self.auditConfigPath = configPath

    '''
    getAuditConfigPath
    Returns - Path to audit config file
    '''
    def getAuditConfigPath(self):
        return self.auditConfigPath

    '''
    readFile - copy file to local '/tmp' directory
    Parameters:
        host - host machine
        remotepath - file path on host machine
        filename - name of file
    Returns:
        None
    '''
    def getRemoteFile(self, host, remotepath, filename):
        shell = RemoteMachineShellConnection(host)
        shell.get_file(remotepath, filename, audit.DOWNLOADPATH)

    def readFile(self, pathAuditFile, fileName):
        self.getRemoteFile(self.host, pathAuditFile, fileName)

    '''
    writeFile - writing the config file
    Parameters:
        pathAuditFile - path to audit config
        fileName - name of audit config file
        lines - lines that need to be copied
    Returns:
        none
    '''
    def writeFile(self, pathAuditFile=None, fileName=None, lines=None):
        if (pathAuditFile is None):
            pathAuditFile = self.getAuditConfigPathInitial()
        if (fileName is None):
            fileName = audit.AUDITCONFIGFILENAME
        shell = RemoteMachineShellConnection(self.host)
        try:
            with open ("/tmp/audit.json", 'w') as outfile:
                json.dump(lines, outfile)
            result = shell.copy_file_local_to_remote('/tmp/audit.json', pathAuditFile + fileName)
        finally:
            shell.disconnect()

    '''
    returnEvent - reads actual audit event from audit.log file and returns last event
    Parameters:
        eventNumber - event number that needs to be queried
    Returns:
        dictionary of actual event from audit.log
    '''
    def returnEvent(self, eventNumber, audit_log_file=None, filtering=False):
        try:
            data = []
            if audit_log_file is None:
                audit_log_file = audit.AUDITLOGFILENAME
            self.readFile(self.pathLogFile, audit_log_file)
            with open(audit.DOWNLOADPATH + audit_log_file) as f:
                for line in f:
                    tempJson = json.loads(line)
                    if (tempJson['id'] == eventNumber):
                        data.append(json.loads(line))
            f.close()
            return data[len(data) - 1]
        except:
            self.log.info("ERROR ---- Event Not Found in audit.log file. Please check the log file")
            if filtering:
                return None

    '''
    check_if_audit_event_generated - Check if audit event is generated
    Parameters:
        audit_log_file - log file name
    Returns:
        True if event generated false otherwise
    '''
    def check_if_audit_event_generated(self, audit_log_file=None):
        data = []
        audit_event_generated = False
        if audit_log_file is None:
            audit_log_file = audit.AUDITLOGFILENAME
        self.readFile(self.pathLogFile, audit_log_file)
        with open(audit.DOWNLOADPATH + audit_log_file) as f:
            for line in f:
                temp_json = json.loads(line)
                if temp_json['id'] == self.eventID:
                    audit_event_generated = True
                    break
        return audit_event_generated

    '''
    getAuditConfigElement - get element of a configuration file
    Parameters
        element - element from audit config file
    Returns
        element of the config file or entire file if element == 'all'
    '''
    def getAuditConfigElement(self, element):
        data = []
        self.readFile(self.getAuditConfigPathInitial(), audit.AUDITCONFIGFILENAME)
        json_data = open (audit.DOWNLOADPATH + audit.AUDITCONFIGFILENAME)
        data = json.load(json_data)
        if (element == 'all'):
            return data
        else:
            return data[element]

    '''
    returnEventsDef - read event definition
    Parameters:None
    Returns:
        list of events from audit_events.json file
    '''
    def returnEventsDef(self):
        data = []
        self.readFile(self.pathDescriptor, audit.AUDITDESCFILE)
        json_data = open (audit.DOWNLOADPATH + audit.AUDITDESCFILE)
        data = json.load(json_data)
        return data

    '''
    getAuditLogPath - return value of log_path from REST API
    Returns:
        returns log_path from audit config file
    '''
    def getAuditLogPath(self):
        rest = RestConnection(self.host)
        content = rest.getAuditSettings()
        return content['logPath'] + "/"

    '''
    getAuditStatus - return value of audit status from REST API
    Returns:
        returns audit status from audit config file
    '''
    def getAuditStatus(self):
        rest = RestConnection(self.host)
        content = rest.getAuditSettings()
        return content['auditdEnabled']

    '''
    getAuditRotateInterval - return value of rotate Interval from REST API
    Returns:
        returns audit status from audit config file
    '''
    def getAuditRotateInterval(self):
        rest = RestConnection(self.host)
        content = rest.getAuditSettings()
        return content['rotateInterval']

    '''
    setAuditLogPath - set log_path via REST API
    Parameter:
        auditLogPath - path to log_path
    Returns:
        status - status rest command
    '''
    def setAuditLogPath(self, auditLogPath):
        rest = RestConnection(self.host)
        status = rest.setAuditSettings(logPath=auditLogPath)
        return status

    '''
    setAuditEnable - set audit_enabled via REST API
    Parameter:
        audit_enabled - true/false for setting audit status
    Returns:
        status - status rest command
    '''
    def setAuditEnable(self, auditEnable):
        rest = RestConnection(self.host)
        status = rest.setAuditSettings(enabled=auditEnable, logPath=self.currentLogFile)
        return status

    '''
    setAuditFeatureDisabled - Disabled a feature from being Audited
    Parameter:
        disabled - List of event id's to be disabled
    Returns:
        status - status rest command    
    '''
    def setAuditFeatureDisabled(self, disabled=None):
        rest = RestConnection(self.host)
        status = rest.setAuditSettings(disabled=disabled)
        return status

    """
    setWhiteListUsers - Whitelist users so Audit events are not logged
    Parameter:
        users - Comma seperated list of whitelisted users in the format username/local or username/external
    Returns:
        status - status rest command  
    """
    def setWhiteListUsers(self, users=''):
        rest = RestConnection(self.host)
        status = rest.setAuditSettings(users=users)
        return status

    '''
    checkConfig - Wrapper around audit class
    Parameters:
        expectedResult - dictionary of fields and value for event
    '''
    def checkConfig(self, expectedResults):
        fieldVerification, valueVerification = self.validateEvents(expectedResults,n1ql_audit)
        self.assertTrue(fieldVerification, "One of the fields is not matching")
        self.assertTrue(valueVerification, "Values for one of the fields is not matching")

    '''
    setAuditRotateInterval - set rotate_internval via REST API
    Parameter:
        rotate_internval - log rotate interval
    Returns:
        status - status rest command
    '''
    def setAuditRotateInterval(self, rotateInterval):
        rest = RestConnection(self.host)
        status = rest.setAuditSettings(rotateInterval=rotateInterval, logPath=self.currentLogFile)
        return status

    '''
    getTimeStampFirstEvent - timestamp of first event in audit.log
    Returns:
        timestamp of first event in audit.log
    '''
    def getTimeStampFirstEvent(self):
        self.readFile(self.pathLogFile, audit.AUDITLOGFILENAME)
        with open(audit.DOWNLOADPATH + audit.AUDITLOGFILENAME) as f:
            data = line = f.readline()
        data = ((json.loads(line))['timestamp'])[:19]
        return data


    '''
    returnFieldsDef - returns event definition separated by sections
    Parameters:
        data - audit_events.json file in a list of dictionary
        eventNumber - event number that needs to be queried
    Returns:
        defaultFields - dictionary of default fields
        mandatoryFields - list is dictionary of mandatory fields
        mandatorySecLevel - list of dictionary containing 2nd level of mandatory fields
        optionalFields - list is dictionary of optional fields
        optionalSecLevel - list of dictionary containing 2nd level of optional fields
    '''
    def returnFieldsDef(self, data, eventNumber):
        defaultFields = {}
        mandatoryFields = []
        mandatorySecLevel = []
        optionalFields = []
        optionalSecLevel = []
        fields = ['mandatory_fields', 'optional_fields']
        for items in data['modules']:
            for particulars in items['events']:
                if particulars['id'] == eventNumber:
                    for key, value in particulars.items():
                        if (key not in fields):
                            defaultFields[key] = value
                        elif key == 'mandatory_fields':
                            for items in particulars['mandatory_fields']:
                                mandatoryFields.append(items)
                                if (isinstance((particulars['mandatory_fields'][items.encode('utf-8')]), dict)):
                                    tempStr = items
                                    for secLevel in particulars['mandatory_fields'][items].items():
                                        tempStr = tempStr + ":" + secLevel[0]
                                    mandatorySecLevel.append(tempStr)
                        elif key == 'optional_fields':
                            for items in particulars['optional_fields']:
                                optionalFields.append(items)
                                if (isinstance((particulars['optional_fields'][items.encode('utf-8')]), dict)):
                                    tempStr = items
                                    for secLevel in particulars['optional_fields'][items].items():
                                        tempStr = tempStr + ":" + secLevel[0]
                                    optionalSecLevel.append(tempStr)

        #self.log.info ("Value of default fields is - {0}".format(defaultFields))
        #self.log.info ("Value of mandatory fields is {0}".format(mandatoryFields))
        #self.log.info ("Value of mandatory sec level is {0}".format(mandatorySecLevel))
        #self.log.info ("Value of optional fields i {0}".format(optionalFields))
        #self.log.info ("Value of optional sec level is {0}".format(optionalSecLevel))
        return defaultFields, mandatoryFields, mandatorySecLevel, optionalFields, optionalSecLevel

    '''
    returnFieldsDef - returns event definition separated by sections
    Parameters:
        data - event from audit.log file in a list of dictionary
        eventNumber - event number that needs to be queried
        module - Name of the module
        defaultFields - dictionary of default fields
        mandatoryFields - list is dictionary of mandatory fields
        mandatorySecLevel - list of dictionary containing 2nd level of mandatory fields
        optionalFields - list is dictionary of optional fields
        optionalSecLevel - list of dictionary containing 2nd level of optional fields
    Returns:
        Boolean - True if all field names match
    '''
    def validateFieldActualLog(self, data, eventNumber, module, defaultFields, mandatoryFields, manFieldSecLevel=None, optionalFields=None, optFieldSecLevel=None, method="Rest", n1ql_audit=False):
        flag = True
        for items in defaultFields:
            #self.log.info ("Default Value getting checked is - {0}".format(items))
            if items not in data:
                self.log.info (" Default value not matching with expected expected value is - {0}".format(items))
                flag = False
        for items in mandatoryFields:
            self.log.info ("Top Level Mandatory Field Default getting checked is - {0}".format(items))
            if items in data:
                if (isinstance ((data[items]), dict)):
                    for items1 in manFieldSecLevel:
                        tempStr = items1.split(":")
                        if tempStr[0] == items:
                            for items in data[items]:
                                #self.log.info ("Second Level Mandatory Field Default getting checked is - {0}".format(items))
                                if (items not in tempStr and method is not 'REST'):
                                    #self.log.info (" Second level Mandatory field not matching with expected expected value is - {0}".format(items))
                                    flag = False
            else:
                flag = False
                if (method == 'REST' and items == 'sessionid'):
                    flag = True
                self.log.info (" Top level Mandatory field not matching with expected expected value is - {0}".format(items))
        for items in optionalFields:
            self.log.info ("Top Level Optional Field Default getting checked is - {0}".format(items))
            if items in data:
                if (isinstance ((data[items]), dict)):
                    for items1 in optFieldSecLevel:
                        tempStr = items1.split(":")
                        if tempStr[0] == items:
                            for items in data[items]:
                                #self.log.info ("Second Level Optional Field Default getting checked is - {0}".format(items))
                                if (items not in tempStr and method is not 'REST'):
                                    self.log.info (" Second level Optional field not matching with expected expected value is - {0}".format(items))
                                    #flag = False
            else:
                #flag = False
                if (method == 'REST' and items == "sessionid"):
                    flag = True
                self.log.info (" Top level Optional field not matching with expected expected value is - {0}".format(items))
        if n1ql_audit:
            flag = True
        return flag

    '''
    validateData - validate data from audit.log with expected Result
    Parameters:
        data - event data from audit.log, based on eventID
        expectedResult - dictionary of expected Result to be validated
    Results:
        Boolean - True if data from audit.log matches with expectedResult
    '''

    def validateData(self, data, expectedResult):
        self.log.info (" Event from audit.log -- {0}".format(data))
        flag = True
        ignore = False
        for items in data:
            if items not in expectedResult:
                expectedResult[items] = data[items]
            if items == 'timestamp':
                tempFlag = self.validateTimeStamp(data['timestamp'])
                if (tempFlag is False):
                    flag = False
            else:
                if (isinstance(data[items], dict)):
                    for seclevel in data[items]:
                        tempLevel = items + ":" + seclevel
                        if (tempLevel in expectedResult.keys()):
                            tempValue = expectedResult[tempLevel]
                        else:
                            if seclevel in expectedResult.keys():
                                tempValue = expectedResult[seclevel]
                            else:
                                ignore = True
                                tempValue = data[items][seclevel]
                        if (seclevel == 'port' and data[items][seclevel] >= 30000 and data[items][seclevel] <= 65535):
                            self.log.info ("Matching port is an ephemeral port -- actual port is {0}".format(data[items][seclevel]))
                        else:
                            if not ignore:
                                self.log.info('expected values - {0} -- actual value -- {1} - eventName - {2}'
                                         .format(tempValue,data[items][seclevel], tempLevel))
                            if data[items][seclevel] != tempValue:
                                self.log.info('Mis-Match Found expected values - {0} -- actual value -- {1} - eventName - {2}'
                                         .format(tempValue, data[items][seclevel], tempLevel))
                                flag = False
                        ignore = False
                else:
                    if (items == 'port' and data[items] >= 30000 and data[items] <= 65535):
                        self.log.info ("Matching port is an ephemeral port -- actual port is {0}".format(data[items]))
                    else:
                        if items == "requestId" or items == 'clientContextId':
                            expectedResult[items] = data[items]
                        self.log.info ('expected values - {0} -- actual value -- {1} - eventName - {2}'.format(expectedResult[items.encode('utf-8')], data[items.encode('utf-8')], items))
                        if (items == 'peername'):
                            if (expectedResult[items] not in data[items]):
                                flag = False
                                self.log.info ('Mis - Match Found expected values - {0} -- actual value -- {1} - eventName - {2}'.format(expectedResult[items.encode('utf-8')], data[items.encode('utf-8')], items))
                        else:
                            if (data[items] != expectedResult[items]):
                                flag = False
                                self.log.info ('Mis - Match Found expected values - {0} -- actual value -- {1} - eventName - {2}'.format(expectedResult[items.encode('utf-8')], data[items.encode('utf-8')], items))
            ignore = False
        return flag

    '''
    validateDate - validate date from audit.log and current date
    Parameters:
        actualDate - timestamp captured from audit.log for event
    Results:
        Boolean - True if difference of timestamp is < 30 seconds
    '''
    def validateTimeStamp(self, actualTime=None):
        try:
            self.log.info(actualTime)
            date = actualTime[:10]
            hourMin = actualTime[11:16]
            tempTimeZone = actualTime[-6:]
            shell = RemoteMachineShellConnection(self.host)
            try:
                currDate = shell.execute_command('date +"%Y-%m-%d"')
                currHourMin = shell.execute_command('date +"%H:%M"')
                currTimeZone = shell.execute_command('date +%z')
            finally:
                shell.disconnect()
            self.log.info (" Matching expected date - currDate {0}; actual Date - {1}".format(currDate[0][0], date))
            self.log.info (" Matching expected time - currTime {0} ; actual Time - {1}".format(currHourMin[0][0], hourMin))
            if date != currDate[0][0].rstrip():
                self.log.info('Compare date')
                self.log.info ("Mis-match in values for timestamp - date")
                return False
            else:
                self.log.info('Compare hours and minutes')
                if ((int((hourMin.split(":"))[0])) != (int((currHourMin[0][0].split(":"))[0]))) or ((int((hourMin.split(":"))[1]) + 10) < (int((currHourMin[0][0].split(":"))[1]))):
                    self.log.info ("Mis-match in values for timestamp - time")
                    return False
                else:
                    self.log.info('Compare timezone')
                    tempTimeZone = tempTimeZone.replace(":", "")
                    if (tempTimeZone != currTimeZone[0][0].rstrip()):
                        self.log.info("Mis-match in value of timezone. Actual: %s Expected: %s" %(tempTimeZone, currTimeZone[0][0].rstrip()))
                        return False

        except Exception, e:
            self.log.info ("Value of execption is {0}".format(e))
            return False


    '''
    validateEvents - external interface to validate event definition and value from audit.log
    Parameters:
        expectedResults - dictionary of keys as fields in audit.log and expected values for reach
    Returns:
        fieldVerification - Boolean - True if all matching fields have been found.
        valueVerification - Boolean - True if data matches with expected Results
    '''
    def validateEvents(self, expectedResults, n1ql_audit=False):
        defaultField, mandatoryFields, mandatorySecLevel, optionalFields, optionalSecLevel = self.returnFieldsDef(self.eventDef, self.eventID)
        actualEvent = self.returnEvent(self.eventID)
        fieldVerification = self.validateFieldActualLog(actualEvent, self.eventID, 'ns_server', self.defaultFields, mandatoryFields, \
                                                    mandatorySecLevel, optionalFields, optionalSecLevel, self.method, n1ql_audit)
        expectedResults = dict(defaultField.items() + expectedResults.items())
        valueVerification = self.validateData(actualEvent, expectedResults)
        return fieldVerification, valueVerification

    '''
    Make sure audit log is empty
    '''
    def validateEmpty(self):
        actualEvent = self.returnEvent(self.eventID, filtering=True)
        if actualEvent:
            return False, actualEvent
        else:
            return True, actualEvent

    def checkLastEvent(self):
        try:
            actualEvent = self.returnEvent(self.eventID)
            return self.validateTimeStamp(actualEvent['timestamp'])
        except:
            return False
