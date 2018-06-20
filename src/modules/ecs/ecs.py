"""
DELL EMC ECS API Data Collection Module.
"""
import requests
from requests.auth import HTTPBasicAuth


class ECSAuthentication(object):
    """
    Stores ECS Authentication Information
    """
    def __init__(self, protocol, host, username, password, port, logger):
        self.protocol = protocol
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.logger = logger
        self.logger.info('ECSAuthentication::Object instance initialization complete.')
        self.url = "{0}://{1}:{2}".format(self.protocol, self.host, self.port)
        self.token = ''

    def get_url(self):
        """
        Returns an ECS Management url made from protocol, host and port.
        """
        return self.url

    def get_token(self):
        """
        Returns an ECS Management token
        """
        return self.tokens

    def connect(self):
        """
        Connect to ECS and if successful update token
        """
        self.logger.info('ECSAuthentication::connect()::We are about to attempt to connect with the following URL : '
                         + "{0}://{1}:{2}".format(self.protocol, self.host, self.port) + '/login')

        r = requests.get("{0}://{1}:{2}".format(self.protocol, self.host, self.port) + '/login',
                         verify=False, auth=HTTPBasicAuth(self.username, self.password))

        self.logger.info('ECSAuthentication::connect()::login call returned with status code: ' + str(r.status_code))
        if r.status_code == 200:
            self.logger.info('ECSAuthentication::connect()::login call returned with a 200 status code.  '
                             'X-SDS-AUTH-TOKEN Header contains: ' + r.headers['X-SDS-AUTH-TOKEN'])
            print r.headers['X-SDS-AUTH-TOKEN']
            self.token = r.headers['X-SDS-AUTH-TOKEN']
        else:
            self.logger.info('ECSManagementAPI::connect()::login call '
                             'failed with a status code of ' + r.status_code)
            self.token = None


class ECSManagementAPI(object):
    """
    Perform ECS Management API Calls
    """

    def __init__(self, authentication, logger, response_json=None):
        self.authentication = authentication
        self.response_json = response_json
        self.logger = logger

    def localzone(self):

        # Perform ECS Dashboard Local Zone API Call
        headers = {'X-SDS-AUTH-TOKEN': "'{0}'".format(self.authentication.token), 'content-type': 'application/json'}

        r = requests.get("{0}//dashboard/zones/localzone".format(self.authentication.url),
                         headers=headers, verify=False)

        if r.status_code == 200:
            self.logger.info('ECSManagementAPI::localzone()::/dashboard/zones/localzone '
                             'call returned with a 200 status code.')
            self.response_json = r.json()
            self.logger.debug('ECSManagementAPI::localzone()::r.text() contains: \n' + r.text)

            if type(self.response_json) is list:
                self.logger.debug('ECSManagementAPI::localzone()::r.json() returned a list. ')
            elif type(self.response_json) is dict:
                self.logger.debug('ECSManagementAPI::localzone()::r.json() returned a dictionary. ')
            else:
                self.logger.debug('ECSManagementAPI::localzone()::r.json() returned unknown. ')
        else:
            self.logger.info('ECSManagementAPI::localzone()::/dashboard/zones/localzone call failed '
                             'with a status code of ' + r.status_code)
            self.response_json = None

        return self.response_json

    def capacity(self):

        # Perform ECS Capacity API Call
        headers = {'X-SDS-AUTH-TOKEN': "'{0}'".format(self.authentication.token), 'content-type': 'application/json'}

        r = requests.get("{0}//object/capacity.json".format(self.authentication.url),
                         headers=headers, verify=False)

        if r.status_code == 200:
            self.logger.info('ECSManagementAPI::capacity()::/object/capacity call returned '
                             'with a 200 status code.  Text is: ' + r.text)
            self.response_json = r.json()

            self.logger.debug('ECSManagementAPI::capacity()::r.text() contains: \n' + r.text)

            if type(self.response_json) is list:
                self.logger.debug('ECSManagementAPI::capacity()::r.json() returned a list. ')
            elif type(self.response_json) is dict:
                self.logger.debug('ECSManagementAPI::capacity()::r.json() returned a dictionary. ')
            else:
                self.logger.debug('ECSManagementAPI::capacity()::r.json() returned unknown. ')
        else:
            self.logger.info('ECSManagementAPI::capacity()::/object/capacity call failed '
                             'with a status code of ' + r.status_code)
            self.response_json = None

        return self.response_json

