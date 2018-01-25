"""
Pardot Hook

NOTE: This hook requires a modified version of the PyPardot4 package that
allows the version to be manually set to v3 or v4. This modified version
can be found here: https://github.com/astronomerio/PyPardot4 and can be added
to your environment by including the following in your requirements.txt:
`-e git+https://github.com/astronomerio/PyPardot4.git#egg=pypardot`

There is an open PR in PyPardot4 to make this change permanent:
https://github.com/mneedham91/PyPardot4/pull/13
"""

from airflow.hooks.base_hook import BaseHook
from pypardot.client import PardotAPI
import logging


class PardotHook(BaseHook):
    def __init__(
            self,
            conn_id,
            *args,
            **kwargs):
        self.conn_id = conn_id
        self._args = args
        self._kwargs = kwargs

        self.connection = None
        self.extras = None
        self.pardot = None

    def get_conn(self):
        """
        Initialize a pardot instance.
        :param email:       The email address associated with the account.
        :type string:       string
        :param password:    The password associated with the account.
        :type string:       string
        :param user_key:    The issued user_key associated with the account.
        :type user_key:     string
        :param api_version: The version of the Pardot API that the account uses.
                            This is set on the account level within Pardot.
                            Possible values include:
                                - 3
                                - 4
        :type api_version:  integer
        """
        if self.pardot:
            return self.pardot

        self.connection = self.get_connection(self.conn_id)
        self.extras = self.connection.extra_dejson
        pardot = PardotAPI(email=self.extras['email'],
                           password=self.extras['password'],
                           user_key=self.extras['user_key'],
                           version=self.extras['api_version'])

        pardot.authenticate()
        self.pardot = pardot

        return pardot

    def run_query(self,
                  pardot_obj,
                  results_field,
                  method_to_call='query',
                  **kwargs):
        """
        Run a query against pardot
        :param object:                   name of the Pardot object
        :param results_field:           name of the results array from
                                        response
        """
        pardot = self.get_conn()
        pardot_object = getattr(pardot, pardot_obj)

        result = getattr(pardot_object, method_to_call)(**kwargs)

        offset = 0
        total_results = result[results_field]

        while len(total_results) < result['total_results']:
            offset += len(result[results_field])
            result = getattr(pardot_object, method_to_call)(offset=offset,
                                                            **kwargs)
            total_results += result[results_field]
            logging.info('Retrieved: ' + str(offset) + ' records.')

        if total_results is not None:
            logging.info('Retrieved: ' +
                         str(len(total_results)) +
                         ' total records.')

        return total_results

    def get_records(self, sql):
        pass

    def get_pandas_df(self, sql):
        pass

    def run(self, sql):
        pass
