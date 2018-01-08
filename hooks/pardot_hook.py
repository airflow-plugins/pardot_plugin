from airflow.hooks.base_hook import BaseHook
from pypardot.client import PardotAPI
import json
# TODO: inherit from base hook


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
        """
        if self.pardot:
            return self.pardot

        self.connection = self.get_connection(self.conn_id)
        self.extras = self.connection.extra_dejson
        pardot = PardotAPI(email=self.extras['username'],
                           password=self.extras['password'],
                           user_key=self.extras['user_key'])

        pardot.authenticate()
        self.pardot = pardot

        return pardot

    def run_query(self, model, results_field, replication_key_value, method_to_call='query', **kwargs):
        """
        Run a query against pardot
        :param model:                   name of the Pardot model
        :param results_field:           name of the results array from 
                                        response
        :param replication_key_value:   pardot replicaton key
        """
        pardot = self.get_conn()
        pardot_model = getattr(pardot, model)
        print ('Initial result: ')

        result = getattr(pardot_model, method_to_call)(
            id_greater_than=replication_key_value, **kwargs)

        offset = 0
        total_results = result[results_field]
        print(result)
        while len(total_results) < result['total_results'] and len(total_results) < 200:
            offset += len(result[results_field])
            result = getattr(pardot_model, method_to_call)(
                id_greater_than=replication_key_value, offset=offset, **kwargs)
            total_results += result[results_field]

        return total_results

    def get_records(self, sql):
        pass

    def get_pandas_df(self, sql):
        pass

    def run(self, sql):
        pass
