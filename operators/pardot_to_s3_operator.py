import logging
import json
import collections
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from pardot_plugin.hooks.pardot_hook import PardotHook
from tempfile import NamedTemporaryFile


class PardotToS3Operator(BaseOperator):
    """
    Make a query against Pardot and write the resulting data to s3
    """
    template_field = ('s3_key', )

    @apply_defaults
    def __init__(
        self,
        pardot_conn_id,
        pardot_obj,
        results_field,
        pardot_args={},
        s3_conn_id=None,
        s3_key=None,
        s3_bucket=None,
        fields=None,
        replication_key_name=None,
        replication_key_value=0,
        *args,
        **kwargs
    ):
        """ 
        Initialize the operator
        :param pardot_conn_id:          name of the Airflow connection that has
                                        your Pardot username, password and user_key
        :param pardot_obj:              name of the Pardot object we are
                                        fetching data from
        :param results_field            name of the results array from 
                                        response, acording to pypardot4 documentation
        :param pardot_args              *(optional)* dictionary with extra pardot
                                        arguments
        :param s3_conn_id:              name of the Airflow connection that has
                                        your Amazon S3 conection params
        :param s3_bucket:               name of the destination S3 bucket
        :param s3_key:                  name of the destination file from bucket
        :param fields:                  *(optional)* list of fields that you want
                                        to get from the object.
                                        If *None*, then this will get all fields
                                        for the object
        :param replication_key_value:   *(optional)* value of the replication key,
                                        if needed. The operator will import only 
                                        results with the id grater than the value of
                                        this param.

        :param \**kwargs:               Extra params for the pardot query, based on python
                                        pardot module
        """

        super().__init__(*args, **kwargs)

        self.pardot_conn_id = pardot_conn_id
        self.pardot_obj = pardot_obj
        self.pardot_args = pardot_args
        self.results_field = results_field

        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

        self.fields = fields
        self.replication_key_value = replication_key_value
        self._kwargs = kwargs

    def filter_fields(self, result):
        """
        Filter the fields from an resulting object.

        This will return a object only with fields given
        as parameter in the constructor.

        All fields are returned when "fields" param is None.
        """
        if not self.fields:
            return result
        obj = {}
        for field in self.fields:
            obj[field] = result[field]
        return obj

    def get_visits(self, hook, replication_key_value):
        """
        Get all visits.
        """
        prospects = hook.run_query('prospects', 'prospect', 0)
        ids = ''

        for prospect in prospects:
            if ids is not '': 
                ids += ','
            ids += str(prospect.get('id'))    
        results = hook.run_query(self.pardot_obj, self.results_field,
                                 self.replication_key_value, method_to_call='query_by_prospect_ids', prospect_ids=ids)
        return results

    def execute(self, context):
        """
        Execute the operator.
        This will get all the data for a particular Pardot model
        and write it to a file.
        """
        logging.info("Prepping to gather data from Pardot")
        hook = PardotHook(
            conn_id=self.pardot_conn_id
        )

        # attempt to login to Pardot
        # if this process fails, it will raise an error and die right here
        # we could wrap it
        hook.get_conn()

        logging.info(
            "Making request for"
            " {0} object".format(self.pardot_obj)
        )
    
        # fetch the results from pardot and filter them
        if self.pardot_obj is 'visits':
            # visits can be queried only by prospect or visit id 
            # so we first query all prospects and pass the ids to
            # the hook
            results = self.get_visits(hook, self.replication_key_value)
        else: 
            results = hook.run_query(
                    self.pardot_obj, 
                    self.results_field,
                    self.replication_key_value, 
                    **self.pardot_args)
        filterd_results = self.filter_fields(results)

        # write the results to a temporary file and save that file to s3
        with NamedTemporaryFile("w") as tmp:
            for result in filterd_results:
                tmp.write(json.dumps(result) + '\n')

            tmp.flush()

            # dest_s3 = S3Hook(s3_conn_id=self.s3_conn_id)
            # dest_s3.load_file(
            #     filename=tmp.name,
            #     key=self.s3_key,
            #     bucket_name=self.s3_bucket,
            #     replace=True

            # )
            # dest_s3.connection.close()
            # tmp.close()

    logging.info("Query finished!")
