from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator, Variable, SkipMixin

from airflow.hooks.S3_hook import S3Hook
from PardotPlugin.hooks.pardot_hook import PardotHook

from datetime import timedelta
from tempfile import NamedTemporaryFile
import logging
import json


class PardotToS3Operator(BaseOperator, SkipMixin):
    """
    Make a query against Pardot and write the resulting data to s3
    """
    template_fields = ('s3_key',
                       'pardot_args')

    @apply_defaults
    def __init__(
        self,
        pardot_conn_id,
        pardot_obj,
        pardot_time_offset=-5,
        pardot_args={},
        s3_conn_id=None,
        s3_key=None,
        s3_bucket=None,
        fields=None,
        replication_field='updated_at',
        replication_type=None,
        *args,
        **kwargs
    ):
        """
        Initialize the operator
        :param pardot_conn_id:          The name of the Airflow connection
                                        that has the associated Pardot
                                        username, password and user_key
        :type pardot_conn_id:           string
        :param pardot_obj:              The name of the Pardot object
        :type pardot_obj:               string
        :param pardot_args              *(optional)* Extra Pardot arguments.
                                        dditionally, the parameter 'visit_type'
                                        should be added to this dictionary when
                                        querying visits. Possible values
                                        for visit_type include:
                                            - visits
                                            - prospects
        :type pardot_args:              dictionary
        :param pardot_time_offset:      The timezone offset from UTC that the
                                        Pardot account is associated with. For
                                        example, if the Pardot account has
                                        timestamps in the Eastern US timezone,
                                        this value would be "-5" (i.e. 5 hours
                                        before UTC). By default, this value
                                        is set to Eastern US time.
        :type pardot_time_offset:       integer
        :param replication_type:        *(optional)*  Theh type of incremental
                                        queries being issued to Pardot API.
                                        Possible values for this parameter
                                        include:
                                            - time
                                            - key
        :type replication_type:
        :param replication_field:       *(optional)*  The field for the
                                        replication key, only required if
                                        replication_type is set to "time".
                                        This parameter can be one of the
                                        following values:
                                            - updated_at
                                            - created_at
        :type replication_field:
        :param s3_conn_id:              name of the Airflow connection that has
                                        your Amazon S3 conection params
        :type s3_conn_id:               string
        :param s3_bucket:               Name of the destination S3 bucket
        :type s3_bucket:                string
        :param s3_key:                  Name of the destination file.
        :type s3_key:                   string
        :param fields:                  *(optional)* The list of fields to be
                                        returned from the object.
                                        If *None*, then this will returned
                                        all fields for the object.
        :param **kwargs:                Extra params for the pardot query,
                                        based on python pardot module.
        """

        super().__init__(*args, **kwargs)

        self.pardot_conn_id = pardot_conn_id
        self.pardot_obj = pardot_obj
        self.pardot_args = pardot_args
        self.pardot_time_offset = pardot_time_offset
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

        self.fields = fields
        self.replication_field = replication_field
        self.replication_type = replication_type
        self._kwargs = kwargs

        if self.replication_field not in ('updated_at', 'created_at'):
            raise Exception('Specified replication field not available.')

        if self.replication_type is not None and\
           self.replication_type not in ('time', 'key'):
            raise Exception('Specified replication type not available.')

    def execute(self, context):
        """
        Execute the operator.
        This will get all the data for a particular
        Pardot model and write it to a file.
        """
        logging.info("Prepping to gather data from Pardot")

        hook = PardotHook(
            conn__kid=self.pardot_conn_id
        )

        # attempt to login to Pardot
        # if this process fails, it will raise an error and die right here
        # we could wrap it

        hook.get_conn()

        self.set_replication_values(context)

        logging.info(
            "Making request for"
            " {0} object".format(self.pardot_obj)
        )

        # fetch the results from pardot and filter them
        if self.pardot_obj is 'visits':
            # Visits can be queried only by prospect or visit id
            # so we first query all prospects/visits and pass the ids to
            # the hook.

            results = self.get_visits(hook, context)
        else:
            if self.replication_type == 'key':
                results = hook.run_query(self.pardot_obj,
                                         self.results_mapper(self.pardot_obj),
                                         **self.pardot_args)
                if len(results):
                    self.set_max(results, context)
            else:
                results = hook.run_query(self.pardot_obj,
                                         self.results_mapper(self.pardot_obj),
                                         **self.pardot_args)
        if len(results) == 0 or results is None:
            logging.info("No records pulled from Pardot.")
            downstream_tasks = context['task'].get_flat_relatives(upstream=False)
            logging.info('Skipping downstream tasks...')
            logging.debug("Downstream task_ids %s", downstream_tasks)

            if downstream_tasks:
                self.skip(context['dag_run'],
                          context['ti'].execution_date,
                          downstream_tasks)
            return True
        else:
            # Write the results to a temporary file and save that file to s3.
            with NamedTemporaryFile("w") as tmp:
                for result in results:
                    filtered_result = self.filter_fields(result)
                    tmp.write(json.dumps(filtered_result) + '\n')

                tmp.flush()

                dest_s3 = S3Hook(s3_conn_id=self.s3_conn_id)
                dest_s3.load_file(
                    filename=tmp.name,
                    key=self.s3_key,
                    bucket_name=self.s3_bucket,
                    replace=True

                )
                dest_s3.connection.close()
                tmp.close()

    logging.info("Query finished!")

    def get_visits(self, hook, context):
        """
        Get all visits.
        """

        visit_type = 'prospects'
        method_call = 'query_by_prospect_ids'

        if 'visit_type' in self.pardot_args.keys():
            popped_type = self.pardot_args.pop('visit_type').lower()
            if popped_type == 'visitors':
                visit_type = popped_type
                method_call = 'query_by_visitor_ids'
            elif popped_type == 'prospects':
                pass
            else:
                raise Exception('Specified visit type not supported.')

        records = hook.run_query(visit_type,
                                 self.results_mapper(visit_type),
                                 **self.pardot_args)

        if records is not None:
            if self.replication_type == 'key':
                self.set_max(records, context)
            elif self.replication_field == 'updated_at':
                self.pardot_args.pop('updated_before')
                self.pardot_args.pop('updated_after')
            elif self.replication_field == 'created_at':
                self.pardot_args.pop('created_before')
                self.pardot_args.pop('created_after')

            id_list = []
            ids = ''

            logging.info('Received all {0}: '.format(visit_type) +
                         str(len(records)))

            # Iterate through all records visitor/prospect records,
            # pulling out  the 'id' value and adding to a string
            # that will be passed into the  visits request.
            for i, record in enumerate(records, 1):
                if ids is not '':
                    ids += ','

                ids += str(record.get('id'))

                # At the last record, append the string
                # to the id list and exit.
                if i == len(records):
                    id_list.append(ids)

                # Chunk the id request strings into groups of 500
                # ids to stay under  GET request  length limits.
                # Every 500 ids, flush the id string to id_list
                # and start a new id string.
                elif i % 500 == 0:
                    id_list.append(ids)
                    ids = ''

            logging.info('Constructed list of ids to make request.')
            logging.info('Making ' +
                         str(len(id_list)) +
                         ' requests.')

            final_results = []

            # Iterate through each id request string in the
            # id_list and add them to a final output.
            for id_set in id_list:
                results = hook.run_query(self.pardot_obj,
                                         self.results_mapper(self.pardot_obj),
                                         method_to_call=method_call,
                                         prospect_ids=id_set)
                if results is not None:
                    final_results.extend(results)

            return final_results
        else:
            return records

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

    def results_mapper(self, pardot_obj):
        """
        Map the specified pardot object to the
        appropriate array name in the response.
        The naming convention for these arrays
        varies by object.
        """
        mapping = [{'pardot_name': 'listmemberships',
                    'results_field': 'list_membership'},
                   {'pardot_name': 'lists',
                    'results_field': 'list'},
                   {'pardot_name': 'opportunities',
                    'results_field': 'opportunity'},
                   {'pardot_name': 'prospects',
                    'results_field': 'prospect'},
                   {'pardot_name': 'tagobjects',
                    'results_field': 'tagObject'},
                   {'pardot_name': 'tags',
                    'results_field': 'tag'},
                   {'pardot_name': 'visitors',
                    'results_field': 'visitor'},
                   {'pardot_name': 'visits',
                    'results_field': 'visit'},
                   {'pardot_name': 'visitoractivities',
                    'results_field': 'visitor_activity'}]

        for endpoint in mapping:
            if pardot_obj == endpoint['pardot_name']:
                return endpoint['results_field']

    def set_replication_values(self, context):
        if self.replication_type == 'time':
            time_offset = timedelta(hours=self.pardot_time_offset)
            execution_date = context['ti'].get_template_context()['execution_date'] + time_offset
            next_execution_date = context['ti'].get_template_context()['next_execution_date'] + time_offset
            print('Execution date: ' + str(execution_date))
            print('Next execution date: ' + str(next_execution_date))
            if self.replication_field == 'updated_at':
                self.pardot_args['updated_after'] = execution_date
                self.pardot_args['updated_before'] = next_execution_date
            else:
                self.pardot_args['created_after'] = execution_date
                self.pardot_args['created_before'] = next_execution_date
        else:
            try:
                # Look for the stored max id associated with the previous
                # task instance. If this does not exist, set 'id_greater_than'
                # to 0.
                variable_name = '__'.join(list(str(_) for _ in context['ti'].previous_ti.key))
                self.pardot_args['id_greater_than'] = Variable.get('INCREMENTAL_KEY__' +
                                                                   variable_name)
            except:
                self.pardot_args['id_greater_than'] = 0

    def set_max(self, results, context):
        id_set = []
        for item in results:
            id_set.extend([v for k, v in item.items() if k == 'id'])

        Variable.set('INCREMENTAL_KEY__' +
                     '__'.join(list(str(_) for _ in context['ti'].key)),
                     max(id_set))
