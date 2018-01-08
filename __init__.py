from airflow.plugins_manager import AirflowPlugin
from pardot_plugin.operators.pardot_to_s3_operator import PardotToS3Operator
from pardot_plugin.hooks.pardot_hook import PardotHook


class pardot_plugin(AirflowPlugin):
    name = "pardot_plugin"
    operators = [PardotToS3Operator]
    hooks = [PardotToS3Operator]
    # Leave in for explicitness
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
