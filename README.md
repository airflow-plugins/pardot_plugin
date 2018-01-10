# Plugin - Pardot to S3

This plugin moves data from the [Pardot](http://developer.pardot.com/) API to S3 based on the specified object

## Hooks
### PardotHook
This hook handles the authentication and request to Pardot. Based on [pypardot4](https://github.com/mneedham91/PyPardot4) module.

### S3Hook
[Core Airflow S3Hook](https://pythonhosted.org/airflow/_modules/S3_hook.html) with the standard boto dependency.

## Operators
### PardotToS3Operator
This operator composes the logic for this plugin. It fetches the pardot specified object and saves the result in a S3 Bucket, under a specified key, in
njson format. The parameters it can accept include the following.

- `pardot_conn_id`: The pardot connection id from Airflow
- `pardot_obj`: Pardot object to query
- `results_field`: name of the results array from response, acording to pypardot4 documentation
- `pardot_args`: *optional* dictionary with any extra arguments accepted by pypardot4, 
- `s3_conn_id`: S3 connection id from Airflow.  
- `s3_bucket`: The output s3 bucket.  
- `s3_key`: The input s3 key.  
- `fields`: *optional* list of fields that you want to get from the object. If *None*, then this will get all fields for the object
- `replication_key_value`: *(optional)*  value of the replication key, if needed. The operator will import only results with the id grater than the value of this param.