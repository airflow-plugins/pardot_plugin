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
- `pardot_args`: *optional* dictionary with any extra arguments accepted by pypardot4,
- `pardot_time_offset`: *optional* The timezone offset from UTC that the Pardot account is associated with. For example, if the Pardot account has timestamps in the Eastern US timezone, this value would be "-5" (i.e. 5 hours before UTC). By default, this value is set to Eastern US time.
- `s3_conn_id`: S3 connection id from Airflow.  
- `s3_bucket`: The output s3 bucket.  
- `s3_key`: The input s3 key.  
- `fields`: *optional* list of fields that you want to get from the object. If *None*, then this will get all fields for the object
- `replication_type`:  *(optional)*  Theh type of incremental queries being issued to Pardot API. Possible values for this parameter include `time` and `key`.
- `replication_field`: *(optional)*  The field for the replication key, only required if `replication_type` is set to `time`. This parameter can be one of the following values:
  - `updated_at`
  - `created_at`


- `**kwargs`:  replication key and value, if replication_key parameter is given and extra params for pardot method if needed.
