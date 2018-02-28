"""
List Membership

http://developer.pardot.com/kb/api-version-4/list-memberships/
http://developer.pardot.com/kb/object-field-references/#list-membership
"""


listmembership = [{'name': 'id',
                   'type': 'integer'},
                  {'name': 'list_id',
                   'type': 'integer'},
                  {'name': 'prospect_id',
                   'type': 'integer'},
                  {'name': 'opted_out',
                   'type': 'boolean'},
                  {'name': 'created_at',
                   'type': 'timestamp'},
                  {'name': 'updated_at',
                   'type': 'timestamp'}]
