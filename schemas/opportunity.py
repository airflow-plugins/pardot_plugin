"""
Opportunity

http://developer.pardot.com/kb/api-version-4/opportunities/
http://developer.pardot.com/kb/object-field-references/#opportunity
"""

opportunity = [{'name': 'id',
                'type': 'integer'},
               {'name': 'campaign',
                'type': 'varchar(512)'},
               {'name': 'prospects',
                'type': 'varchar(max)'},
               {'name': 'name',
                'type': 'varchar(512)'},
               {'name': 'value',
                'type': 'real'},
               {'name': 'probability',
                'type': 'integer'},
               {'name': 'type',
                'type': 'varchar(512)'},
               {'name': 'stage',
                'type': 'varchar(512)'},
               {'name': 'status',
                'type': 'varchar(512)'},
               {'name': 'closed_at',
                'type': 'timestamp'},
               {'name': 'created_at',
                'type': 'timestamp'},
               {'name': 'updated_at',
                'type': 'timestamp'}]
