"""
Visit

http://developer.pardot.com/kb/api-version-4/visits/
http://developer.pardot.com/kb/object-field-references/#visit
"""

visit = [{'name': 'id',
          'type': 'integer'},
         {'name': 'visitor_id',
          'type': 'integer'},
         {'name': 'prospect_id',
          'type': 'integer'},
         {'name': 'visitor_page_view_count',
          'type': 'integer'},
         {'name': 'first_visitor_page_view_at',
          'type': 'timestamp'},
         {'name': 'last_visitor_page_view_at',
          'type': 'timestamp'},
         {'name': 'duration_in_seconds',
          'type': 'integer'},
         {'name': 'campaign_parameter',
          'type': 'varchar(512)'},
         {'name': 'medium_parameter',
          'type': 'varchar(512)'},
         {'name': 'source_parameter',
          'type': 'varchar(512)'},
         {'name': 'content_parameter',
          'type': 'varchar(512)'},
         {'name': 'term_parameter',
          'type': 'varchar(512)'},
         {'name': 'created_at',
          'type': 'timestamp'},
         {'name': 'updated_at',
          'type': 'timestamp'}]
