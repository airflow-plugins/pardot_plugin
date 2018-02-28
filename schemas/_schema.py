from PardotPlugin.schemas.list import list
from PardotPlugin.schemas.listmembership import listmembership
from PardotPlugin.schemas.opportunity import opportunity
from PardotPlugin.schemas.prospect import prospect
from PardotPlugin.schemas.tagobject import tagobject
from PardotPlugin.schemas.tag import tag
from PardotPlugin.schemas.visit import visit
from PardotPlugin.schemas.visitor import visitor
from PardotPlugin.schemas.visitoractivity import visitoractivity


schema = {'list': list,
          'listmembership': listmembership,
          'opportunity': opportunity,
          'prospect': prospect,
          'tagobject': tagobject,
          'tag': tag,
          'visitor': visitor,
          'visitoractivity': visitoractivity,
          'visit': visit}
