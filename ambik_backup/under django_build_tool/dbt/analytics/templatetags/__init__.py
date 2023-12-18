# from django.template.defaulttags import URLNode

# old_render = URLNode.render
# def new_render(cls, context):
#   """ Override existing url method to use pluses instead of spaces
#   """
#   return old_render(cls, context).replace("%20", "+")
# URLNode.render = new_render