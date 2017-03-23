from . import schema as s


def get_ogr(code):
    """Generic method to get the ogr from its code, managing retrieval from right table
    """
    ogr = s.Ogr.get(s.Ogr.code == code)
    model = s.ogr_type_model[ogr.type]
    return model.get(model.ogr == ogr)
