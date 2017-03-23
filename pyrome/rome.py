from . import schema as s
from . import contents


def get_ogr(code):
    """Generic method to get the ogr from its code, managing retrieval from right table
    """
    ogr = s.Ogr.get(s.Ogr.code == code)
    model = s.ogr_type_model[ogr.type]
    obj = model.get(model.ogr == ogr)
    obj = contents.to_dict(obj)
    return obj


def get_rome(ogr_id):
    """Get a rome as a hierarchie of tuple
    """
    obj = s.Rome.get(ogr_id=ogr_id)
    return contents.rome_to_dict(obj)
