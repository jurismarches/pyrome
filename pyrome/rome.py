import collections

import peewee as pw

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


def _tree(node, by_pere):
    data = contents.to_dict(node)
    data["children"] = [_tree(n, by_pere) for n in by_pere.get(node.ogr_id, [])]
    return data


def referentiel(ogr_id):
    """Return a complete referentiel, that is a tree of rome
    """
    # get types of nodes
    types = list(
        s.Arborescence
        .select(pw.fn.distinct(s.Ogr.type))
        .join(s.Ogr, on=s.Arborescence.item_ogr_id)
        .where(s.Arborescence.referentiel_id == ogr_id).tuples())
    assert len(types) > 0, "can't retrieve referentiel without object types"
    assert len(types) == 1, "can't retrieve referentiel mixing object types"
    # FIXME can we add prefetch ?
    leaf_model = s.ogr_type_model[types[0][0]]
    # retrieve all nodes
    item_nodes = (
        s.Arborescence
        .select(s.Arborescence, s.Ogr, leaf_model)
        .join(s.Ogr, on=s.Arborescence.item_ogr)
        .join(leaf_model, on=leaf_model.ogr)
        .where(s.Arborescence.referentiel_id == ogr_id))
    non_item_nodes = (
        s.Arborescence
        .select(s.Arborescence)
        .where((s.Arborescence.referentiel_id == ogr_id) &
               ((s.Arborescence.item_ogr.is_null(True)) |
                (s.Arborescence.item_ogr == 0))))
    # nodes indexed by pere id
    by_pere = collections.defaultdict(list)
    for n in item_nodes:
        by_pere[n.pere_id].append(n)
    for n in non_item_nodes:
        by_pere[n.pere_id].append(n)
    # build tree from root
    tree = _tree(by_pere[None][0], by_pere)
    return tree


#~ def check_db():
    #~ # all arborescence of type feuille have an ogr associated
    #~ s.Arborescence.select().where((s.Arborescence
