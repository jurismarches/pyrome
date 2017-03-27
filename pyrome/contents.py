import copy
from . import schema as s


def _fnames(model):
    return list(model._meta.fields.keys())


class AttrDict(dict):

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError as e:
            return AttributeError(*e.args)


def simple_to_dict(obj):
    return dict(obj._data)


def _relation_to_dict_factory(model, rel_model, rel_name):
    fields = list(model._meta.fields)
    rel_fields = list(rel_model._meta.fields)

    def relation_to_dict(obj):
        related = getattr(obj, rel_name)
        data = dict((k, v) for k, v in obj._data.items() if k in fields)
        data.update((k, v) for k, v in related._data.items() if k in rel_fields)
        return data

    return relation_to_dict


rome_appellation_to_dict = _relation_to_dict_factory(
    s.RomeAppellation, s.Appellation, "appellation")
rome_activite_to_dict = _relation_to_dict_factory(
    s.RomeActivite, s.Activite, "activite")
rome_competence_to_dict = _relation_to_dict_factory(
    s.RomeCompetence, s.Competence, "competence")
rome_env_travail_to_dict = _relation_to_dict_factory(
    s.RomeEnvTravail, s.EnvTravail, "env_travail")


_rome_fields = _fnames(s.Rome)
_fiche_fields = _fnames(s.Fiche)


def rome_to_dict(obj):
    data = dict((k, v) for k, v in obj._data.items() if k in _rome_fields)
    fiche = list(obj.fiche)
    if fiche:
        data.update((k, v) for k, v in fiche[0]._data.items() if k in _fiche_fields)
    data["appellation"] = [rome_appellation_to_dict(r) for r in obj.rome_appellation]
    data["activite"] = [rome_activite_to_dict(r) for r in obj.rome_activite]
    data["competence"] = [rome_competence_to_dict(r) for r in obj.rome_competence]
    data["env_travail"] = [rome_env_travail_to_dict(r) for r in obj.rome_env_travail]
    return data


def prefetched_rome_to_dict(obj):
    data = dict((k, v) for k, v in obj._data.items() if k in _rome_fields)
    fiche = list(obj.fiche)
    if fiche:
        data.update((k, v) for k, v in obj.fiche[0]._data.items() if k in _fiche_fields)
    data["appellation"] = [rome_appellation_to_dict(r) for r in obj.rome_appellation_prefetch]
    data["activite"] = [rome_activite_to_dict(r) for r in obj.rome_activite_prefetch]
    data["competence"] = [rome_competence_to_dict(r) for r in obj.rome_competence_prefetch]
    data["env_travail"] = [rome_env_travail_to_dict(r) for r in obj.rome_env_travail_prefetch]
    return data


_arborescence_fields = ["ogr_id", "code_noeud", "libelle"]
_ogr_type_to_relation = dict(s.Ogr.TYPE)


def arborescence_to_dict(obj):
    data = {k: v for k, v in obj._data.items() if k in _arborescence_fields}
    if obj.item_ogr_id:
        item_ogr = obj.item_ogr
        rel_name = _ogr_type_to_relation[item_ogr.type]
        item_obj = getattr(item_ogr, rel_name)
        data[rel_name] = to_dict(item_obj)
    return data


MODEL_TO_DICT = {
    s.Activite: simple_to_dict,
    s.Appellation: simple_to_dict,
    s.Competence: simple_to_dict,
    s.EnvTravail: simple_to_dict,
    s.RomeActivite: rome_activite_to_dict,
    s.RomeAppellation: rome_appellation_to_dict,
    s.RomeCompetence: rome_competence_to_dict,
    s.RomeEnvTravail: rome_env_travail_to_dict,
    s.Fiche: simple_to_dict,
    s.Rome: rome_to_dict,
    s.Arborescence: arborescence_to_dict}


PREFETCH_MODEL_TO_DICT = copy.copy(MODEL_TO_DICT)
PREFETCH_MODEL_TO_DICT.update({
    s.Rome: prefetched_rome_to_dict})


def to_dict(obj):
    return MODEL_TO_DICT[obj.__class__](obj)


def prefetched_to_dict(obj):
    return PREFETCH_MODEL_TO_DICT[obj.__class__](obj)
