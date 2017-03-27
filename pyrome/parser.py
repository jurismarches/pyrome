import collections
import xml.etree.ElementTree as ET
import zipfile
from itertools import islice


from . import schema as s


class Loader:

    BATCH_SIZE = 1000

    ogr_fname = collections.OrderedDict([
        (s.Activite, "unix_referentiel_activite_v330_iso8859-15.xml"),
        (s.Appellation, "unix_referentiel_appellation_v330_iso8859-15.xml"),
        (s.EnvTravail, "unix_referentiel_env_travail_v330_iso8859-15.xml"),
        (s.Competence, "unix_referentiel_competence_v330_iso8859-15.xml"),
        (s.Rome, "unix_referentiel_code_rome_v330_iso8859-15.xml")])

    ogr_types = {v: k for k, v in s.Ogr.TYPE}

    rome_relations = {
        s.RomeAppellation, s.RomeActivite, s.RomeCompetence, s.RomeEnvTravail, s.Mobilite}

    fiche_fname = "unix_fiche_emploi_metier_v330_iso8859-15.xml"

    arborescence_fname = "unix_arborescence_v330_iso8859-15.xml"

    keymap = {
        s.Activite: {"ogr": "code_ogr"},
        s.Appellation: {"ogr": "code_ogr"},
        s.EnvTravail: {"ogr": "code_ogr"},
        s.Competence: {"ogr": "code_ogr"},
        s.Rome: {"ogr": "code_ogr"},
        s.RomeAppellation: {"appellation": "code_ogr"},
        s.RomeEnvTravail: {"env_travail": "code_ogr"},
        s.RomeActivite: {"activite": "code_ogr"},
        s.RomeCompetence: {"competence": "code_ogr"},
        s.Arborescence: {
            "ogr": "code_ogr",
            "pere": "code_ogr_pere",
            "item_ogr": "code_item_arbor_associe"}}

    def _batched(self, iterator):
        while True:
            batch = list(islice(iterator, None, self.BATCH_SIZE))
            if batch:
                yield batch
            else:
                raise StopIteration()

    def iter_data(self, content):
        if not isinstance(content, (ET.Element, ET.ElementTree)):
            tree = ET.fromstring(content)
        else:
            tree = content
        for item in tree:
            yield {child.tag: child.text for child in item}

    def iter_transform(self, iterator, model, defaults={}):
        keymap = self.keymap.get(model, {})
        cols = model._meta.fields
        for data in iterator:
            # transform
            for cname, ctype in cols.items():
                # map
                key = keymap.get(cname, cname)
                # lookup default
                data[cname] = data.get(key, defaults.get(key))
            yield data

    def insert_data(self, model, data):
        fields = [k for k in model._meta.fields.keys() if k != "id"]
        if model._meta.name in self.ogr_types.keys():
            # create ogr entries first
            t = self.ogr_types[model._meta.name]
            s.Ogr.insert_many([{"code": d["ogr"], "type":t} for d in data]).execute()
        # keep columns only
        data = [{k: d[k] for k in fields} for d in data]
        model.insert_many(data).execute()

    def load_data(self, model, content, defaults={}):
        iterator = self.iter_transform(self.iter_data(content), model, defaults)
        for data in self._batched(iterator):
            self.insert_data(model, data)

    def load_card_bloc(self, bloc, defaults):
        self.load_data(
            s.RomeActivite,
            bloc.find("activite_de_base") or bloc.find("activite_specifique"),
            defaults)
        self.load_data(
            s.RomeCompetence,
            bloc.find("savoir_theorique_et_proceduraux"),
            defaults)
        self.load_data(
            s.RomeCompetence,
            bloc.find("savoir_action"),
            defaults)

    def collect_rome_code(self):
        results = s.Rome.select(s.Rome.code_rome, s.Rome.ogr).execute()
        self.rome_code_ogr = {r.code_rome: r.ogr for r in results}

    def load_mobilite(self, content, defaults):
        iterator = self.iter_transform(self.iter_data(content), s.Mobilite, defaults)
        for data in self._batched(iterator):
            # adjust
            for d in data:
                code = d["code_rome_cible"].split(maxsplit=1)[0]
                d["cible_rome"] = self.rome_code_ogr[code]
            # create
            self.insert_data(s.Mobilite, data)

    def load_fiche(self, content):
        """load fiche
        """
        tree = ET.fromstring(content)
        self.collect_rome_code()
        cards = []
        simple_attrs = [
            "numero", "definition", "formations_associees",
            "condition_exercice_activite", "classement_emploi_metier"]
        for card in tree:
            data = {}
            for attrname in simple_attrs:
                data[attrname] = card.find("numero").text
            data["rome"] = card.find("bloc_code_rome").find("code_ogr").text

            defaults = {"rome": data["rome"], "bloc": None}
            self.load_data(
                s.RomeAppellation,
                card.find("appellation"),
                defaults)
            self.load_data(
                s.RomeEnvTravail,
                card.find("environnement_de_travail"),
                defaults)
            self.load_card_bloc(card.find("les_activites_de_base"), defaults)

            for bloc in card.find("les_activites_specifique"):
                defaults = {
                    "rome": data["rome"],
                    "bloc": bloc.find("position_bloc").text}
                self.load_card_bloc(bloc, defaults)

            self.load_mobilite(
                card.find("les_mobilites").find("proche"),
                {"origine_rome": data["rome"], "type": 0})
            self.load_mobilite(
                card.find("les_mobilites").find("si_evolution"),
                {"origine_rome": data["rome"], "type": 1})

            cards.append(data)

        for data in self._batched(iter(cards)):
            self.insert_data(s.Fiche, data)

    def load_arborescence(self, content):
        """Load arborescence table
        """
        noeud_to_type = {v: k for k, v in s.Arborescence.TYPE_NOEUD}

        # we need a first pass to map code to id, to have pere as a key
        code_to_ogr_id = {}
        referentiel_label_to_id = {}
        for d in self.iter_data(content):
            code_to_ogr_id[d["code_noeud"]] = d["code_ogr"]
            if not d["code_pere"] or not d["code_pere"].strip():
                referentiel_label_to_id[d["libelle_referentiel"]] = d["code_ogr"]

        iterator = self.iter_transform(self.iter_data(content), s.Arborescence)
        for data in self._batched(iterator):
            referentiel_data = []
            # transform
            for d in data:
                if not d["item_ogr"] or d["item_ogr"] == "0":
                    d["item_ogr"] = None
                if not d["code_pere"] or not d["code_pere"].strip():
                    # root
                    d["pere"] = None
                    referentiel_data.append(
                        {"ogr": d["ogr"], "libelle": d["libelle_referentiel"]})
                else:
                    # get pere
                    d["pere"] = code_to_ogr_id[d["code_pere"]]
                d["referentiel"] = referentiel_label_to_id[d["libelle_referentiel"]]
                d["type_noeud"] = noeud_to_type[d["libelle_noeud"]]
            if referentiel_data:
                self.insert_data(s.Referentiel, referentiel_data)
            self.insert_data(s.Arborescence, data)

    def __call__(self, zip_path, db_path):
        zip_file = zipfile.ZipFile(zip_path, 'r')
        with s.RomeDB(db_path) as db, zip_file as z:
            s.Ogr.create_table()
            # create tables
            for model, path in self.ogr_fname.items():
                with db.atomic():
                    model.create_table()
                    self.load_data(model, z.read(path))
            with db.atomic():
                for model in self.rome_relations:
                    model.create_table()
                s.Fiche.create_table()
                self.load_fiche(z.read(self.fiche_fname))
            with db.atomic():
                s.Arborescence.create_table()
                s.Referentiel.create_table()
                self.load_arborescence(z.read(self.arborescence_fname))


if __name__ == "__main__":
    import argparse
    import os
    import sys

    parser = argparse.ArgumentParser(description='Load zip of ROME')
    parser.add_argument("zip_path", help='Path to zip file containing ROME data')
    parser.add_argument("db_path", help='Path where to store data')
    parser.add_argument("-x", "--overwrite", action='store_true',
                        help='Delete db if it already exists')

    args = parser.parse_args()
    # verify overwrite of db
    if os.path.exists(args.db_path):
        if not args.overwrite:
            print("database exists", file=sys.stderr)
            exit(1)
        else:
            os.remove(args.db_path)

    # Go
    Loader()(args.zip_path, args.db_path)
