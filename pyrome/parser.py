import collections
import sqlite3
import xml.etree.ElementTree as ET
import zipfile
from itertools import islice

from schema import PK, FK, PFK, Schema


class Loader:

    BATCH_SIZE = 1000

    ogr_fname = collections.OrderedDict([
        ("activite", "unix_referentiel_activite_v330_iso8859-15.xml"),
        ("appellation", "unix_referentiel_appellation_v330_iso8859-15.xml"),
        ("env_travail", "unix_referentiel_env_travail_v330_iso8859-15.xml"),
        ("competence", "unix_referentiel_competence_v330_iso8859-15.xml"),
        ("rome", "unix_referentiel_code_rome_v330_iso8859-15.xml")])

    fiche_fname = "unix_fiche_emploi_metier_v330_iso8859-15.xml"

    arborescence_fname = "unix_arborescence_v330_iso8859-15.xml"

    keymap = {
        "activite" : {"ogr_oid": "code_ogr"},
        "appellation" : {"ogr_oid": "code_ogr"},
        "env_travail" : {"ogr_oid": "code_ogr"},
        "competence" : {"ogr_oid": "code_ogr"},
        "rome" : {"ogr_oid": "code_ogr"},
        "rome_appellation" : {"appellation_oid": "code_ogr"},
        "rome_env_travail" : {"env_travail_oid": "code_ogr"},
        "rome_activite" : {"activite_oid": "code_ogr"},
        "arborescence" : {
            "ogr_oid": "code_ogr",
            "pere_ogr_oid": "code_ogr_pere",
            "item_oid": "code_item_arbor_associe"}}

    type_to_sqlite3 = {PK:"INTEGER", FK: "INTEGER", PFK: "INTEGER", int: "INTEGER", str: "TEXT"}

    def _batched(self, iterator):
        while True:
            batch = list(islice(iterator, None, self.BATCH_SIZE))
            if batch:
                yield batch
            else:
                raise StopIteration()

    def get_conn(self, db_path):
        return sqlite3.connect(db_path)

    def create_table(self, name, db_conn):
        cols = getattr(Schema, name)
        col_defs = []
        for cname, ctype in cols.items():
            col_def = "%s %s" % (cname, self.type_to_sqlite3[ctype])
            if issubclass(ctype, PK):
                col_def += " PRIMARY KEY"
            if issubclass(ctype, FK):
                # remove _oid
                table_name = cname[:-4]
                while not hasattr(Schema, table_name):
                    # remove first part
                    table_name = table_name.split("_", 1)[1]
                col_def += " REFERENCES %s (oid)" % (table_name,)
            col_defs.append(col_def)
        db_conn.execute("""
            CREATE TABLE %s (%s)""" % (name, ",".join(col_defs)))

    def iter_data(self, name, content, defaults={}):
        keymap = self.keymap.get(name, {})
        if not isinstance(content, (ET.Element, ET.ElementTree)):
            tree = ET.fromstring(content)
        else:
            tree = content
        cols = getattr(Schema, name)
        for item in tree:
            # make dict
            data = {child.tag: child.text for child in item}
            # transform
            for cname, ctype in cols.items():
                key = keymap.get(cname, cname)
                if ctype in {FK, PK, PFK}:
                    ctype = int
                value = data.get(key, defaults.get(key))
                data[cname] = ctype(value) if value is not None else None
            yield data

    def insert_data(self, name, data, cursor):
        cols = getattr(Schema, name)
        col_names = ", ".join(cols.keys())
        col_params = ", ".join(":" + k for k in cols.keys())
        statement = """
            INSERT INTO %s
            (%s)
            VALUES
            (%s)""" % (name, col_names, col_params)
        cursor.executemany(statement, data)

    def load_data(self, name, content, db_conn, defaults={}):
        cursor = db_conn.cursor()
        for data in self._batched(self.iter_data(name, content, defaults)):
            self.insert_data(name, data, cursor)
        cursor.close()

    def load_card_bloc(self, bloc, db_conn, defaults):
        self.load_data(
            "rome_activite",
            bloc.find("activite_de_base") or bloc.find("activite_specifique"),
            db_conn,
            defaults)
        self.load_data(
            "rome_competence",
            bloc.find("savoir_theorique_et_proceduraux"),
            db_conn,
            defaults)
        self.load_data(
            "rome_competence",
            bloc.find("savoir_action"),
            db_conn,
            defaults)

    def load_rome_code(self, db_conn):   
        # load
        results = db_conn.execute("SELECT code_rome, ogr_oid FROM rome;")
        self.rome_code_ogr = dict(results)

    def load_mobilite(self, content, db_conn, defaults):
        cursor = db_conn.cursor()
        for data in self._batched(self.iter_data("mobilite", content, defaults)):
            # adjust
            for d in data:
                code = d["code_rome_cible"].split(maxsplit=1)[0]
                d["cible_rome_oid"] = self.rome_code_ogr[code]
            # create
            self.insert_data("mobilite", data, cursor)
        cursor.close()

    def load_cards(self, content, db_conn):
        """load cards
        """
        tree = ET.fromstring(content)
        self.load_rome_code(db_conn)
        for card in tree:
            data = {}
            data["num"] = card.find("numero").text
            data["rome_oid"] = card.find("bloc_code_rome").find("code_ogr").text
            defaults = {"rome_oid": data["rome_oid"], "bloc": None}
            self.load_data(
                "rome_appellation",
                card.find("appellation"),
                db_conn,
                defaults)
            self.load_data(
                "rome_env_travail",
                card.find("environnement_de_travail"),
                db_conn,
                defaults)
            self.load_card_bloc(card.find("les_activites_de_base"), db_conn, defaults)
            for bloc in card.find("les_activites_specifique"):
                defaults = {
                    "rome_oid": data["rome_oid"],
                    "bloc": bloc.find("position_bloc").text}
                self.load_card_bloc(bloc, db_conn, defaults)
            self.load_mobilite(
                card.find("les_mobilites").find("proche"),
                db_conn,
                {"origine_rome_oid": int(data["rome_oid"])})

    def load_arborescence(self, content, db_conn):
        cursor = db_conn.cursor()
        noeud_to_type = {v: k for k, v in Schema.arborescence__type_noeud.items()}
        referentiel_label_to_root = {}
        for data in self._batched(self.iter_data("arborescence", content)):
            # transform
            for d in data:
                if not d["item_ogr_oid"]:
                    d["item_ogr_oid"] = None
                if not d["code_ogr_pere"] or not d["code_ogr_pere"].strip():
                    d["pere_ogr_oid"] = None
                    referentiel_label_to_root[d["libelle_referentiel"]] = d["ogr_oid"]
                d["referentiel_oid"] = referentiel_label_to_root[d["libelle_referentiel"]]
                # for now set item_type to None, we will compute it later
                d["item_type"] = None
                d["type_noeud"] = noeud_to_type[d["libelle_noeud"]]
            self.insert_data("arborescence", data, cursor)
        cursor.close()

    def fill_ogr(self, db_conn):
        for ctype, tname in Schema.ogr__type.items():
            db_conn.execute("""
                INSERT INTO ogr (code, type)
                SELECT ogr_oid as code, %d as type FROM %s""" %
                (ctype, tname))
            # and arborescence
            db_conn.execute("""
                UPDATE arborescence
                SET item_type = %d
                WHERE item_ogr_oid IN (SELECT ogr_oid FROM %s)
                """ % (ctype, tname))

    def __call__(self, zip_path, db_path):
        with self.get_conn(db_path) as db_conn:
            with zipfile.ZipFile(zip_path, 'r') as z:
                for name, path in self.ogr_fname.items():
                    self.create_table(name, db_conn)
                    self.load_data(name, z.read(path), db_conn)
                    db_conn.commit()
                self.create_table("rome_appellation", db_conn)
                self.create_table("rome_env_travail", db_conn)
                self.create_table("rome_activite", db_conn)
                self.create_table("rome_competence", db_conn)
                self.create_table("mobilite", db_conn)
                self.create_table("fiche", db_conn)
                db_conn.commit()
                self.load_cards(z.read(self.fiche_fname), db_conn)
                db_conn.commit()
                self.create_table("arborescence", db_conn)
                self.load_arborescence(z.read(self.arborescence_fname), db_conn)
                db_conn.commit()
                self.create_table("ogr", db_conn)
                self.fill_ogr(db_conn)
                db_conn.commit()


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
    if os.path.exists(args.db_path):
        if not args.overwrite:
            print("database exists", file=sys.stderr)
            exit(1)
        else:
            os.remove(args.db_path)
    Loader()(args.zip_path, args.db_path)
