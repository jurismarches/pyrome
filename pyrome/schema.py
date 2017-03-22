

class FK:
    """a foreign key"""


class PK:
    """a primary key"""


class PFK(PK, FK):
    """A primary key and foreign key"""


class Schema:
    """Data schema
    """

    ogr__type = {
        0: "rome",
        1: "env_travail",
        2: "competence",
        3: "appellation",
        4: "activite",
        5: "arborescence"}

    ogr = {
        "code": PK,
        "type": int}

    env_travail = {
        "ogr_oid": PFK,
        "libelle_environnement": str,
        "libelle": str}

    competence = {
        "ogr_oid": PFK,
        "libelle_competence": str,
        "libelle": str}

    appellation = {
        "ogr_oid": PFK,
        "libelle_appellation": str,
        "libelle": str,
        "libelle_court": str}

    activite = {
        "ogr_oid": PFK,
        "libelle_activite": str,
        "libelle": str,
        "libelle_application": str,
        "libelle_en_tete_rgpmt": str,
        "libelle_impression": str}

    rome = {
        "ogr_oid": PFK,
        "code_rome": str,
        "libelle": str}

    rome_appellation = {
        "rome_oid": FK,
        "appellation_oid": FK,
        "priorisation": int}

    rome_env_travail = {
        "rome_oid": FK,
        "env_travail_oid": FK,
        "priorisation": int,
        "bloc": int}

    rome_activite = {
        "rome_oid": FK,
        "activite_oid": FK,
        "position": int,
        "priorisation": int,
        "bloc": int}

    rome_competence = {
        "rome_oid": FK,
        "competence_oid": FK,
        "position": int,
        "priorisation": int,
        "bloc": int}

    mobilite = {
      "origine_rome_oid": FK,
      "cible_rome_oid": FK,
      "type": int}

    fiche = {
        "num": int,
        "rome_oid": FK,
        "definition": str,
        "formations_associees": str,
        "condition_exercice_activite": str,
        "classement_emploi_metier": str}

    arborescence__type_noeud = {
        0: "RACINE",
        1: "NOEUD",
        2: "FEUILLE"}

    referentiel = {
        "libelle": str}

    arborescence = {
        "ogr_oid": FK,
        "pere_ogr_oid": FK,
        "referentiel_oid": FK,
        "item_ogr_oid": FK,
        "item_type": int, 
        "type_noeud": int,
        "code_noeud": str,
        "libelle": str}
