import peewee as pw


rome_db = pw.SqliteDatabase(None)


class BaseModel(pw.Model):

    class Meta:
        database = rome_db


class Ogr(BaseModel):
    """All Ogr objects are linked to this table
    """

    TYPE = (
        (0, "rome"),
        (1, "env_travail"),
        (2, "competence"),
        (3, "appellation"),
        (4, "activite"),
        (5, "arborescence"))

    code = pw.PrimaryKeyField()
    type = pw.IntegerField(choices=TYPE)


class EnvTravail(BaseModel):
    ogr = pw.ForeignKeyField(Ogr, related_name="env_travail", primary_key=True)
    libelle_environnement = pw.CharField()
    libelle = pw.CharField()


class Competence(BaseModel):
    ogr = pw.ForeignKeyField(Ogr, related_name="competence", primary_key=True)
    libelle_competence = pw.CharField()
    libelle = pw.CharField()


class Appellation(BaseModel):
    ogr = pw.ForeignKeyField(Ogr, related_name="appellation", primary_key=True)
    libelle_appellation = pw.CharField()
    libelle = pw.CharField()
    libelle_court = pw.CharField()


class Activite(BaseModel):
    ogr = pw.ForeignKeyField(Ogr, related_name="activite", primary_key=True)
    libelle_activite = pw.CharField()
    libelle = pw.CharField()
    libelle_application = pw.CharField(null=True)
    libelle_en_tete_rgpmt = pw.CharField(null=True)
    libelle_impression = pw.CharField(null=True)


class Rome(BaseModel):
    ogr = pw.ForeignKeyField(Ogr, related_name="rome", primary_key=True)
    code_rome = pw.CharField()
    libelle = pw.CharField()

    @staticmethod
    def full_prefetch(query):
        """full prefetch of corelated tables
        """
        return pw.prefetch(
            query,
            RomeAppellation.select(RomeAppellation, Appellation).join(Appellation),
            RomeEnvTravail.select(RomeEnvTravail, EnvTravail).join(EnvTravail),
            RomeActivite.select(RomeActivite, Activite).join(Activite),
            RomeCompetence.select(RomeCompetence, Competence).join(Competence))


class RomeAppellation(BaseModel):
    rome = pw.ForeignKeyField(Rome, related_name="rome_appellation")
    appellation = pw.ForeignKeyField(Appellation, related_name="appellation_rome")
    priorisation = pw.IntegerField()


class RomeEnvTravail(BaseModel):
    rome = pw.ForeignKeyField(Rome, related_name="rome_env_travail")
    env_travail = pw.ForeignKeyField(EnvTravail, related_name="env_travail_rome")
    priorisation = pw.IntegerField()
    bloc = pw.IntegerField(null=True)


class RomeActivite(BaseModel):
    rome = pw.ForeignKeyField(Rome, related_name="rome_activite")
    activite = pw.ForeignKeyField(Activite, related_name="activite_rome")
    position = pw.IntegerField(),
    priorisation = pw.IntegerField(),
    bloc = pw.IntegerField(null=True)


class RomeCompetence(BaseModel):
    rome = pw.ForeignKeyField(Rome, related_name="rome_competence")
    competence = pw.ForeignKeyField(Competence, related_name="competence_rome")
    position = pw.IntegerField()
    priorisation = pw.IntegerField()
    bloc = pw.IntegerField(null=True)


class Mobilite(BaseModel):
    TYPE = [
        (0, "proche"),
        (1, "si_evolution")]
    origine_rome = pw.ForeignKeyField(Rome, related_name="mobilite_origine")
    cible_rome = pw.ForeignKeyField(Rome, related_name="mobilite_cible")
    type = pw.IntegerField(choices=TYPE)


class Fiche(BaseModel):
    numero = pw.IntegerField(null=True)
    rome = pw.ForeignKeyField(Rome, related_name="fiche")
    definition = pw.CharField(),
    formations_associees = pw.CharField(),
    condition_exercice_activite = pw.CharField(),
    classement_emploi_metier = pw.CharField()


class Referentiel(BaseModel):
    ogr = pw.ForeignKeyField(Ogr, primary_key=True, related_name="referentiel")
    libelle = pw.CharField()


class Arborescence(BaseModel):
    TYPE_NOEUD = (
        (0, "RACINE"),
        (1, "NOEUD"),
        (2, "FEUILLE"))

    ogr = pw.ForeignKeyField(Ogr, primary_key=True, related_name="arborescence")
    pere = pw.ForeignKeyField('self', related_name="fils", null=True)
    referentiel = pw.ForeignKeyField(Referentiel, related_name="noeuds")
    item_ogr = pw.ForeignKeyField(Ogr, related_name="noeuds", null=True)
    type_noeud = pw.IntegerField(choices=TYPE_NOEUD)
    code_noeud = pw.CharField()
    libelle = pw.CharField()


class RomeDB:
    """a context manager to configure and connect database
    """

    def __init__(self, *args):
        rome_db.init(*args)

    def __enter__(self):
        rome_db.connect()
        return rome_db

    def __exit__(self, *args, **kwargs):
        rome_db.close()


ogr_type_model = {
    0: Rome, 1: EnvTravail, 2: Competence, 3: Appellation, 4: Activite, 5: Arborescence}
