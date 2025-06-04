import pyodbc

def create_tables(cursor):
    """
    Creates the main + history tables:
      - avis, avis_history
      - fournisseurs, fournisseurs_history
      - avis_fournisseurs
      - contrats, contrats_history
      - depenses, depenses_history
    """

    # ---------- AVIS + AVIS_HISTORY ----------
    sql_avis = """
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'avis')
    BEGIN
        CREATE TABLE avis (
            numeroseao    NVARCHAR(50) NOT NULL PRIMARY KEY,
            numero        NVARCHAR(50) NULL,
            organisme     NVARCHAR(MAX) NULL,
            municipal     BIT           NULL,
            adresse1      NVARCHAR(MAX) NULL,
            adresse2      NVARCHAR(MAX) NULL,
            ville         NVARCHAR(MAX) NULL,
            province      NVARCHAR(50)  NULL,
            pays          NVARCHAR(50)  NULL,
            codepostal    NVARCHAR(20)  NULL,
            titre         NVARCHAR(MAX) NULL,
            [type]        NVARCHAR(100) NULL,
            [nature]      NVARCHAR(100) NULL,
            [precision]   NVARCHAR(100) NULL,
            categorieseao NVARCHAR(MAX) NULL,
            datepublication       DATETIME NULL,
            datefermeture         DATETIME NULL,
            datesaisieouverture   DATETIME NULL,
            datesaisieadjudication DATETIME NULL,
            dateadjudication      DATETIME NULL,
            regionlivraison       NVARCHAR(50) NULL,
            unspscprincipale      NVARCHAR(50) NULL,
            disposition           NVARCHAR(MAX) NULL,
            hyperlienseao         NVARCHAR(MAX) NULL,
            source_file           NVARCHAR(MAX) NULL,
            imported_at           DATETIME      DEFAULT GETDATE()
        );
    END;
    """
    cursor.execute(sql_avis)

    sql_avis_history = """
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'avis_history')
    BEGIN
        CREATE TABLE avis_history (
            avis_history_id INT IDENTITY(1,1) PRIMARY KEY,
            numeroseao      NVARCHAR(50),
            numero          NVARCHAR(50) NULL,
            organisme       NVARCHAR(MAX) NULL,
            municipal       BIT           NULL,
            adresse1        NVARCHAR(MAX) NULL,
            adresse2        NVARCHAR(MAX) NULL,
            ville           NVARCHAR(MAX) NULL,
            province        NVARCHAR(50)  NULL,
            pays            NVARCHAR(50)  NULL,
            codepostal      NVARCHAR(20)  NULL,
            titre           NVARCHAR(MAX) NULL,
            [type]          NVARCHAR(100) NULL,
            [nature]        NVARCHAR(100) NULL,
            [precision]     NVARCHAR(100) NULL,
            categorieseao   NVARCHAR(MAX) NULL,
            datepublication       DATETIME NULL,
            datefermeture         DATETIME NULL,
            datesaisieouverture   DATETIME NULL,
            datesaisieadjudication DATETIME NULL,
            dateadjudication      DATETIME NULL,
            regionlivraison       NVARCHAR(50) NULL,
            unspscprincipale      NVARCHAR(50) NULL,
            disposition           NVARCHAR(MAX) NULL,
            hyperlienseao         NVARCHAR(MAX) NULL,
            source_file           NVARCHAR(MAX) NULL,
            imported_at           DATETIME DEFAULT GETDATE(),
            archived_at           DATETIME DEFAULT GETDATE()
        );
    END;
    """
    cursor.execute(sql_avis_history)

    # ---------- FOURNISSEURS + HISTORY ----------
    sql_fournisseurs = """
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'fournisseurs')
    BEGIN
        CREATE TABLE fournisseurs (
            fourn_id        INT IDENTITY(1,1) PRIMARY KEY,
            neq             NVARCHAR(50) NULL,
            nomorganisation NVARCHAR(MAX) NULL,
            adresse1        NVARCHAR(MAX) NULL,
            adresse2        NVARCHAR(MAX) NULL,
            ville           NVARCHAR(MAX) NULL,
            province        NVARCHAR(50)  NULL,
            pays            NVARCHAR(50)  NULL,
            codepostal      NVARCHAR(20)  NULL,
            existing_neq    NVARCHAR(50)  NULL,
            source_file     NVARCHAR(MAX) NULL,
            imported_at     DATETIME      DEFAULT GETDATE()
        );
    END;
    """
    cursor.execute(sql_fournisseurs)

    sql_fournisseurs_history = """
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'fournisseurs_history')
    BEGIN
        CREATE TABLE fournisseurs_history (
            fournisseurs_history_id INT IDENTITY(1,1) PRIMARY KEY,
            fourn_id        INT NULL,
            neq             NVARCHAR(50),
            nomorganisation NVARCHAR(MAX),
            adresse1        NVARCHAR(MAX),
            adresse2        NVARCHAR(MAX),
            ville           NVARCHAR(MAX),
            province        NVARCHAR(50),
            pays            NVARCHAR(50),
            codepostal      NVARCHAR(20),
            existing_neq    NVARCHAR(50),
            source_file     NVARCHAR(MAX),
            imported_at     DATETIME DEFAULT GETDATE(),
            archived_at     DATETIME DEFAULT GETDATE()
        );
    END;
    """
    cursor.execute(sql_fournisseurs_history)

    # ---------- AVIS_FOURNISSEURS ----------
    sql_avis_f = """
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'avis_fournisseurs')
    BEGIN
        CREATE TABLE avis_fournisseurs (
            avis_fourn_id INT IDENTITY(1,1) PRIMARY KEY,
            numeroseao     NVARCHAR(50),
            numero         NVARCHAR(50),
            neq            NVARCHAR(50) NULL,
            nomorganisation NVARCHAR(MAX) NULL,
            admissible     BIT NULL,
            conforme       BIT NULL,
            adjudicataire  BIT NULL,
            montantsoumis  DECIMAL(18,2) NULL,
            montantssoumisunite INT NULL,
            montantcontrat DECIMAL(18,2) NULL,
            montanttotalcontrat DECIMAL(18,2) NULL,
            source_file    NVARCHAR(MAX) NULL,
            imported_at    DATETIME      DEFAULT GETDATE()
        );
    END;
    """
    cursor.execute(sql_avis_f)

    # ---------- CONTRATS + HISTORY ----------
    sql_contrats = """
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'contrats')
    BEGIN
        CREATE TABLE contrats (
            numeroseao        NVARCHAR(50),
            numero            NVARCHAR(50),
            datefinale        DATETIME NULL,
            datepublicationfinale DATETIME NULL,
            montantfinal      DECIMAL(18,2) NULL,
            nomcontractant    NVARCHAR(MAX) NULL,
            neqcontractant    NVARCHAR(50) NULL,
            source_file       NVARCHAR(MAX) NULL,
            imported_at       DATETIME DEFAULT GETDATE(),
            PRIMARY KEY (numeroseao, numero)
        );
    END;
    """
    cursor.execute(sql_contrats)

    sql_contrats_hist = """
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'contrats_history')
    BEGIN
        CREATE TABLE contrats_history (
            contrats_history_id INT IDENTITY(1,1) PRIMARY KEY,
            numeroseao        NVARCHAR(50),
            numero            NVARCHAR(50),
            datefinale        DATETIME NULL,
            datepublicationfinale DATETIME NULL,
            montantfinal      DECIMAL(18,2) NULL,
            nomcontractant    NVARCHAR(MAX) NULL,
            neqcontractant    NVARCHAR(50) NULL,
            source_file       NVARCHAR(MAX) NULL,
            imported_at       DATETIME DEFAULT GETDATE(),
            archived_at       DATETIME DEFAULT GETDATE()
        );
    END;
    """
    cursor.execute(sql_contrats_hist)

    # ---------- DEPENSES + DEPENSES_HISTORY ----------
    sql_depenses = """
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'depenses')
    BEGIN
        CREATE TABLE depenses (
            depense_id INT IDENTITY(1,1) PRIMARY KEY,
            numeroseao  NVARCHAR(50) NOT NULL,
            numero      NVARCHAR(50) NULL,
            datedepense DATETIME NULL,
            datepublicationdepense DATETIME NULL,
            montantdepense DECIMAL(18,2) NULL,
            description  NVARCHAR(MAX) NULL,
            nomcontractant NVARCHAR(MAX) NULL,
            neqcontractant NVARCHAR(50) NULL,
            source_file  NVARCHAR(MAX) NULL,
            imported_at  DATETIME DEFAULT GETDATE()
        );
    END;
    """
    cursor.execute(sql_depenses)

    sql_depenses_history = """
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'depenses_history')
    BEGIN
        CREATE TABLE depenses_history (
            depense_hist_id INT IDENTITY(1,1) PRIMARY KEY,
            numeroseao       NVARCHAR(50),
            numero           NVARCHAR(50) NULL,
            datedepense      DATETIME NULL,
            datepublicationdepense DATETIME NULL,
            montantdepense   DECIMAL(18,2) NULL,
            description      NVARCHAR(MAX) NULL,
            nomcontractant   NVARCHAR(MAX) NULL,
            neqcontractant   NVARCHAR(50) NULL,
            source_file      NVARCHAR(MAX) NULL,
            imported_at      DATETIME DEFAULT GETDATE(),
            archived_at      DATETIME DEFAULT GETDATE()
        );
    END;
    """
    cursor.execute(sql_depenses_history)
