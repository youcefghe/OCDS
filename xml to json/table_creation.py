
def create_tables(cursor):
    # --------------------------------------------------------
    # 1. 'releases' table
    # --------------------------------------------------------
    sql_releases = """
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'releases')
    BEGIN
        CREATE TABLE dbo.releases (
            ocid         NVARCHAR(100) PRIMARY KEY,
            release_id   NVARCHAR(255),
            date         DATETIME,
            tag          NVARCHAR(MAX),
            initiation_type NVARCHAR(255),
            language     NVARCHAR(255),

            -- TENDER fields stored in releases:
            tender_id    NVARCHAR(100),
            tender_title NVARCHAR(MAX),
            tender_status NVARCHAR(255),
            tender_procurement_method NVARCHAR(255),
            tender_procurement_method_details NVARCHAR(MAX),
            tender_procurement_method_rationale NVARCHAR(MAX),
            tender_main_procurement_category NVARCHAR(255),
            tender_additional_procurement_categories NVARCHAR(MAX),
            tender_procuring_entity_id NVARCHAR(100),
            tender_start_date DATETIME,
            tender_end_date   DATETIME,
            tender_duration_in_days INT,
            tender_number_of_tenderers INT,
            tender_documents  NVARCHAR(MAX),

            -- Single tender item columns:
            tender_item_id NVARCHAR(100),
            tender_item_description NVARCHAR(MAX),
            tender_item_classification_scheme NVARCHAR(100),
            tender_item_classification_id NVARCHAR(100),
            tender_item_classification_description NVARCHAR(MAX),

            -- Single additionalClassification columns:
            tender_item_additional_scheme NVARCHAR(100),
            tender_item_additional_id NVARCHAR(100),
            tender_item_additional_description NVARCHAR(MAX)
        );
    END;
    """
    cursor.execute(sql_releases)

    # History for releases
    sql_releases_history = """
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'releases_history')
    BEGIN
        CREATE TABLE dbo.releases_history (
            history_id     INT IDENTITY(1,1) PRIMARY KEY,
            ocid           NVARCHAR(100),
            release_id     NVARCHAR(255),
            date           DATETIME,
            tag            NVARCHAR(MAX),
            initiation_type NVARCHAR(255),
            language       NVARCHAR(255),

            -- copy of TENDER columns
            tender_id      NVARCHAR(100),
            tender_title   NVARCHAR(MAX),
            tender_status  NVARCHAR(255),
            tender_procurement_method NVARCHAR(255),
            tender_procurement_method_details NVARCHAR(MAX),
            tender_procurement_method_rationale NVARCHAR(MAX),
            tender_main_procurement_category NVARCHAR(255),
            tender_additional_procurement_categories NVARCHAR(MAX),
            tender_procuring_entity_id NVARCHAR(100),
            tender_start_date DATETIME,
            tender_end_date   DATETIME,
            tender_duration_in_days INT,
            tender_number_of_tenderers INT,
            tender_documents  NVARCHAR(MAX),

            -- Single tender item columns for history:
            tender_item_id NVARCHAR(100),
            tender_item_description NVARCHAR(MAX),
            tender_item_classification_scheme NVARCHAR(100),
            tender_item_classification_id NVARCHAR(100),
            tender_item_classification_description NVARCHAR(MAX),

            -- Single additionalClassification columns for history:
            tender_item_additional_scheme NVARCHAR(100),
            tender_item_additional_id NVARCHAR(100),
            tender_item_additional_description NVARCHAR(MAX),

            modified_date  DATETIME DEFAULT GETDATE()
        );
    END;
    """
    cursor.execute(sql_releases_history)

    sql_trg_releases_update = """
    IF OBJECT_ID('dbo.trg_releases_update', 'TR') IS NULL
    BEGIN
        EXEC('
        CREATE TRIGGER dbo.trg_releases_update
        ON dbo.releases
        AFTER UPDATE
        AS
        BEGIN
            INSERT INTO dbo.releases_history
                (ocid, release_id, date, tag, initiation_type, language,
                 tender_id, tender_title, tender_status,
                 tender_procurement_method, tender_procurement_method_details,
                 tender_procurement_method_rationale, tender_main_procurement_category,
                 tender_additional_procurement_categories, tender_procuring_entity_id,
                 tender_start_date, tender_end_date, tender_duration_in_days,
                 tender_number_of_tenderers, tender_documents,
                 tender_item_id, tender_item_description,
                 tender_item_classification_scheme, tender_item_classification_id,
                 tender_item_classification_description,
                 tender_item_additional_scheme, tender_item_additional_id,
                 tender_item_additional_description, modified_date)
            SELECT
                d.ocid,
                d.release_id,
                d.date,
                d.tag,
                d.initiation_type,
                d.language,

                d.tender_id,
                d.tender_title,
                d.tender_status,
                d.tender_procurement_method,
                d.tender_procurement_method_details,
                d.tender_procurement_method_rationale,
                d.tender_main_procurement_category,
                d.tender_additional_procurement_categories,
                d.tender_procuring_entity_id,
                d.tender_start_date,
                d.tender_end_date,
                d.tender_duration_in_days,
                d.tender_number_of_tenderers,
                d.tender_documents,

                d.tender_item_id,
                d.tender_item_description,
                d.tender_item_classification_scheme,
                d.tender_item_classification_id,
                d.tender_item_classification_description,
                d.tender_item_additional_scheme,
                d.tender_item_additional_id,
                d.tender_item_additional_description,
                GETDATE()
            FROM deleted d;
        END
        ')
    END;
    """
    cursor.execute(sql_trg_releases_update)

    # --------------------------------------------------------
    # 2. 'parties' + 'release_parties'
    # --------------------------------------------------------
    sql_parties = """
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'parties')
    BEGIN
        CREATE TABLE dbo.parties (
            party_id       NVARCHAR(100) PRIMARY KEY,
            name           NVARCHAR(255),
            role           NVARCHAR(255),
            street_address NVARCHAR(MAX),
            locality       NVARCHAR(255),
            region         NVARCHAR(255),
            postal_code    NVARCHAR(20),
            country_name   NVARCHAR(255),
            details        NVARCHAR(MAX),
            alias_parties  NVARCHAR(MAX) NULL
        );
    END;
    """
    cursor.execute(sql_parties)

    sql_parties_history = """
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'parties_history')
    BEGIN
        CREATE TABLE dbo.parties_history (
            history_id     INT IDENTITY(1,1) PRIMARY KEY,
            party_id       NVARCHAR(100),
            name           NVARCHAR(255),
            role           NVARCHAR(255),
            street_address NVARCHAR(MAX),
            locality       NVARCHAR(255),
            region         NVARCHAR(255),
            postal_code    NVARCHAR(20),
            country_name   NVARCHAR(255),
            details        NVARCHAR(MAX),
            alias_parties  NVARCHAR(MAX),
            modified_date  DATETIME DEFAULT GETDATE()
        );
    END;
    """
    cursor.execute(sql_parties_history)

    sql_trg_parties_update = """
    IF OBJECT_ID('dbo.trg_parties_update', 'TR') IS NULL
    BEGIN
        EXEC('
        CREATE TRIGGER dbo.trg_parties_update
        ON dbo.parties
        AFTER UPDATE
        AS
        BEGIN
            INSERT INTO dbo.parties_history
                (party_id, name, role, street_address, locality, region, postal_code,
                 country_name, details, alias_parties, modified_date)
            SELECT
                d.party_id,
                d.name,
                d.role,
                d.street_address,
                d.locality,
                d.region,
                d.postal_code,
                d.country_name,
                d.details,
                d.alias_parties,
                GETDATE()
            FROM deleted d;
        END
        ')
    END;
    """
    cursor.execute(sql_trg_parties_update)

    sql_release_parties = """
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'release_parties')
    BEGIN
        CREATE TABLE dbo.release_parties (
            ocid     NVARCHAR(100),
            party_id NVARCHAR(100),
            role     NVARCHAR(255),
            PRIMARY KEY (ocid, party_id, role),
            FOREIGN KEY (ocid) REFERENCES dbo.releases (ocid),
            FOREIGN KEY (party_id) REFERENCES dbo.parties (party_id)
        );
    END;
    """
    cursor.execute(sql_release_parties)

    sql_release_parties_history = """
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'release_parties_history')
    BEGIN
        CREATE TABLE dbo.release_parties_history (
            history_id   INT IDENTITY(1,1) PRIMARY KEY,
            ocid         NVARCHAR(100),
            party_id     NVARCHAR(100),
            role         NVARCHAR(255),
            modified_date DATETIME DEFAULT GETDATE()
        );
    END;
    """
    cursor.execute(sql_release_parties_history)

    sql_trg_release_parties_update = """
    IF OBJECT_ID('dbo.trg_release_parties_update', 'TR') IS NULL
    BEGIN
        EXEC('
        CREATE TRIGGER dbo.trg_release_parties_update
        ON dbo.release_parties
        AFTER UPDATE
        AS
        BEGIN
            INSERT INTO dbo.release_parties_history
                (ocid, party_id, role, modified_date)
            SELECT
                d.ocid,
                d.party_id,
                d.role,
                GETDATE()
            FROM deleted d;
        END
        ')
    END;
    """
    cursor.execute(sql_trg_release_parties_update)
    # --------------------------------------------------------
    # 4. 'lots' table
    # --------------------------------------------------------
    sql_lots = """
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'lots')
    BEGIN
        CREATE TABLE dbo.lots (
            lot_id  NVARCHAR(100) PRIMARY KEY,
            ocid    NVARCHAR(100) NOT NULL,
            title   NVARCHAR(MAX),
            status  NVARCHAR(255),
            contract_period_start_date DATETIME,
            contract_period_end_date   DATETIME,
            FOREIGN KEY (ocid) REFERENCES dbo.releases (ocid)
        );
    END;
    """
    cursor.execute(sql_lots)

    sql_lots_history = """
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'lots_history')
    BEGIN
        CREATE TABLE dbo.lots_history (
            history_id                 INT IDENTITY(1,1) PRIMARY KEY,
            lot_id                     NVARCHAR(100),
            ocid                       NVARCHAR(100),
            title                      NVARCHAR(MAX),
            status                     NVARCHAR(255),
            contract_period_start_date DATETIME,
            contract_period_end_date   DATETIME,
            modified_date              DATETIME DEFAULT GETDATE()
        );
    END;
    """
    cursor.execute(sql_lots_history)

    sql_trg_lots_update = """
    IF OBJECT_ID('dbo.trg_lots_update', 'TR') IS NULL
    BEGIN
        EXEC('
        CREATE TRIGGER dbo.trg_lots_update
        ON dbo.lots
        AFTER UPDATE
        AS
        BEGIN
            INSERT INTO dbo.lots_history
                (lot_id, ocid, title, status, contract_period_start_date,
                 contract_period_end_date, modified_date)
            SELECT
                d.lot_id,
                d.ocid,
                d.title,
                d.status,
                d.contract_period_start_date,
                d.contract_period_end_date,
                GETDATE()
            FROM deleted d;
        END
        ')
    END;
    """
    cursor.execute(sql_trg_lots_update)

    # --------------------------------------------------------
    # 5. 'bids'
    # --------------------------------------------------------
    sql_bids = """
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'bids')
    BEGIN
        CREATE TABLE dbo.bids (
            bid_row_id INT IDENTITY(1,1) PRIMARY KEY,
            party_id   NVARCHAR(100),
            ocid       NVARCHAR(100),
            related_lot NVARCHAR(100) NULL,
            admissible BIT,
            conform    BIT,
            value      DECIMAL(15, 2),
            value_unit NVARCHAR(255),
            FOREIGN KEY (party_id) REFERENCES dbo.parties (party_id),
            FOREIGN KEY (related_lot) REFERENCES dbo.lots (lot_id)
        );
    END;
    """
    cursor.execute(sql_bids)

    sql_bids_history = """
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'bids_history')
    BEGIN
        CREATE TABLE dbo.bids_history (
            history_id  INT IDENTITY(1,1) PRIMARY KEY,
            bid_row_id  INT,
            party_id    NVARCHAR(100),
            ocid        NVARCHAR(100),
            related_lot NVARCHAR(100),
            admissible  BIT,
            conform     BIT,
            value       DECIMAL(15, 2),
            value_unit  NVARCHAR(255),
            modified_date DATETIME DEFAULT GETDATE()
        );
    END;
    """
    cursor.execute(sql_bids_history)

    sql_trg_bids_update = """
    IF OBJECT_ID('dbo.trg_bids_update', 'TR') IS NULL
    BEGIN
        EXEC('
        CREATE TRIGGER dbo.trg_bids_update
        ON dbo.bids
        AFTER UPDATE
        AS
        BEGIN
            INSERT INTO dbo.bids_history
                (bid_row_id, party_id, ocid, related_lot, admissible, conform,
                 value, value_unit, modified_date)
            SELECT
                d.bid_row_id,
                d.party_id,
                d.ocid,
                d.related_lot,
                d.admissible,
                d.conform,
                d.value,
                d.value_unit,
                GETDATE()
            FROM deleted d;
        END
        ')
    END;
    """
    cursor.execute(sql_trg_bids_update)

    # --------------------------------------------------------
    # 6. 'awards'
    # --------------------------------------------------------
    sql_awards = """
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'awards')
    BEGIN
        CREATE TABLE dbo.awards (
            award_id           NVARCHAR(255) PRIMARY KEY,
            ocid               NVARCHAR(100),
            status             NVARCHAR(255),
            date               DATETIME,
            value_amount       DECIMAL(15, 2),
            value_currency     NVARCHAR(10),
            value_total_amount DECIMAL(15, 2),
            FOREIGN KEY (ocid) REFERENCES dbo.releases (ocid)
        );
    END;
    """
    cursor.execute(sql_awards)

    sql_awards_history = """
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'awards_history')
    BEGIN
        CREATE TABLE dbo.awards_history (
            history_id         INT IDENTITY(1,1) PRIMARY KEY,
            award_id           NVARCHAR(255),
            ocid               NVARCHAR(100),
            status             NVARCHAR(255),
            date               DATETIME,
            value_amount       DECIMAL(15, 2),
            value_currency     NVARCHAR(10),
            value_total_amount DECIMAL(15, 2),
            modified_date      DATETIME DEFAULT GETDATE()
        );
    END;
    """
    cursor.execute(sql_awards_history)

    sql_trg_awards_update = """
    IF OBJECT_ID('dbo.trg_awards_update', 'TR') IS NULL
    BEGIN
        EXEC('
        CREATE TRIGGER dbo.trg_awards_update
        ON dbo.awards
        AFTER UPDATE
        AS
        BEGIN
            INSERT INTO dbo.awards_history
                (award_id, ocid, status, date, value_amount, value_currency,
                 value_total_amount, modified_date)
            SELECT
                d.award_id,
                d.ocid,
                d.status,
                d.date,
                d.value_amount,
                d.value_currency,
                d.value_total_amount,
                GETDATE()
            FROM deleted d;
        END
        ')
    END;
    """
    cursor.execute(sql_trg_awards_update)

    # --------------------------------------------------------
    # 7. 'suppliers_awards'
    # --------------------------------------------------------
    sql_suppliers_awards = """
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'suppliers_awards')
    BEGIN
        CREATE TABLE dbo.suppliers_awards (
            award_id     NVARCHAR(255),
            supplier_id  NVARCHAR(100),
            supplier_ocid NVARCHAR(100),
            FOREIGN KEY (supplier_id) REFERENCES dbo.parties (party_id),
            FOREIGN KEY (award_id) REFERENCES dbo.awards (award_id)
        );
    END;
    """
    cursor.execute(sql_suppliers_awards)

    sql_suppliers_awards_history = """
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'suppliers_awards_history')
    BEGIN
        CREATE TABLE dbo.suppliers_awards_history (
            history_id    INT IDENTITY(1,1) PRIMARY KEY,
            award_id      NVARCHAR(255),
            supplier_id   NVARCHAR(100),
            supplier_ocid NVARCHAR(100),
            modified_date DATETIME DEFAULT GETDATE()
        );
    END;
    """
    cursor.execute(sql_suppliers_awards_history)

    sql_trg_suppliers_awards_update = """
    IF OBJECT_ID('dbo.trg_suppliers_awards_update', 'TR') IS NULL
    BEGIN
        EXEC('
        CREATE TRIGGER dbo.trg_suppliers_awards_update
        ON dbo.suppliers_awards
        AFTER UPDATE
        AS
        BEGIN
            INSERT INTO dbo.suppliers_awards_history
                (award_id, supplier_id, supplier_ocid, modified_date)
            SELECT
                d.award_id,
                d.supplier_id,
                d.supplier_ocid,
                GETDATE()
            FROM deleted d;
        END
        ')
    END;
    """
    cursor.execute(sql_trg_suppliers_awards_update)

    # --------------------------------------------------------
    # 8. 'contracts'
    # --------------------------------------------------------
    sql_contracts = """
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'contracts')
    BEGIN
        CREATE TABLE dbo.contracts (
            contract_id    NVARCHAR(255) PRIMARY KEY,
            ocid           NVARCHAR(100),
            award_id       NVARCHAR(255),
            status         NVARCHAR(255),
            period_end_date DATETIME,
            value_amount   DECIMAL(15, 2),
            value_currency NVARCHAR(10),
            date_signed    DATETIME,
            FOREIGN KEY (ocid) REFERENCES dbo.releases (ocid),
            FOREIGN KEY (award_id) REFERENCES dbo.awards (award_id)
        );
    END;
    """
    cursor.execute(sql_contracts)

    sql_contracts_history = """
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'contracts_history')
    BEGIN
        CREATE TABLE dbo.contracts_history (
            history_id     INT IDENTITY(1,1) PRIMARY KEY,
            contract_id    NVARCHAR(255),
            ocid           NVARCHAR(100),
            award_id       NVARCHAR(255),
            status         NVARCHAR(255),
            period_end_date DATETIME,
            value_amount   DECIMAL(15, 2),
            value_currency NVARCHAR(10),
            date_signed    DATETIME,
            modified_date  DATETIME DEFAULT GETDATE()
        );
    END;
    """
    cursor.execute(sql_contracts_history)

    sql_trg_contracts_update = """
    IF OBJECT_ID('dbo.trg_contracts_update', 'TR') IS NULL
    BEGIN
        EXEC('
        CREATE TRIGGER dbo.trg_contracts_update
        ON dbo.contracts
        AFTER UPDATE
        AS
        BEGIN
            INSERT INTO dbo.contracts_history
                (contract_id, ocid, award_id, status, period_end_date, value_amount,
                 value_currency, date_signed, modified_date)
            SELECT
                d.contract_id,
                d.ocid,
                d.award_id,
                d.status,
                d.period_end_date,
                d.value_amount,
                d.value_currency,
                d.date_signed,
                GETDATE()
            FROM deleted d;
        END
        ')
    END;
    """
    cursor.execute(sql_trg_contracts_update)

    # --------------------------------------------------------
    # 9. 'contract_amendments'
    # --------------------------------------------------------
    sql_amendments = """
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'contract_amendments')
    BEGIN
        CREATE TABLE dbo.contract_amendments (
            amendment_id   NVARCHAR(100) NOT NULL,
            contract_id    NVARCHAR(255) NOT NULL,
            rationale      NVARCHAR(MAX),
            amendment_date DATETIME,
            PRIMARY KEY (amendment_id, contract_id),
            FOREIGN KEY (contract_id) REFERENCES dbo.contracts (contract_id)
        );
    END;
    """
    cursor.execute(sql_amendments)

    sql_amendments_history = """
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'contract_amendments_history')
    BEGIN
        CREATE TABLE dbo.contract_amendments_history (
            history_id     INT IDENTITY(1,1) PRIMARY KEY,
            amendment_id   NVARCHAR(100),
            contract_id    NVARCHAR(255),
            rationale      NVARCHAR(MAX),
            amendment_date DATETIME,
            modified_date  DATETIME DEFAULT GETDATE()
        );
    END;
    """
    cursor.execute(sql_amendments_history)

    sql_trg_amendments_update = """
    IF OBJECT_ID('dbo.trg_contract_amendments_update', 'TR') IS NULL
    BEGIN
        EXEC('
        CREATE TRIGGER dbo.trg_contract_amendments_update
        ON dbo.contract_amendments
        AFTER UPDATE
        AS
        BEGIN
            INSERT INTO dbo.contract_amendments_history
                (amendment_id, contract_id, rationale, amendment_date, modified_date)
            SELECT
                d.amendment_id,
                d.contract_id,
                d.rationale,
                d.amendment_date,
                GETDATE()
            FROM deleted d;
        END
        ')
    END;
    """
    cursor.execute(sql_trg_amendments_update)

     # --------------------------------------------------------
    # 10) 'contract_transactions' 
    # --------------------------------------------------------
    sql_contract_transactions = """
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'contract_transactions')
    BEGIN
        CREATE TABLE dbo.contract_transactions (
            ocid              NVARCHAR(100),
            transaction_id    NVARCHAR(255),
            contract_id       NVARCHAR(255),
            source            NVARCHAR(MAX),
            date              DATETIME,
            value_amount      DECIMAL(15,2),
            value_currency    NVARCHAR(10),

            -- Primary Key now (ocid, transaction_id)
            PRIMARY KEY (ocid, transaction_id),

            FOREIGN KEY (contract_id) REFERENCES dbo.contracts (contract_id),
            FOREIGN KEY (ocid) REFERENCES dbo.releases (ocid)
        );
    END;
    """
    cursor.execute(sql_contract_transactions)

    # History table for contract_transactions 
    sql_contract_transactions_history = """
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'contract_transactions_history')
    BEGIN
        CREATE TABLE dbo.contract_transactions_history (
            history_id        INT IDENTITY(1,1) PRIMARY KEY,

            ocid              NVARCHAR(100),
            transaction_id    NVARCHAR(255),
            contract_id       NVARCHAR(255),
            source            NVARCHAR(MAX),
            date              DATETIME,
            value_amount      DECIMAL(15,2),
            value_currency    NVARCHAR(10),

            modified_date     DATETIME DEFAULT GETDATE()
        );
    END;
    """
    cursor.execute(sql_contract_transactions_history)

    sql_trg_contract_transactions_update = """
    IF OBJECT_ID('dbo.trg_contract_transactions_update', 'TR') IS NULL
    BEGIN
        EXEC('
        CREATE TRIGGER dbo.trg_contract_transactions_update
        ON dbo.contract_transactions
        AFTER UPDATE
        AS
        BEGIN
            INSERT INTO dbo.contract_transactions_history
                (ocid, transaction_id, contract_id, source, date, value_amount,
                 value_currency, modified_date)
            SELECT
                d.ocid,
                d.transaction_id,
                d.contract_id,
                d.source,
                d.date,
                d.value_amount,
                d.value_currency,
                GETDATE()
            FROM deleted d;
        END
        ')
    END;
    """
    cursor.execute(sql_trg_contract_transactions_update)

    # --------------------------------------------------------
    # 11. 'related_processes'
    # --------------------------------------------------------
    sql_related_processes = """
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'related_processes')
    BEGIN
        CREATE TABLE dbo.related_processes (
            id           NVARCHAR(255) PRIMARY KEY,
            ocid         NVARCHAR(100),
            identifier   NVARCHAR(255),
            uri          NVARCHAR(MAX),
            relationship NVARCHAR(MAX),
            title        NVARCHAR(MAX),
            scheme       NVARCHAR(255),
            FOREIGN KEY (ocid) REFERENCES dbo.releases (ocid)
        );
    END;
    """
    cursor.execute(sql_related_processes)

    sql_related_processes_history = """
    IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'related_processes_history')
    BEGIN
        CREATE TABLE dbo.related_processes_history (
            history_id   INT IDENTITY(1,1) PRIMARY KEY,
            id           NVARCHAR(255),
            ocid         NVARCHAR(100),
            identifier   NVARCHAR(255),
            uri          NVARCHAR(MAX),
            relationship NVARCHAR(MAX),
            title        NVARCHAR(MAX),
            scheme       NVARCHAR(255),
            modified_date DATETIME DEFAULT GETDATE()
        );
    END;
    """
    cursor.execute(sql_related_processes_history)

    sql_trg_related_processes_update = """
    IF OBJECT_ID('dbo.trg_related_processes_update', 'TR') IS NULL
    BEGIN
        EXEC('
        CREATE TRIGGER dbo.trg_related_processes_update
        ON dbo.related_processes
        AFTER UPDATE
        AS
        BEGIN
            INSERT INTO dbo.related_processes_history
                (id, ocid, identifier, uri, relationship, title, scheme, modified_date)
            SELECT
                d.id,
                d.ocid,
                d.identifier,
                d.uri,
                d.relationship,
                d.title,
                d.scheme,
                GETDATE()
            FROM deleted d;
        END
        ')
    END;
    """
    cursor.execute(sql_trg_related_processes_update)
