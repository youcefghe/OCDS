import pyodbc
import logging
from datetime import datetime
from table_creation import create_tables  # Make sure table_creation.py is in the same directory

# ---------------------------------------------------------------------------
# Configure logging (to both file and console)
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("etl.log", mode='w', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# --- Helper Functions ---
def safe_str(val):
    return str(val).strip() if val else ""

def format_date(val):
    """
    Convert a DB datetime or string into a SQL-friendly datetime string ("YYYY-MM-DD HH:MM:SS").
    """
    if not val:
        return None
    try:
        dt = datetime.strptime(str(val), "%Y-%m-%d %H:%M:%S")
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        return None

def get_first_something(string_val):
    """
    Extracts the first part (e.g., "S7") from a string like "S7 - Services ..."
    """
    if not string_val:
        return ""
    parts = string_val.split('-', maxsplit=1)
    return parts[0].strip()

def map_tender_procurement_method(avis_type):
    """
    Use "open" if type is 3, 16, or 17;
    "direct" if type is 9;
    "limited" if type is 6, 10, or 14.
    """
    if avis_type in (3, 16, 17):
        return "open"
    elif avis_type == 9:
        return "direct"
    elif avis_type in (6, 10, 14):
        return "limited"
    return "open"  # fallback

def map_tender_procurement_method_details(avis_type):
    """
    Provides descriptive strings based on avis.type.
    """
    if avis_type == 3:
        return "Contrat adjugé suite à un appel d’offres public"
    elif avis_type == 6:
        return "Contrat adjugé suite à un appel d’offres sur invitation"
    elif avis_type == 9:
        return "Contrat octroyé de gré à gré"
    elif avis_type == 10:
        return "Contrat adjugé suite à un appel d’offres sur invitations"
    elif avis_type == 14:
        return "Contrat suite à un appel d’offres sur invitation publié au SEAO"
    elif avis_type == 16:
        return "Contrat conclu relatif aux infrastructures de transport"
    elif avis_type == 17:
        return "Contrat conclu - Appel d'offres public non publié au SEAO"
    return "Autre type de contrat"

def map_main_procurement_category(precision_val):
    if precision_val == 1:
        return "Services professionnels"
    elif precision_val == 2:
        return "Services de nature technique"
    return "Autres"

def map_additional_procurement_categories(nature_val):
    mapping = {
        1: "Approvisionnement (biens)",
        2: "Services",
        3: "Travaux de construction",
        5: "Autre",
        6: "Concession",
        7: "Vente de biens immeubles",
        8: "Vente de biens meubles"
    }
    return [mapping[nature_val]] if nature_val in mapping else []

# --- Party Upsert Functions ---
def upsert_party(party_id, name, street, locality, region, postal, country, details, cursor):
    """
    Upserts a party record into the parties table.
    Checks by exact name: if a party with the same name exists, it updates and returns its party_id;
    otherwise, it inserts a new record.
    """
    sql_check = "SELECT party_id FROM parties WHERE name = ?"
    cursor.execute(sql_check, (name,))
    row = cursor.fetchone()
    if row:
        existing_party_id = row[0]
        sql_update = """
        UPDATE parties
        SET street_address = ?, locality = ?, region = ?, postal_code = ?, country_name = ?, details = ?
        WHERE party_id = ?
        """
        params_update = (street, locality, region, postal, country, details, existing_party_id)
        cursor.execute(sql_update, params_update)
        return existing_party_id
    else:
        sql_insert = """
        INSERT INTO parties (party_id, name, street_address, locality, region, postal_code, country_name, details)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        params_insert = (party_id, name, street, locality, region, postal, country, details)
        cursor.execute(sql_insert, params_insert)
        return party_id

def insert_release_party(ocid, party_id, role, cursor):
    """
    Inserts a linking row into release_parties if not already present.
    """
    sql = """
    IF NOT EXISTS (SELECT 1 FROM release_parties WHERE ocid = ? AND party_id = ? AND role = ?)
    BEGIN
        INSERT INTO release_parties (ocid, party_id, role)
        VALUES (?, ?, ?)
    END
    """
    params = (ocid, party_id, role, ocid, party_id, role)
    cursor.execute(sql, params)

def upsert_bid(party_id, ocid, admissible, conform, value, value_unit, cursor):
    """
    Upserts a bid record into the bids table.
    """
    sql = """
    IF EXISTS (SELECT 1 FROM bids WHERE party_id = ? AND ocid = ?)
    BEGIN
        UPDATE bids
        SET admissible = ?, conform = ?, value = ?, value_unit = ?
        WHERE party_id = ? AND ocid = ?
    END
    ELSE
    BEGIN
        INSERT INTO bids (party_id, ocid, related_lot, admissible, conform, value, value_unit)
        VALUES (?, ?, NULL, ?, ?, ?, ?)
    END
    """
    params = (
        party_id, ocid,
        admissible, conform, value, value_unit,
        party_id, ocid,
        party_id, ocid, admissible, conform, value, value_unit
    )
    cursor.execute(sql, params)

# --- Cleanup Function for History Tables ---
def cleanup_history_tables(cursor):
    """
    Deletes all data from the specified history tables.
    """
    history_tables = [
        "bids_history",
        "contract_transactions_history",
        "contracts_history",
        "parties_history",
        "release_parties_history",
        "releases_history"
    ]
    for table in history_tables:
        logging.info(f"Deleting all data from {table}...")
        cursor.execute(f"DELETE FROM {table}")

# --- Transformation & Load Functions ---

def transform_avis(source_cursor, target_cursor):
    """
    Process avis rows that meet two conditions:
      - The avis row's categorieseao is in the specified list.
      - It has at least one matching contract (by numeroseao).
    """
    sql_avis = """
    SELECT a.numeroseao, a.numero, a.organisme, a.municipal, a.adresse1, a.adresse2, a.ville, a.province, a.pays, a.codepostal,
           a.titre, a.datepublication, a.datefermeture, a.hyperlienseao, a.unspscprincipale, a.disposition, a.categorieseao,
           a.type, a.precision, a.nature
    FROM avis a
    WHERE a.categorieseao IN (
        'G12 - Moteurs, turbines, composants et accessoires connexes',
        'C02 - Ouvrages de génie civil',
        'G31 - Équipement de transport et pièces de rechange',
        'S8 - Contrôle de la qualité, essais et inspections et services de représentants techniques',
        'S5 - Services environnementaux',
        'G19 - Machinerie et outils',
        'S19 - Location à bail ou location d''installations immobilières',
        'G25 - Constructions préfabriquées',
        'G6 - Matériaux de construction',
        'C01 - Bâtiments',
        'IMM1 - Vente de biens immeubles',
        'G25 - Constructions préfabriqués',
        'C03 - Autres travaux de construction',
        'S3 - Services d''architecture et d''ingénierie'
    )
    AND EXISTS (
         SELECT 1 FROM contrats c
         WHERE c.numeroseao = a.numeroseao
    )
    """
    source_cursor.execute(sql_avis)
    rows = source_cursor.fetchall()
    total_avis = len(rows)
    logging.info(f"Found {total_avis} avis rows (filtered by categories) that have a matching contract.")

    for idx, row in enumerate(rows, start=1):
        logging.info(f"[avis] Processing row {idx}/{total_avis} → numeroseao={row.numeroseao}")
        
        ocid = "ocds-ec9k95-" + safe_str(row.numeroseao)
        release_id = safe_str(row.numero)
        date_val = format_date(row.datepublication)
        tag_val = "avis"
        initiation_type = "tender"
        language = "fr"

        try:
            typed_type = int(row.type)
        except:
            typed_type = 0
        try:
            typed_precision = int(row.precision)
        except:
            typed_precision = 0
        try:
            typed_nature = int(row.nature)
        except:
            typed_nature = 0

        buyer_id = "OP-" + safe_str(row.numeroseao)
        buyer_party_id = upsert_party(
            buyer_id,
            safe_str(row.organisme),
            safe_str(row.adresse1) + " " + safe_str(row.adresse2),
            safe_str(row.ville),
            safe_str(row.province),
            safe_str(row.codepostal),
            safe_str(row.pays),
            '{"Municipal": "' + ("1" if row.municipal else "0") + '"}',
            target_cursor
        )
        
        sql_release = """
        IF EXISTS (SELECT 1 FROM releases WHERE ocid = ?)
        BEGIN
            UPDATE releases SET
                release_id = ?,
                date = ?,
                tag = ?,
                initiation_type = ?,
                language = ?,
                tender_id = ?,
                tender_title = ?,
                tender_status = ?,
                tender_procurement_method = ?,
                tender_procurement_method_details = ?,
                tender_main_procurement_category = ?,
                tender_additional_procurement_categories = ?,
                tender_procuring_entity_id = ?,
                tender_start_date = ?,
                tender_end_date = ?,
                tender_documents = ?,
                tender_item_id = ?,
                tender_item_description = ?,
                tender_item_classification_scheme = ?,
                tender_item_classification_id = ?,
                tender_item_classification_description = ?,
                tender_item_additional_scheme = ?,
                tender_item_additional_id = ?,
                tender_item_additional_description = ?
            WHERE ocid = ?
        END
        ELSE
        BEGIN
            INSERT INTO releases (
                ocid, release_id, date, tag, initiation_type, language,
                tender_id, tender_title, tender_status, tender_procurement_method,
                tender_procurement_method_details, tender_main_procurement_category,
                tender_additional_procurement_categories, tender_procuring_entity_id,
                tender_start_date, tender_end_date, tender_documents,
                tender_item_id, tender_item_description, tender_item_classification_scheme,
                tender_item_classification_id, tender_item_classification_description,
                tender_item_additional_scheme, tender_item_additional_id, tender_item_additional_description
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        END
        """

        tender_proc_method = map_tender_procurement_method(typed_type)
        tender_proc_method_details = map_tender_procurement_method_details(typed_type)
        tender_main_cat = map_main_procurement_category(typed_precision)
        tender_addl_cat = map_additional_procurement_categories(typed_nature)
        tender_addl_cat_str = ",".join(tender_addl_cat)
        item_id = get_first_something(safe_str(row.categorieseao))
        
        vals = (
            ocid, release_id, date_val, tag_val, initiation_type, language,
            release_id, safe_str(row.titre), "complete",
            tender_proc_method,
            tender_proc_method_details,
            tender_main_cat,
            tender_addl_cat_str,
            buyer_party_id,
            date_val,
            format_date(row.datefermeture),
            safe_str(row.hyperlienseao),
            item_id,
            safe_str(row.categorieseao),
            "UNSPSC",
            safe_str(row.unspscprincipale),
            safe_str(row.disposition),
            "CATEGORY",
            item_id,
            safe_str(row.categorieseao),
            ocid,
            # For INSERT, same values repeated:
            ocid, release_id, date_val, tag_val, initiation_type, language,
            release_id, safe_str(row.titre), "complete",
            tender_proc_method,
            tender_proc_method_details,
            tender_main_cat,
            tender_addl_cat_str,
            buyer_party_id,
            date_val,
            format_date(row.datefermeture),
            safe_str(row.hyperlienseao),
            item_id,
            safe_str(row.categorieseao),
            "UNSPSC",
            safe_str(row.unspscprincipale),
            safe_str(row.disposition),
            "CATEGORY",
            item_id,
            safe_str(row.categorieseao)
        )
        target_cursor.execute(sql_release, vals)
        
        insert_release_party(ocid, buyer_party_id, "buyer", target_cursor)
        
        sql_suppliers = """
        SELECT af.adjudicataire, af.admissible, af.conforme, af.montantsoumis, af.montantssoumisunite,
               f.neq, f.nomorganisation, f.adresse1, f.adresse2, f.ville, f.province, f.pays, f.codepostal
        FROM avis_fournisseurs af
        JOIN fournisseurs f ON af.neq = f.neq
        WHERE af.numeroseao = ?
        """
        source_cursor.execute(sql_suppliers, row.numeroseao)
        suppliers = source_cursor.fetchall()
        
        supplier_count = 0
        for s in suppliers:
            supplier_id = "FO-" + safe_str(s.neq) if safe_str(s.neq) else "FO-MISSING"
            supplier_party_id = upsert_party(
                supplier_id,
                safe_str(s.nomorganisation),
                safe_str(s.adresse1) + " " + safe_str(s.adresse2),
                safe_str(s.ville),
                safe_str(s.province),
                safe_str(s.codepostal),
                safe_str(s.pays),
                '{"NEQ": "' + safe_str(s.neq) + '"}',
                target_cursor
            )
            role = "supplier" if s.adjudicataire else "tenderer"
            insert_release_party(ocid, supplier_party_id, role, target_cursor)
            
            bid_admissible = 1 if s.admissible else 0
            bid_conform = 1 if s.conforme else 0
            bid_value = float(s.montantsoumis) if s.montantsoumis is not None else 0.0
            bid_value_unit = str(s.montantssoumisunite) if s.montantssoumisunite is not None else "CAD"
            upsert_bid(supplier_party_id, ocid, bid_admissible, bid_conform, bid_value, bid_value_unit, target_cursor)
            
            supplier_count += 1
        
        sql_update_tender = "UPDATE releases SET tender_number_of_tenderers = ? WHERE ocid = ?"
        target_cursor.execute(sql_update_tender, (supplier_count, ocid))

def transform_contrats(source_cursor, target_cursor):
    """
    Process contrats rows and load into the contracts table.
    Only take contrats that have a matching avis row (with specified categories).
    SWAP: period_end_date is set from datepublicationfinale, and date_signed from datefinale (or fallback).
    """
    sql_contrats = """
    SELECT c.numeroseao, c.numero, c.datefinale, c.datepublicationfinale, c.montantfinal
    FROM contrats c
    WHERE c.numeroseao IN (
        SELECT a.numeroseao
        FROM avis a
        WHERE a.categorieseao IN (
            'G12 - Moteurs, turbines, composants et accessoires connexes',
            'C02 - Ouvrages de génie civil',
            'G31 - Équipement de transport et pièces de rechange',
            'S8 - Contrôle de la qualité, essais et inspections et services de représentants techniques',
            'S5 - Services environnementaux',
            'G19 - Machinerie et outils',
            'S19 - Location à bail ou location d''installations immobilières',
            'G25 - Constructions préfabriquées',
            'G6 - Matériaux de construction',
            'C01 - Bâtiments',
            'IMM1 - Vente de biens immeubles',
            'G25 - Constructions préfabriquées',
            'C03 - Autres travaux de construction',
            'S3 - Services d''architecture et d''ingénierie'
        )
    )
    """
    source_cursor.execute(sql_contrats)
    rows = source_cursor.fetchall()
    total_contrats = len(rows)
    logging.info(f"Found {total_contrats} contrats rows that match an avis with specified categories.")

    for idx, row in enumerate(rows, start=1):
        logging.info(f"[contrats] Processing row {idx}/{total_contrats} → numeroseao={row.numeroseao}")
        
        ocid = "ocds-ec9k95-" + safe_str(row.numeroseao)
        contract_id = safe_str(row.numero)
        period_end_date = format_date(row.datepublicationfinale)  # SWAP logic
        date_signed = format_date(row.datefinale if row.datefinale else row.datepublicationfinale)
        status = "active" if not row.datepublicationfinale else "terminated"
        amount = float(row.montantfinal or 0.0)
        
        sql_contract = """
        IF EXISTS (SELECT 1 FROM contracts WHERE contract_id = ?)
        BEGIN
            UPDATE contracts
            SET ocid = ?, status = ?, period_end_date = ?, value_amount = ?, date_signed = ?
            WHERE contract_id = ?
        END
        ELSE
        BEGIN
            INSERT INTO contracts (contract_id, ocid, status, period_end_date, value_amount, date_signed)
            VALUES (?, ?, ?, ?, ?, ?)
        END
        """
        vals = (
            contract_id,
            ocid, status, period_end_date, amount, date_signed,
            contract_id,
            contract_id, ocid, status, period_end_date, amount, date_signed
        )
        target_cursor.execute(sql_contract, vals)

def transform_depenses(source_cursor, target_cursor):
    """
    Process depenses rows and load into the contract_transactions table.
    Only take depenses rows that match an avis row (with specified categories).
    """
    sql_dep = """
    SELECT d.depense_id, d.numeroseao, d.datedepense, d.montantdepense, d.description
    FROM depenses d
    WHERE d.numeroseao IN (
        SELECT a.numeroseao
        FROM avis a
        WHERE a.categorieseao IN (
            'G12 - Moteurs, turbines, composants et accessoires connexes',
            'C02 - Ouvrages de génie civil',
            'G31 - Équipement de transport et pièces de rechange',
            'S8 - Contrôle de la qualité, essais et inspections et services de représentants techniques',
            'S5 - Services environnementaux',
            'G19 - Machinerie et outils',
            'S19 - Location à bail ou location d''installations immobilières',
            'G25 - Constructions préfabriquées',
            'G6 - Matériaux de construction',
            'C01 - Bâtiments',
            'IMM1 - Vente de biens immeubles',
            'G25 - Constructions préfabriquées',
            'C03 - Autres travaux de construction',
            'S3 - Services d''architecture et d''ingénierie'
        ) AND EXISTS (SELECT 1 FROM contrats c WHERE c.numeroseao = a.numeroseao)
    )
    """
    source_cursor.execute(sql_dep)
    rows = source_cursor.fetchall()
    total_depenses = len(rows)
    logging.info(f"Found {total_depenses} depenses rows that match an avis with specified categories.")

    for idx, row in enumerate(rows, start=1):
        logging.info(f"[depenses] Processing row {idx}/{total_depenses} → depense_id={row.depense_id}")
        
        ocid = "ocds-ec9k95-" + safe_str(row.numeroseao)
        txn_id = "txn-" + safe_str(row.depense_id)
        txn_date = format_date(row.datedepense)
        amount = float(row.montantdepense or 0.0)
        source_desc = safe_str(row.description)
        
        sql_txn = """
        IF EXISTS (SELECT 1 FROM contract_transactions WHERE ocid = ? AND transaction_id = ?)
        BEGIN
            UPDATE contract_transactions
            SET contract_id = NULL, source = ?, date = ?, value_amount = ?, value_currency = 'CAD'
            WHERE ocid = ? AND transaction_id = ?
        END
        ELSE
        BEGIN
            INSERT INTO contract_transactions (ocid, transaction_id, contract_id, source, date, value_amount, value_currency)
            VALUES (?, ?, NULL, ?, ?, ?, 'CAD')
        END
        """
        vals = (
            ocid, txn_id, source_desc, txn_date, amount, ocid, txn_id,
            ocid, txn_id, source_desc, txn_date, amount
        )
        target_cursor.execute(sql_txn, vals)

# ---------------------------------------------------------------------------
# MAIN MIGRATION FUNCTION
# ---------------------------------------------------------------------------
def migrate_data():
    try:
        # Connect to source (XMLtest)
        src_conn = pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=DESKTOP-91AK8MU\\SQLEXPRESS;"
            "DATABASE=XMLData;"
            "Trusted_Connection=yes;"
        )
        src_cursor = src_conn.cursor()
        
        # Connect to target 
        tgt_conn = pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=DESKTOP-91AK8MU\\SQLEXPRESS;"
            "DATABASE=ConstructionData;"
            "Trusted_Connection=yes;"
        )
        tgt_cursor = tgt_conn.cursor()
        
        logging.info("🔨 Creating tables in JSontest...")
        create_tables(tgt_cursor)
        tgt_conn.commit()
        
        # Process avis, contrats, and depenses as before
        logging.info("🔍 Transforming and loading avis data (only those with specified categories, plus matching contrat)...")
        transform_avis(src_cursor, tgt_cursor)
        tgt_conn.commit()
        
        logging.info("🔍 Transforming and loading contrats data (only those with a matching avis with specified categories)...")
        transform_contrats(src_cursor, tgt_cursor)
        tgt_conn.commit()
        
        logging.info("🔍 Transforming and loading depenses data (only those with a matching avis with specified categories)...")
        transform_depenses(src_cursor, tgt_cursor)
        tgt_conn.commit()
        
        # Cleanup history tables after migration
        logging.info("🧹 Cleaning up history tables...")
        cleanup_history_tables(tgt_cursor)
        tgt_conn.commit()
        
        logging.info("✅ Migration completed successfully.")
        print("✅ Migration completed successfully.")
        
    except Exception as e:
        logging.error(f"❌ Migration failed: {str(e)}")
        print(f"❌ Migration failed: {str(e)}")
        tgt_conn.rollback()
    finally:
        src_cursor.close()
        src_conn.close()
        tgt_cursor.close()
        tgt_conn.close()
        logging.info("🔌 All connections closed.")
        print("🔌 All connections closed.")

if __name__ == "__main__":
    migrate_data()
