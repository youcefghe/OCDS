"""
full_etl_with_unique_parties.py

A complete Python ETL script that:
  1) Connects to the source (XMLtest) and target (JSontest) databases.
  2) Creates the target schema by calling create_tables() from table_creation.py.
  3) Transforms and loads data from the avis, contrats, and depenses tables.

For parties, the script checks by exact name. If a party with the same name exists, it updates and returns its party_id,
so that duplicate records are not created. That returned party_id is then used when inserting into the release_parties table
and for upserting bids. Additionally, the field tender_procuring_entity_id in releases is set to the same value
as the buyer’s party_id.

In this version, we only updated map_tender_procurement_method and map_tender_procurement_method_details
so that types 10 and 14 are also considered "limited" with their respective descriptive strings.

We have also added:
  - Logging to file (etl.log) and console
  - Progress messages showing how many rows have been processed
  - Filter so that:
      - transform_avis only selects rows with categorieseao in the specified list AND matching contrats or depenses,
      - transform_contrats only selects rows that match an avis with categories in that list,
      - transform_depenses only selects rows that match an avis with categories in that list.

Now, in transform_contrats, we swap period_end_date and date_signed.
"""

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
    Use <open> if type is 3,16,17
    Use <direct> if type is 9
    Use <limited> if type is 6,10,14
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
    Provide textual details for each avis.type:
      3  => 'Contrat adjugé suite à un appel d’offres public'
      6  => 'Contrat adjugé suite à un appel d’offres sur invitation'
      9  => 'Contrat octroyé de gré à gré'
      10 => 'Contrat adjugé suite à un appel d’offres sur invitations'
      14 => 'Contrat suite à un appel d’offres sur invitation publié au SEAO'
      16 => 'Contrat conclu relatif aux infrastructures de transport'
      17 => 'Contrat conclu - Appel d'offres public non publié au SEAO'
      else => 'Autre type de contrat'
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
    First, it checks if a party with the exact name exists.
    If found, updates that record and returns its party_id; otherwise, inserts a new record.
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
    Inserts a row into the release_parties table if not already present.
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

# --- Transformation & Load Functions ---
def transform_avis(source_cursor, target_cursor):
    """
    Process rows from the avis table and load into releases, parties, release_parties, and bids.
    Only take avis that have categorieseao in the specified list,
    AND either a matching contrat or a matching depenses row.
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
        'G25 - Constructions préfabriquées',
        'C03 - Autres travaux de construction',
        'S3 - Services d''architecture et d''ingénierie'
    )
    AND (
        EXISTS (
            SELECT 1 FROM contrats c
            WHERE c.numeroseao = a.numeroseao
        )
        OR EXISTS (
            SELECT 1 FROM depenses d
            WHERE d.numeroseao = a.numeroseao
        )
    )
    """
    source_cursor.execute(sql_avis)
    rows = source_cursor.fetchall()
    total_avis = len(rows)
    logging.info(f"Found {total_avis} avis rows that have categorieseao in the list and match contrat OR depenses.")

    for idx, row in enumerate(rows, start=1):
        logging.info(f"[avis] Processing row {idx}/{total_avis} → numeroseao={row.numeroseao}")
        
        ocid = "ocds-ec9k95-" + safe_str(row.numeroseao)
        release_id = safe_str(row.numero)
        date_val = format_date(row.datepublication)
        tag_val = "avis"
        initiation_type = "tender"
        language = "fr"

        # Convert row.type, row.precision, row.nature to int for mapping
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

        # Upsert Buyer Party first and get its party_id
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
        
        # Upsert Release
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
        
        # Insert Buyer into release_parties
        insert_release_party(ocid, buyer_party_id, "buyer", target_cursor)
        
        # Process Suppliers from avis_fournisseurs
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
        
        # Update tender numberOfTenderers
        sql_update_tender = "UPDATE releases SET tender_number_of_tenderers = ? WHERE ocid = ?"
        target_cursor.execute(sql_update_tender, (supplier_count, ocid))

def transform_contrats(source_cursor, target_cursor):
    """
    Process rows from the contrats table and load into the contracts table.
    Only take contrats that have either a matching avis with categories in the list OR a matching depenses row.
    Logs progress for each row.

    SWAP: We now set period_end_date from row.datepublicationfinale,
          and date_signed from row.datefinale (or fallback).
    """
    sql_contrats = """
    SELECT c.numeroseao, c.numero, c.datefinale, c.datepublicationfinale, c.montantfinal
    FROM contrats c
    WHERE EXISTS (
        SELECT 1 FROM avis a
        WHERE a.numeroseao = c.numeroseao
          AND a.categorieseao IN (
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
    OR EXISTS (
        SELECT 1 FROM depenses d
        WHERE d.numeroseao = c.numeroseao
    )
    """
    source_cursor.execute(sql_contrats)
    rows = source_cursor.fetchall()
    total_contrats = len(rows)
    logging.info(f"Found {total_contrats} contrats rows that match an avis with categories OR a depenses row.")

    for idx, row in enumerate(rows, start=1):
        logging.info(f"[contrats] Processing row {idx}/{total_contrats} → numeroseao={row.numeroseao}")
        
        ocid = "ocds-ec9k95-" + safe_str(row.numeroseao)

        # Ensure a release exists.
        sql_check = "SELECT 1 FROM releases WHERE ocid = ?"
        target_cursor.execute(sql_check, (ocid,))
        if not target_cursor.fetchone():
            dummy_sql = "INSERT INTO releases (ocid, release_id, tag, initiation_type, language) VALUES (?, ?, 'contrat', 'tender', 'fr')"
            target_cursor.execute(dummy_sql, (ocid, ocid))
        
        contract_id = safe_str(row.numero)

        # SWAPPED LOGIC:
        # period_end_date is now from datepublicationfinale
        # date_signed is from datefinale (or fallback)
        period_end_date = format_date(row.datepublicationfinale)
        date_signed = format_date(row.datefinale if row.datefinale else row.datepublicationfinale)

        # If datepublicationfinale is missing, we consider the contract "active", else "terminated"
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
    Process rows from the depenses table and load into the contract_transactions table.
    Only take depenses that have either a matching avis with categories in the list OR a matching contrats row.
    Logs progress for each row.
    """
    sql_dep = """
    SELECT d.depense_id, d.numeroseao, d.datedepense, d.montantdepense, d.description
    FROM depenses d
    WHERE EXISTS (
        SELECT 1 FROM avis a
        WHERE a.numeroseao = d.numeroseao
          AND a.categorieseao IN (
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
    OR EXISTS (
        SELECT 1 FROM contrats c
        WHERE c.numeroseao = d.numeroseao
    )
    """
    source_cursor.execute(sql_dep)
    rows = source_cursor.fetchall()
    total_depenses = len(rows)
    logging.info(f"Found {total_depenses} depenses rows that match an avis with categories OR a contrats row.")

    for idx, row in enumerate(rows, start=1):
        logging.info(f"[depenses] Processing row {idx}/{total_depenses} → depense_id={row.depense_id}")
        
        ocid = "ocds-ec9k95-" + safe_str(row.numeroseao)
        # Ensure a release exists.
        sql_check = "SELECT 1 FROM releases WHERE ocid = ?"
        target_cursor.execute(sql_check, (ocid,))
        if not target_cursor.fetchone():
            dummy_sql = "INSERT INTO releases (ocid, release_id, tag, initiation_type, language) VALUES (?, ?, 'depense', 'tender', 'fr')"
            target_cursor.execute(dummy_sql, (ocid, ocid))
        
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

# --- Main Migration Function ---
def migrate_data():
    try:
        # Connect to source (XMLtest)
        src_conn = pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=DESKTOP-91AK8MU\\SQLEXPRESS;"
            "DATABASE=XMLtest;"
            "Trusted_Connection=yes;"
        )
        src_cursor = src_conn.cursor()
        
        # Connect to target (JSontest)
        tgt_conn = pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=DESKTOP-91AK8MU\\SQLEXPRESS;"
            "DATABASE=jsontest;"
            "Trusted_Connection=yes;"
        )
        tgt_cursor = tgt_conn.cursor()
        
        logging.info("🔨 Creating tables in JSontest...")
        create_tables(tgt_cursor)
        tgt_conn.commit()
        
        logging.info("🔍 Transforming and loading avis data (only those with categories in the list, plus matching contrat OR depenses)...")
        transform_avis(src_cursor, tgt_cursor)
        tgt_conn.commit()
        
        logging.info("🔍 Transforming and loading contrats data (only those with a matching avis that has categories in the list OR a matching depenses row)...")
        transform_contrats(src_cursor, tgt_cursor)
        tgt_conn.commit()
        
        logging.info("🔍 Transforming and loading depenses data (only those with a matching avis that has categories in the list OR a matching contrats row)...")
        transform_depenses(src_cursor, tgt_cursor)
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
