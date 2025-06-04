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

# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Helper Functions  (add this right under safe_str / format_date)
# ---------------------------------------------------------------------------
def safe_int(val, default=0):
    """
    Convert val to int; if it fails (None, '', non‑numeric), return default.
    """
    try:
        return int(str(val).strip())
    except Exception:
        return default

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
    return string_val.split('-', 1)[0].strip()

def map_tender_procurement_method(avis_type):
    if avis_type in (3, 16, 17):
        return "open"
    if avis_type == 9:
        return "direct"
    if avis_type in (6, 10, 14):
        return "limited"
    return "open"

def map_tender_procurement_method_details(avis_type):
    return {
        3:  "Contrat adjugé suite à un appel d’offres public",
        6:  "Contrat adjugé suite à un appel d’offres sur invitation",
        9:  "Contrat octroyé de gré à gré",
        10: "Contrat adjugé suite à un appel d’offres sur invitations",
        14: "Contrat suite à un appel d’offres sur invitation publié au SEAO",
        16: "Contrat conclu relatif aux infrastructures de transport",
        17: "Contrat conclu - Appel d'offres public non publié au SEAO"
    }.get(avis_type, "Autre type de contrat")

def map_main_procurement_category(precision_val):
    return {1: "Services professionnels", 2: "Services de nature technique"}.get(precision_val, "Autres")

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

# ---------------------------------------------------------------------------
# Party / Bid helpers (unchanged)
# ---------------------------------------------------------------------------
def upsert_party(party_id, name, street, locality, region, postal, country, details, cursor):
    sql_check = "SELECT party_id FROM parties WHERE name = ?"
    cursor.execute(sql_check, (name,))
    row = cursor.fetchone()
    if row:
        cursor.execute(
            """UPDATE parties
               SET street_address=?, locality=?, region=?, postal_code=?,
                   country_name=?, details=?
               WHERE party_id=?""",
            (street, locality, region, postal, country, details, row[0])
        )
        return row[0]

    cursor.execute(
        """INSERT INTO parties
           (party_id,name,street_address,locality,region,postal_code,country_name,details)
           VALUES (?,?,?,?,?,?,?,?)""",
        (party_id, name, street, locality, region, postal, country, details)
    )
    return party_id

def insert_release_party(ocid, party_id, role, cursor):
    cursor.execute(
        """IF NOT EXISTS (SELECT 1 FROM release_parties WHERE ocid=? AND party_id=? AND role=?)
           INSERT INTO release_parties (ocid,party_id,role) VALUES (?,?,?)""",
        (ocid, party_id, role, ocid, party_id, role)
    )

def upsert_bid(party_id, ocid, admissible, conform, value, value_unit, cursor):
    cursor.execute(
        """IF EXISTS (SELECT 1 FROM bids WHERE party_id=? AND ocid=?)
               UPDATE bids
               SET admissible=?, conform=?, value=?, value_unit=?
               WHERE party_id=? AND ocid=?
           ELSE
               INSERT INTO bids
               (party_id,ocid,related_lot,admissible,conform,value,value_unit)
               VALUES (?,?,NULL,?,?,?,?)""",
        (party_id, ocid, admissible, conform, value, value_unit,
         party_id, ocid, party_id, ocid, admissible, conform, value, value_unit)
    )

# ---------------------------------------------------------------------------
# NEW helper for awards + supplier link
# ---------------------------------------------------------------------------
def insert_award_and_link(award_id, ocid, amount, total_amount, supplier_id, cursor):
    # 1) award row ----------------------------------------------------------
    cursor.execute(
        """
        IF NOT EXISTS (SELECT 1 FROM awards WHERE award_id = ?)
        BEGIN
            INSERT INTO awards
            (award_id, ocid, status, date, value_amount, value_currency, value_total_amount)
            VALUES (?, ?, 'active', GETDATE(), ?, 'CAD', ?)
        END
        """,
        (award_id,               #   ?  in SELECT
         award_id, ocid,         #  ?,?  in INSERT
         amount, total_amount)   #     ?,? in INSERT
    )

    # 2) supplier‑award link -----------------------------------------------
    cursor.execute(
        """
        IF NOT EXISTS (SELECT 1
                       FROM suppliers_awards
                       WHERE award_id = ? AND supplier_id = ?)
        BEGIN
            INSERT INTO suppliers_awards
            (award_id, supplier_id, supplier_ocid)
            VALUES (?, ?, ?)
        END
        """,
        (award_id, supplier_id,        #   ?,?  in SELECT
         award_id, supplier_id, ocid)  #   ?,?,? in INSERT
    )

# ---------------------------------------------------------------------------
# Cleanup history tables (unchanged)
# ---------------------------------------------------------------------------
def cleanup_history_tables(cursor):
    history_tables = [
        "bids_history",
        "contract_transactions_history",
        "contracts_history",
        "parties_history",
        "release_parties_history",
        "releases_history"
    ]
    for tbl in history_tables:
        logging.info(f"Deleting all data from {tbl} …")
        cursor.execute(f"DELETE FROM {tbl}")

# ---------------------------------------------------------------------------
# TRANSFORM FUNCTIONS
# ---------------------------------------------------------------------------
def transform_avis(source_cursor, target_cursor):
    """
    Loads avis + suppliers + bids, and now also creates awards /
    suppliers_awards rows for each winning supplier (adjudicataire = 1).
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
    AND EXISTS (SELECT 1 FROM contrats c WHERE c.numeroseao = a.numeroseao)
    """
    source_cursor.execute(sql_avis)
    avis_rows = source_cursor.fetchall()
    logging.info(f"Found {len(avis_rows)} avis rows to load")

    # -----------------------------------------------------------------------
    for idx, row in enumerate(avis_rows, 1):
        ocid       = "ocds-ec9k95-" + safe_str(row.numeroseao)
        release_id = safe_str(row.numero)
        date_val   = format_date(row.datepublication)

        # Buyer party --------------------------------------------------------
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

        # Release upsert -----------------------------------------------------
        typed_type      = safe_int(row.type)
        typed_precision = safe_int(row.precision)
        typed_nature    = safe_int(row.nature)

        tender_proc_method          = map_tender_procurement_method(typed_type)
        tender_proc_method_details  = map_tender_procurement_method_details(typed_type)
        tender_main_cat             = map_main_procurement_category(typed_precision)
        tender_addl_cat_str         = ",".join(map_additional_procurement_categories(typed_nature))
        item_id                     = get_first_something(safe_str(row.categorieseao))

        target_cursor.execute(
            """
            IF EXISTS (SELECT 1 FROM releases WHERE ocid = ?)
            BEGIN
                UPDATE releases SET
                    release_id = ?,
                    date       = ?,
                    tag        = 'avis',
                    initiation_type = 'tender',
                    language   = 'fr',
                    tender_id  = ?,
                    tender_title = ?,
                    tender_status = 'complete',
                    tender_procurement_method = ?,
                    tender_procurement_method_details = ?,
                    tender_main_procurement_category = ?,
                    tender_additional_procurement_categories = ?,
                    tender_procuring_entity_id = ?,
                    tender_start_date = ?,
                    tender_end_date   = ?,
                    tender_documents  = ?,
                    tender_item_id    = ?,
                    tender_item_description = ?,
                    tender_item_classification_scheme = 'UNSPSC',
                    tender_item_classification_id     = ?,
                    tender_item_classification_description = ?,
                    tender_item_additional_scheme = 'CATEGORY',
                    tender_item_additional_id     = ?,
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
                VALUES (?,?,?,?,?,'fr',?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            END
            """,
            (
                ocid, release_id, date_val,  # UPDATE
                release_id, safe_str(row.titre),
                tender_proc_method, tender_proc_method_details,
                tender_main_cat, tender_addl_cat_str, buyer_party_id,
                date_val, format_date(row.datefermeture), safe_str(row.hyperlienseao),
                item_id, safe_str(row.categorieseao), safe_str(row.unspscprincipale),
                safe_str(row.disposition), item_id, safe_str(row.categorieseao),
                ocid,  # WHERE

                # INSERT values ------------------------------------------------
                ocid, release_id, date_val, "avis", "tender",
                release_id, safe_str(row.titre), "complete",
                tender_proc_method, tender_proc_method_details,
                tender_main_cat, tender_addl_cat_str, buyer_party_id,
                date_val, format_date(row.datefermeture), safe_str(row.hyperlienseao),
                item_id, safe_str(row.categorieseao), "UNSPSC",
                safe_str(row.unspscprincipale), safe_str(row.disposition),
                "CATEGORY", item_id, safe_str(row.categorieseao)
            )
        )

        insert_release_party(ocid, buyer_party_id, "buyer", target_cursor)

        # Suppliers ----------------------------------------------------------
        source_cursor.execute(
            """
            SELECT af.adjudicataire, af.admissible, af.conforme, af.montantsoumis,
                   af.montantssoumisunite, af.montantcontrat, af.montanttotalcontrat,
                   f.neq, f.nomorganisation, f.adresse1, f.adresse2, f.ville, f.province,
                   f.pays, f.codepostal
            FROM avis_fournisseurs af
            JOIN fournisseurs f ON af.neq = f.neq
            WHERE af.numeroseao = ?
            """,
            row.numeroseao
        )
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

            upsert_bid(
                supplier_party_id, ocid,
                1 if s.admissible else 0,
                1 if s.conforme else 0,
                float(s.montantsoumis or 0),
                safe_str(s.montantssoumisunite) or "CAD",
                target_cursor
            )

            # ---- NEW: create award for winners -----------------------------
            if s.adjudicataire:
                insert_award_and_link(
                    award_id   = release_id,    # same id as avis/contract
                    ocid       = ocid,
                    amount     = float(s.montantcontrat or 0),
                    total_amount = float(s.montanttotalcontrat or 0),
                    supplier_id  = supplier_party_id,
                    cursor     = target_cursor
                )

            supplier_count += 1

        target_cursor.execute(
            "UPDATE releases SET tender_number_of_tenderers = ? WHERE ocid = ?",
            (supplier_count, ocid)
        )

        logging.info(f"[avis] {idx}/{len(avis_rows)} processed → ocid={ocid}")

# ---------------------------------------------------------------------------
# transform_contrats  (unchanged)
# ---------------------------------------------------------------------------
def transform_contrats(source_cursor, target_cursor):
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
    logging.info(f"Found {len(rows)} contrats rows")

    for r in rows:
        ocid = "ocds-ec9k95-" + safe_str(r.numeroseao)
        contract_id = safe_str(r.numero)
        period_end_date = format_date(r.datepublicationfinale)
        date_signed = format_date(r.datefinale or r.datepublicationfinale)
        status = "active" if not r.datepublicationfinale else "terminated"
        amount = float(r.montantfinal or 0)

        target_cursor.execute(
            """
            IF EXISTS (SELECT 1 FROM contracts WHERE contract_id = ?)
            BEGIN
                UPDATE contracts
                SET ocid = ?, status = ?, period_end_date = ?, value_amount = ?, date_signed = ?
                WHERE contract_id = ?
            END
            ELSE
            BEGIN
                INSERT INTO contracts
                (contract_id, ocid, status, period_end_date, value_amount, date_signed)
                VALUES (?, ?, ?, ?, ?, ?)
            END
            """,
            (contract_id, ocid, status, period_end_date, amount, date_signed,
             contract_id, contract_id, ocid, status, period_end_date, amount, date_signed)
        )

# ---------------------------------------------------------------------------
# transform_depenses  (unchanged)
# ---------------------------------------------------------------------------
def transform_depenses(source_cursor, target_cursor):
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
    logging.info(f"Found {len(rows)} depenses rows")

    for r in rows:
        ocid = "ocds-ec9k95-" + safe_str(r.numeroseao)
        txn_id = "txn-" + safe_str(r.depense_id)
        txn_date = format_date(r.datedepense)
        amount = float(r.montantdepense or 0)
        source_desc = safe_str(r.description)

        target_cursor.execute(
            """
            IF EXISTS (SELECT 1 FROM contract_transactions WHERE ocid = ? AND transaction_id = ?)
            BEGIN
                UPDATE contract_transactions
                SET contract_id = NULL, source = ?, date = ?, value_amount = ?, value_currency = 'CAD'
                WHERE ocid = ? AND transaction_id = ?
            END
            ELSE
            BEGIN
                INSERT INTO contract_transactions
                (ocid, transaction_id, contract_id, source, date, value_amount, value_currency)
                VALUES (?, ?, NULL, ?, ?, ?, 'CAD')
            END
            """,
            (ocid, txn_id, source_desc, txn_date, amount, ocid, txn_id,
             ocid, txn_id, source_desc, txn_date, amount)
        )

# ---------------------------------------------------------------------------
# MAIN MIGRATION
# ---------------------------------------------------------------------------
def migrate_data():
    try:
        src_conn = pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=DESKTOP-91AK8MU\\SQLEXPRESS;"
            "DATABASE=XMLData;"
            "Trusted_Connection=yes;"
        )
        tgt_conn = pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=DESKTOP-91AK8MU\\SQLEXPRESS;"
            "DATABASE=ConstructionDB;"
            "Trusted_Connection=yes;"
        )
        src_cur, tgt_cur = src_conn.cursor(), tgt_conn.cursor()

        logging.info("🔨 Ensuring tables exist …")
        create_tables(tgt_cur)
        tgt_conn.commit()

        logging.info("🔍 transform_avis (+awards) …")
        transform_avis(src_cur, tgt_cur)
        tgt_conn.commit()

        logging.info("🔍 transform_contrats …")
        transform_contrats(src_cur, tgt_cur)
        tgt_conn.commit()

        logging.info("🔍 transform_depenses …")
        transform_depenses(src_cur, tgt_cur)
        tgt_conn.commit()

        logging.info("🧹 Cleaning history tables …")
        cleanup_history_tables(tgt_cur)
        tgt_conn.commit()

        logging.info("✅ Migration completed successfully.")
        print("✅ Migration completed successfully.")

    except Exception as e:
        logging.error(f"❌ Migration failed: {e}")
        print("❌ Migration failed:", e)
        tgt_conn.rollback()
    finally:
        src_cur.close(); src_conn.close()
        tgt_cur.close(); tgt_conn.close()
        logging.info("🔌 All connections closed.")

# ---------------------------------------------------------------------------
if __name__ == "__main__":
    migrate_data()
