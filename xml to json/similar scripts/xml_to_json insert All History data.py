import pyodbc
import logging
from datetime import datetime
from table_creation import create_tables  # Ensure this creates your *_history tables* as well

# ---------------------------------------------------------------------------
# Configure logging (to both file and console)
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("history_etl.log", mode='w', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# ----------------- Helper Functions (same as old script) -------------------
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

def get_numeroseao_from_ocid(ocid):
    """
    If ocid = "ocds-ec9k95-XXXX", extract 'XXXX' as numeroseao.
    Adjust if your ocid format differs.
    """
    prefix = "ocds-ec9k95-"
    if ocid.startswith(prefix):
        return ocid[len(prefix):]
    return ocid  # fallback

def get_first_something(string_val):
    if not string_val:
        return ""
    parts = string_val.split('-', maxsplit=1)
    return parts[0].strip()

def map_tender_procurement_method(avis_type):
    if avis_type in (3, 16, 17):
        return "open"
    elif avis_type == 9:
        return "direct"
    elif avis_type in (6, 10, 14):
        return "limited"
    return "open"  # fallback

def map_tender_procurement_method_details(avis_type):
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

# ---------------------------------------------------------------------------
# 1) transform_avis_history -> releases_history
# ---------------------------------------------------------------------------
def transform_avis_history(src_cursor, tgt_cursor, ocid_list):
    """
    For each ocid in new DB, parse numeroseao, then read from 'avis_history'
    (no category filter this time), then insert into 'releases_history'.
    """
    logging.info("→ transform_avis_history: start.")
    count_inserted = 0

    insert_sql = """
    INSERT INTO releases_history (
        ocid,
        release_id,
        date,
        tag,
        initiation_type,
        language,
        tender_id,
        tender_title,
        tender_status,
        tender_procurement_method,
        tender_procurement_method_details,
        tender_main_procurement_category,
        tender_additional_procurement_categories,
        tender_procuring_entity_id,
        tender_start_date,
        tender_end_date,
        tender_documents,
        tender_item_id,
        tender_item_description,
        tender_item_classification_scheme,
        tender_item_classification_id,
        tender_item_classification_description,
        tender_item_additional_scheme,
        tender_item_additional_id,
        tender_item_additional_description,
        modified_date
    )
    VALUES (
        ?,?,?,?,?,?,
        ?,?,?,?,?,?,
        ?,?,?,?,?,?,
        ?,?,?,?,?,?,?, GETDATE()
    )
    """

    for ocid in ocid_list:
        numeroseao = get_numeroseao_from_ocid(ocid)

        # Query old DB's avis_history for this numeroseao (no category filter)
        sql = """
        SELECT
            ah.numeroseao,
            ah.numero,
            ah.organisme,
            ah.municipal,
            ah.adresse1,
            ah.adresse2,
            ah.ville,
            ah.province,
            ah.pays,
            ah.codepostal,
            ah.titre,
            ah.datepublication,
            ah.datefermeture,
            ah.hyperlienseao,
            ah.unspscprincipale,
            ah.disposition,
            ah.categorieseao,
            ah.type,
            ah.nature,
            ah.[precision]
        FROM avis_history ah
        WHERE ah.numeroseao = ?
        """
        src_cursor.execute(sql, (numeroseao,))
        rows = src_cursor.fetchall()

        for row in rows:
            release_id = safe_str(row.numero)
            date_val = format_date(row.datepublication)

            typed_type = 0
            typed_precision = 0
            typed_nature = 0
            try:
                typed_type = int(row.type)
            except:
                pass
            try:
                typed_precision = int(row.precision)
            except:
                pass
            try:
                typed_nature = int(row.nature)
            except:
                pass

            tender_proc_method = map_tender_procurement_method(typed_type)
            tender_proc_method_details = map_tender_procurement_method_details(typed_type)
            tender_main_cat = map_main_procurement_category(typed_precision)
            tender_addl_cat = map_additional_procurement_categories(typed_nature)
            tender_addl_cat_str = ",".join(tender_addl_cat)
            item_id = get_first_something(safe_str(row.categorieseao))

            vals = (
                ocid,
                release_id,
                date_val,
                "avis_history",
                "tender_history",
                "fr",

                release_id,                  # tender_id
                safe_str(row.titre),         # tender_title
                "complete",                  # tender_status
                tender_proc_method,
                tender_proc_method_details,
                tender_main_cat,
                tender_addl_cat_str,
                "OP-" + numeroseao,
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
            tgt_cursor.execute(insert_sql, vals)
            count_inserted += 1

    logging.info(f"→ transform_avis_history: inserted {count_inserted} rows into releases_history.")


# ---------------------------------------------------------------------------
# 2) transform_fournisseurs_history -> parties_history + (bids_history)
# ---------------------------------------------------------------------------
def transform_fournisseurs_history(src_cursor, tgt_cursor, ocid_list):
    """
    For each ocid in new DB, parse numeroseao, then find old 'avis_fournisseurs' rows,
    then for each NEQ, read 'fournisseurs_history', insert into parties_history.
    Also insert into bids_history if your new DB has it, ignoring category filters.
    """
    logging.info("→ transform_fournisseurs_history & transform_bids_history: start.")
    count_parties_inserted = 0
    count_bids_inserted = 0

    insert_parties_sql = """
    INSERT INTO parties_history (
        party_id,
        name,
        street_address,
        locality,
        region,
        postal_code,
        country_name,
        details,
        modified_date
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, GETDATE())
    """

    insert_bids_sql = """
    INSERT INTO bids_history (
        party_id,
        ocid,
        related_lot,
        admissible,
        conform,
        value,
        value_unit,
        modified_date
    )
    VALUES (?, ?, NULL, ?, ?, ?, ?, GETDATE())
    """

    for ocid in ocid_list:
        numeroseao = get_numeroseao_from_ocid(ocid)

        # Grab old 'avis_fournisseurs' for this numeroseao
        sql_af = """
        SELECT
            af.adjudicataire,
            af.admissible,
            af.conforme,
            af.montantsoumis,
            af.montantssoumisunite,
            f.neq
        FROM avis_fournisseurs af
        JOIN fournisseurs f ON af.neq = f.neq
        WHERE af.numeroseao = ?
        """
        src_cursor.execute(sql_af, (numeroseao,))
        rows_af = src_cursor.fetchall()

        for row_af in rows_af:
            party_id = "FO-" + safe_str(row_af.neq) if row_af.neq else "FO-MISSING"

            # 1) Insert parties_history from old 'fournisseurs_history' if available
            sql_fh = """
            SELECT
                fh.fournisseurs_history_id,
                fh.neq,
                fh.nomorganisation,
                fh.adresse1,
                fh.adresse2,
                fh.ville,
                fh.province,
                fh.pays,
                fh.codepostal
            FROM fournisseurs_history fh
            WHERE fh.neq = ?
            """
            src_cursor.execute(sql_fh, (row_af.neq,))
            rows_fh = src_cursor.fetchall()

            for row_fh in rows_fh:
                name = safe_str(row_fh.nomorganisation)
                street = safe_str(row_fh.adresse1) + " " + safe_str(row_fh.adresse2)
                locality = safe_str(row_fh.ville)
                region = safe_str(row_fh.province)
                postal = safe_str(row_fh.codepostal)
                country = safe_str(row_fh.pays)
                details_json = '{"NEQ": "' + safe_str(row_fh.neq) + '"}'

                tgt_cursor.execute(insert_parties_sql, (
                    party_id,
                    name,
                    street,
                    locality,
                    region,
                    postal,
                    country,
                    details_json
                ))
                count_parties_inserted += 1

            # 2) Insert bids_history (if you have that table) ignoring category filters
            #    because we already matched ocid in new DB.
            admissible = 1 if row_af.admissible else 0
            conform = 1 if row_af.conforme else 0
            value = float(row_af.montantsoumis or 0.0)
            value_unit = str(row_af.montantssoumisunite) if row_af.montantssoumisunite else "CAD"

            tgt_cursor.execute(insert_bids_sql, (
                party_id,
                ocid,
                admissible,
                conform,
                value,
                value_unit
            ))
            count_bids_inserted += 1

    logging.info(f"→ transform_fournisseurs_history: inserted {count_parties_inserted} rows into parties_history.")
    logging.info(f"→ transform_bids_history: inserted {count_bids_inserted} rows into bids_history.")


# ---------------------------------------------------------------------------
# 3) transform_contrats_history -> contracts_history
#    with the SWAP logic, no category filter
# ---------------------------------------------------------------------------
def transform_contrats_history(src_cursor, tgt_cursor, ocid_list):
    logging.info("→ transform_contrats_history: start.")
    count_inserted = 0

    insert_sql = """
    INSERT INTO contracts_history (
        contract_id,
        ocid,
        status,
        period_end_date,
        value_amount,
        date_signed,
        modified_date
    )
    VALUES (?, ?, ?, ?, ?, ?, GETDATE())
    """

    for ocid in ocid_list:
        numeroseao = get_numeroseao_from_ocid(ocid)

        sql_ch = """
        SELECT
            ch.contrats_history_id,
            ch.numeroseao,
            ch.numero,
            ch.datefinale,
            ch.datepublicationfinale,
            ch.montantfinal
        FROM contrats_history ch
        WHERE ch.numeroseao = ?
        """
        src_cursor.execute(sql_ch, (numeroseao,))
        rows_ch = src_cursor.fetchall()

        for row in rows_ch:
            contract_id = safe_str(row.numero)
            period_end_date = format_date(row.datepublicationfinale)  # SWAP
            date_signed = format_date(row.datefinale if row.datefinale else row.datepublicationfinale)
            status = "active" if not row.datepublicationfinale else "terminated"
            amount = float(row.montantfinal or 0.0)

            vals = (
                contract_id,
                ocid,
                status,
                period_end_date,
                amount,
                date_signed
            )
            tgt_cursor.execute(insert_sql, vals)
            count_inserted += 1

    logging.info(f"→ transform_contrats_history: inserted {count_inserted} rows into contracts_history.")


# ---------------------------------------------------------------------------
# 4) transform_depenses_history -> contract_transactions_history
#    no category filter
# ---------------------------------------------------------------------------
def transform_depenses_history(src_cursor, tgt_cursor, ocid_list):
    logging.info("→ transform_depenses_history: start.")
    count_inserted = 0

    insert_sql = """
    INSERT INTO contract_transactions_history (
        ocid,
        transaction_id,
        contract_id,
        source,
        date,
        value_amount,
        value_currency,
        modified_date
    )
    VALUES (?, ?, NULL, ?, ?, ?, 'CAD', GETDATE())
    """

    for ocid in ocid_list:
        numeroseao = get_numeroseao_from_ocid(ocid)

        sql_dh = """
        SELECT
            dh.depense_hist_id,
            dh.numeroseao,
            dh.datedepense,
            dh.montantdepense,
            dh.description
        FROM depenses_history dh
        WHERE dh.numeroseao = ?
        """
        src_cursor.execute(sql_dh, (numeroseao,))
        rows_dh = src_cursor.fetchall()

        for row in rows_dh:
            txn_id = "txn-" + safe_str(row.depense_hist_id)
            txn_date = format_date(row.datedepense)
            amount = float(row.montantdepense or 0.0)
            desc = safe_str(row.description)

            vals = (
                ocid,
                txn_id,
                desc,
                txn_date,
                amount
            )
            tgt_cursor.execute(insert_sql, vals)
            count_inserted += 1

    logging.info(f"→ transform_depenses_history: inserted {count_inserted} rows into contract_transactions_history.")


# ---------------------------------------------------------------------------
# MAIN MIGRATION FUNCTION
# ---------------------------------------------------------------------------
def migrate_history_data():
    """
    1) Connect to old DB (XMLtest) reading from *history tables* only.
    2) Connect to new DB (jsontest) inserting into *history tables* only:
       - releases_history
       - parties_history (plus bids_history if you want)
       - contracts_history
       - contract_transactions_history
    3) We'll gather all ocids from the new DB's main 'releases' table.
    4) For each ocid, parse old DB's *history* (no category filter this time),
       then insert into new DB's *history tables* with the same SWAP logic, etc.
    5) No main table upserts; only direct inserts into *history tables*.
    """
    try:
        # Source DB with history tables
        src_conn = pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=DESKTOP-91AK8MU\\SQLEXPRESS;"
            "DATABASE=XMLfinal;"
            "Trusted_Connection=yes;"
        )
        src_cursor = src_conn.cursor()

        # Target DB with main + history tables
        tgt_conn = pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=DESKTOP-91AK8MU\\SQLEXPRESS;"
            "DATABASE=constructionDB;"
            "Trusted_Connection=yes;"
        )
        tgt_cursor = tgt_conn.cursor()

        logging.info("🔨 Ensuring tables (and history tables) exist in target DB...")
        create_tables(tgt_cursor)
        tgt_conn.commit()

        # 1) Gather all ocids from new DB's main 'releases' table
        sql_ocids = "SELECT ocid FROM releases"
        tgt_cursor.execute(sql_ocids)
        ocid_rows = tgt_cursor.fetchall()
        ocid_list = [r.ocid for r in ocid_rows]
        logging.info(f"Found {len(ocid_list)} ocids in new DB's releases (main table).")

        # 2) For each ocid, parse old DB's *history*, insert into new DB's *history*
        logging.info("🔍 transform_avis_history (no category filter) ...")
        transform_avis_history(src_cursor, tgt_cursor, ocid_list)
        tgt_conn.commit()

        logging.info("🔍 transform_fournisseurs_history (and bids_history) ...")
        transform_fournisseurs_history(src_cursor, tgt_cursor, ocid_list)
        tgt_conn.commit()

        logging.info("🔍 transform_contrats_history (SWAP logic, no category filter) ...")
        transform_contrats_history(src_cursor, tgt_cursor, ocid_list)
        tgt_conn.commit()

        logging.info("🔍 transform_depenses_history (no category filter) ...")
        transform_depenses_history(src_cursor, tgt_cursor, ocid_list)
        tgt_conn.commit()

        logging.info("✅ History migration completed successfully.")
        print("✅ History migration completed successfully.")

    except Exception as e:
        logging.error(f"❌ History migration failed: {str(e)}")
        print(f"❌ History migration failed: {str(e)}")
        tgt_conn.rollback()
    finally:
        src_cursor.close()
        src_conn.close()
        tgt_cursor.close()
        tgt_conn.close()
        logging.info("🔌 All connections closed.")
        print("🔌 All connections closed.")


if __name__ == "__main__":
    migrate_history_data()
