import logging
import xml.etree.ElementTree as ET
import unicodedata
import re
from datetime import datetime

##############################################################################
# 1) Helper functions
##############################################################################

def to_date(date_str):
    """
    Converts a date string like "YYYY-MM-DD" or "YYYY-MM-DD HH:MM[:SS]" into a
    SQL DATETIME literal: 'YYYY-MM-DD HH:MM:SS'.
    Returns "NULL" if empty or if parsing fails.
    """
    if not date_str or not date_str.strip():
        return "NULL"
    
    raw = date_str.strip()
    # Try multiple formats
    formats = ["%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"]
    for fmt in formats:
        try:
            dt = datetime.strptime(raw, fmt)
            return f"'{dt.strftime('%Y-%m-%d %H:%M:%S')}'"
        except ValueError:
            pass
    return "NULL"

def parse_bit(node):
    """
    Returns 1 if node text is '1', 0 if '0', or 'NULL' otherwise.
    """
    if node is None or not node.text:
        return 'NULL'
    txt = node.text.strip()
    if txt == '1':
        return 1
    if txt == '0':
        return 0
    return 'NULL'

def escape_single_quotes(value):
    """
    Safely escapes any single quotes/apostrophes for T-SQL insertion:
      - Remove control chars
      - Replace curly quotes
      - Double any ASCII apostrophes
      - Wrap in N'...' or return "NULL" if empty
    """
    if not value:
        return "NULL"

    # Remove CR/LF
    txt = str(value).replace("\r", "").replace("\n", " ")
    # Remove ASCII control chars (below 0x20)
    txt = re.sub(r"[\x00-\x1F]+", " ", txt)

    # Replace curly single quotes
    txt = txt.replace("’", "'").replace("‘", "'")
    txt = txt.replace("ʼ", "'").replace("‛", "'")
    # Replace curly double quotes
    txt = txt.replace("“", '"').replace("”", '"')

    # Normalize
    txt = unicodedata.normalize("NFKC", txt)

    # Escape apostrophes for T-SQL
    txt = txt.replace("'", "''")

    return f"N'{txt}'"

def safe_text(parent, tag):
    """
    Safely returns parent.find(tag).text.strip() if it exists, else ''.
    This avoids 'NoneType has no attribute text'.
    """
    node = parent.find(tag)
    if node is not None and node.text is not None:
        return node.text.strip()
    return ''

##############################################################################
# 2) Fournisseurs (upsert)
##############################################################################

def insert_or_update_fournisseur(cursor, fournisseur_data, source_file):
    """
    If NEQ is present, we use it to find/update. Otherwise fallback on name as
    unique key. Moves old row to history if found. Otherwise inserts new.
    """
    sf_str = escape_single_quotes(source_file)

    raw_neq  = (fournisseur_data.get('neq') or '').strip()
    name_raw = (fournisseur_data.get('nomorganisation') or '').strip()

    adr1_str = escape_single_quotes(fournisseur_data.get('adresse1',''))
    adr2_str = escape_single_quotes(fournisseur_data.get('adresse2',''))
    ville_str = escape_single_quotes(fournisseur_data.get('ville',''))
    province  = (fournisseur_data.get('province','')).replace("'", "''")
    pays      = (fournisseur_data.get('pays','')).replace("'", "''")
    codep     = (fournisseur_data.get('codepostal','')).replace("'", "''")

    name_str = escape_single_quotes(name_raw)
    neq_str  = escape_single_quotes(raw_neq)

    if raw_neq:
        # Look up by NEQ
        sql_check = f"""
        SELECT fourn_id, neq, nomorganisation
        FROM fournisseurs
        WHERE neq = {neq_str}
        """
        cursor.execute(sql_check)
        row = cursor.fetchone()

        if row:
            fourn_id = row[0]
            old_neq  = row[1]
            old_name = row[2]

            # Move old row to history
            sql_move = f"""
            INSERT INTO fournisseurs_history (
                fourn_id, neq, nomorganisation,
                adresse1, adresse2, ville, province, pays, codepostal,
                existing_neq, source_file, imported_at
            )
            SELECT
                fourn_id, neq, nomorganisation,
                adresse1, adresse2, ville, province, pays, codepostal,
                existing_neq, source_file, imported_at
            FROM fournisseurs
            WHERE fourn_id = {fourn_id};
            """
            cursor.execute(sql_move)

            existing_neq_val = ''
            if old_name and old_name != name_raw:
                existing_neq_val = old_neq

            sql_update = f"""
            UPDATE fournisseurs
            SET
                nomorganisation = {name_str},
                adresse1        = {adr1_str},
                adresse2        = {adr2_str},
                ville           = {ville_str},
                province        = '{province}',
                pays            = '{pays}',
                codepostal      = '{codep}',
                existing_neq    = {escape_single_quotes(existing_neq_val)},
                source_file     = {sf_str},
                imported_at     = GETDATE()
            WHERE fourn_id = {fourn_id};
            """
            cursor.execute(sql_update)

        else:
            # Insert new
            sql_insert = f"""
            INSERT INTO fournisseurs (
                neq, nomorganisation,
                adresse1, adresse2, ville, province, pays, codepostal,
                existing_neq, source_file
            )
            VALUES (
                {neq_str}, {name_str},
                {adr1_str}, {adr2_str}, {ville_str}, '{province}', '{pays}', '{codep}',
                NULL,
                {sf_str}
            );
            """
            cursor.execute(sql_insert)

    else:
        # Fallback on name if NEQ not present
        if not name_raw:
            return  # Nothing to insert

        sql_check = f"""
        SELECT fourn_id, neq, nomorganisation
        FROM fournisseurs
        WHERE neq IS NULL
          AND nomorganisation = {name_str}
        """
        cursor.execute(sql_check)
        row = cursor.fetchone()

        if row:
            fourn_id = row[0]
            # Move old row to history
            sql_move = f"""
            INSERT INTO fournisseurs_history (
                fourn_id, neq, nomorganisation,
                adresse1, adresse2, ville, province, pays, codepostal,
                existing_neq, source_file, imported_at
            )
            SELECT
                fourn_id, neq, nomorganisation,
                adresse1, adresse2, ville, province, pays, codepostal,
                existing_neq, source_file, imported_at
            FROM fournisseurs
            WHERE fourn_id = {fourn_id};
            """
            cursor.execute(sql_move)

            sql_update = f"""
            UPDATE fournisseurs
            SET
                adresse1    = {adr1_str},
                adresse2    = {adr2_str},
                ville       = {ville_str},
                province    = '{province}',
                pays        = '{pays}',
                codepostal  = '{codep}',
                source_file = {sf_str},
                imported_at = GETDATE()
            WHERE fourn_id = {fourn_id};
            """
            cursor.execute(sql_update)

        else:
            # Insert new row w/o NEQ
            sql_insert = f"""
            INSERT INTO fournisseurs (
                neq, nomorganisation,
                adresse1, adresse2, ville, province, pays, codepostal,
                existing_neq, source_file
            )
            VALUES (
                NULL, {name_str},
                {adr1_str}, {adr2_str}, {ville_str}, '{province}', '{pays}', '{codep}',
                NULL,
                {sf_str}
            );
            """
            cursor.execute(sql_insert)

##############################################################################
# 3) Avis_Fournisseurs deletion and insertion strategy
##############################################################################

def delete_avis_fournisseurs(cursor, numeroseao):
    """
    Deletes all avis_fournisseurs records for the given numeroseao.
    """
    numeroseao_str = escape_single_quotes(numeroseao)
    sql_delete = f"DELETE FROM avis_fournisseurs WHERE numeroseao = {numeroseao_str};"
    cursor.execute(sql_delete)

def insert_avis_fournisseur(cursor, link_data, source_file):
    """
    Inserts a new avis_fournisseurs record.
    """
    numeroseao = link_data.get('numeroseao', '')
    numero     = link_data.get('numero', '')
    neq        = link_data.get('neq', '')
    nomorg     = link_data.get('nomorganisation', '')

    # Escape all text fields
    numeroseao_str = escape_single_quotes(numeroseao)
    numero_str     = escape_single_quotes(numero)
    nomorg_str     = escape_single_quotes(nomorg)
    sf_str         = escape_single_quotes(source_file)

    insert_neq = f"'{neq}'" if neq else "NULL"

    adm_val       = link_data.get('admissible', 'NULL')
    conf_val      = link_data.get('conforme', 'NULL')
    adj_val       = link_data.get('adjudicataire', 'NULL')
    mont_soumis   = link_data.get('montantsoumis', 'NULL')
    mont_soumis_u = link_data.get('montantssoumisunite', 'NULL')
    mont_contrat  = link_data.get('montantcontrat', 'NULL')
    mont_total    = link_data.get('montanttotalcontrat', 'NULL')

    sql_insert = f"""
    INSERT INTO avis_fournisseurs (
        numeroseao, numero, neq, nomorganisation,
        admissible, conforme, adjudicataire,
        montantsoumis, montantssoumisunite,
        montantcontrat, montanttotalcontrat,
        source_file
    )
    VALUES (
        {numeroseao_str},
        {numero_str},
        {insert_neq},
        {nomorg_str},
        {adm_val},
        {conf_val},
        {adj_val},
        {mont_soumis},
        {mont_soumis_u},
        {mont_contrat},
        {mont_total},
        {sf_str}
    );
    """
    cursor.execute(sql_insert)

##############################################################################
# 4) Avis (upsert)
##############################################################################

def insert_or_update_avis(cursor, avis_data, source_file):
    numeroseao = avis_data.get('numeroseao','').strip()
    if not numeroseao:
        return

    sql_check = f"""
    SELECT numeroseao
    FROM avis
    WHERE numeroseao = '{numeroseao}'
    """
    cursor.execute(sql_check)
    row = cursor.fetchone()

    sf_str   = escape_single_quotes(source_file)
    org_str  = escape_single_quotes(avis_data.get('organisme',''))
    ad1_str  = escape_single_quotes(avis_data.get('adresse1',''))
    ad2_str  = escape_single_quotes(avis_data.get('adresse2',''))
    ville_str= escape_single_quotes(avis_data.get('ville',''))
    province = escape_single_quotes(avis_data.get('province',''))
    pays     = escape_single_quotes(avis_data.get('pays',''))
    codep    = escape_single_quotes(avis_data.get('codepostal',''))
    titre    = escape_single_quotes(avis_data.get('titre',''))

    type_raw   = avis_data.get('type','').strip()
    type_str   = escape_single_quotes(type_raw) if type_raw else "NULL"

    nature_raw = avis_data.get('nature','').strip()
    nature_str = escape_single_quotes(nature_raw) if nature_raw else "NULL"

    prec_raw   = avis_data.get('precision','').strip()
    prec_str   = escape_single_quotes(prec_raw) if prec_raw else "NULL"

    categorieseao = escape_single_quotes(avis_data.get('categorieseao',''))
    datepublication     = to_date(avis_data.get('datepublication',''))
    datefermeture       = to_date(avis_data.get('datefermeture',''))
    datesaisieouverture = to_date(avis_data.get('datesaisieouverture',''))
    datesaisieadjud     = to_date(avis_data.get('datesaisieadjudication',''))
    dateadjudication    = to_date(avis_data.get('dateadjudication',''))
    regionlivraison     = escape_single_quotes(avis_data.get('regionlivraison',''))
    unspscprincipale    = escape_single_quotes(avis_data.get('unspscprincipale',''))
    disposition         = escape_single_quotes(avis_data.get('disposition',''))
    hyperlienseao       = escape_single_quotes(avis_data.get('hyperlienseao',''))

    municipal_val = avis_data.get('municipal','NULL')
    if municipal_val.upper() != 'NULL' and not municipal_val.isdigit():
        municipal_val = 'NULL'

    numero_str = escape_single_quotes(avis_data.get('numero',''))

    if row:
        #
        # 1) Move old record to avis_history
        #
        sql_move = f"""
        INSERT INTO avis_history (
            numeroseao, numero, organisme, municipal,
            adresse1, adresse2, ville, province, pays, codepostal,
            titre, [type], [nature], [precision], categorieseao,
            datepublication, datefermeture, datesaisieouverture,
            datesaisieadjudication, dateadjudication,
            regionlivraison, unspscprincipale, disposition,
            hyperlienseao, source_file, imported_at
        )
        SELECT
            numeroseao, numero, organisme, municipal,
            adresse1, adresse2, ville, province, pays, codepostal,
            titre, [type], [nature], [precision], categorieseao,
            datepublication, datefermeture, datesaisieouverture,
            datesaisieadjudication, dateadjudication,
            regionlivraison, unspscprincipale, disposition,
            hyperlienseao, source_file, imported_at
        FROM avis
        WHERE numeroseao = '{numeroseao}';
        """
        cursor.execute(sql_move)

        #
        # 2) Update the main avis table with new data
        #
        sql_update = f"""
        UPDATE avis
        SET
            numero = {numero_str},
            organisme = {org_str},
            municipal = {municipal_val},
            adresse1 = {ad1_str},
            adresse2 = {ad2_str},
            ville = {ville_str},
            province = {province},
            pays = {pays},
            codepostal = {codep},
            titre = {titre},
            [type] = {type_str},
            [nature] = {nature_str},
            [precision] = {prec_str},
            categorieseao = {categorieseao},
            datepublication = {datepublication},
            datefermeture = {datefermeture},
            datesaisieouverture = {datesaisieouverture},
            datesaisieadjudication = {datesaisieadjud},
            dateadjudication = {dateadjudication},
            regionlivraison = {regionlivraison},
            unspscprincipale = {unspscprincipale},
            disposition = {disposition},
            hyperlienseao = {hyperlienseao},
            source_file = {sf_str},
            imported_at = GETDATE()
        WHERE numeroseao = '{numeroseao}';
        """
        cursor.execute(sql_update)
    else:
        # Insert
        sql_insert = f"""
        INSERT INTO avis (
            numeroseao, numero, organisme, municipal,
            adresse1, adresse2, ville, province, pays, codepostal,
            titre, [type], [nature], [precision], categorieseao,
            datepublication, datefermeture, datesaisieouverture,
            datesaisieadjudication, dateadjudication,
            regionlivraison, unspscprincipale, disposition,
            hyperlienseao, source_file
        )
        VALUES (
            '{numeroseao}', {numero_str}, {org_str}, {municipal_val},
            {ad1_str}, {ad2_str}, {ville_str}, {province}, {pays}, {codep},
            {titre}, {type_str}, {nature_str}, {prec_str}, {categorieseao},
            {datepublication}, {datefermeture}, {datesaisieouverture},
            {datesaisieadjud}, {dateadjudication},
            {regionlivraison}, {unspscprincipale}, {disposition},
            {hyperlienseao}, {sf_str}
        );
        """
        cursor.execute(sql_insert)

def process_avis_file(cursor, file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        xml_content = f.read()
    # Escape raw '&'
    xml_content = xml_content.replace("&", "&amp;")
    tree = ET.ElementTree(ET.fromstring(xml_content))
    root = tree.getroot()

    avis_nodes = root.findall('avis')
    if not avis_nodes and root.tag == 'avis':
        avis_nodes = [root]

    for a_node in avis_nodes:
        avis_data = {
            'numeroseao':           safe_text(a_node, 'numeroseao'),
            'numero':               safe_text(a_node, 'numero'),
            'organisme':            safe_text(a_node, 'organisme'),
            'municipal':            safe_text(a_node, 'municipal'),
            'adresse1':             safe_text(a_node, 'adresse1'),
            'adresse2':             safe_text(a_node, 'adresse2'),
            'ville':                safe_text(a_node, 'ville'),
            'province':             safe_text(a_node, 'province'),
            'pays':                 safe_text(a_node, 'pays'),
            'codepostal':           safe_text(a_node, 'codepostal'),
            'titre':                safe_text(a_node, 'titre'),
            'type':                 safe_text(a_node, 'type'),
            'nature':               safe_text(a_node, 'nature'),
            'precision':            safe_text(a_node, 'precision'),
            'categorieseao':        safe_text(a_node, 'categorieseao'),
            'datepublication':      safe_text(a_node, 'datepublication'),
            'datefermeture':        safe_text(a_node, 'datefermeture'),
            'datesaisieouverture':  safe_text(a_node, 'datesaisieouverture'),
            'datesaisieadjudication': safe_text(a_node, 'datesaisieadjudication'),
            'dateadjudication':     safe_text(a_node, 'dateadjudication'),
            'regionlivraison':      safe_text(a_node, 'regionlivraison'),
            'unspscprincipale':     safe_text(a_node, 'unspscprincipale'),
            'disposition':          safe_text(a_node, 'disposition'),
            'hyperlienseao':        safe_text(a_node, 'hyperlienseao')
        }
        # Upsert the avis record
        insert_or_update_avis(cursor, avis_data, file_path)
        # Delete existing avis_fournisseurs for this avis
        delete_avis_fournisseurs(cursor, avis_data['numeroseao'])
        
        # Process fournisseurs: re-insert all current ones for this avis
        fournisseur_parent = a_node.find('fournisseurs')
        if fournisseur_parent is not None:
            for f_elem in fournisseur_parent.findall('fournisseur'):
                fournisseur_data = {
                    'neq':              safe_text(f_elem, 'neq'),
                    'nomorganisation':  safe_text(f_elem, 'nomorganisation'),
                    'adresse1':         safe_text(f_elem, 'adresse1'),
                    'adresse2':         safe_text(f_elem, 'adresse2'),
                    'ville':            safe_text(f_elem, 'ville'),
                    'province':         safe_text(f_elem, 'province'),
                    'pays':             safe_text(f_elem, 'pays'),
                    'codepostal':       safe_text(f_elem, 'codepostal')
                }
                # Upsert fournisseurs table (if needed)
                insert_or_update_fournisseur(cursor, fournisseur_data, file_path)

                link_data = {
                    'numeroseao':      avis_data['numeroseao'],
                    'numero':          avis_data['numero'],
                    'neq':             fournisseur_data['neq'],
                    'nomorganisation': fournisseur_data['nomorganisation'],
                    'admissible':      parse_bit(f_elem.find('admissible')),
                    'conforme':        parse_bit(f_elem.find('conforme')),
                    'adjudicataire':   parse_bit(f_elem.find('adjudicataire')),
                    'montantsoumis':       safe_text(f_elem, 'montantsoumis')       or 'NULL',
                    'montantssoumisunite': safe_text(f_elem, 'montantssoumisunite') or 'NULL',
                    'montantcontrat':      safe_text(f_elem, 'montantcontrat')      or 'NULL',
                    'montanttotalcontrat': safe_text(f_elem, 'montanttotalcontrat') or 'NULL'
                }
                # Insert the new avis_fournisseurs record(s)
                insert_avis_fournisseur(cursor, link_data, file_path)

##############################################################################
# 5) Contrats (upsert)
##############################################################################

def insert_or_update_contrats(cursor, contrat_data, source_file):
    sf_str = escape_single_quotes(source_file)

    # Get and trim primary key fields
    raw_numeroseao = contrat_data.get('numeroseao', '').strip()
    raw_numero     = contrat_data.get('numero', '').strip()

    # Check for missing primary key fields
    if not raw_numeroseao or not raw_numero:
        logging.error(f"Cannot process contrat record from {source_file} because primary key field is missing: numeroseao='{raw_numeroseao}', numero='{raw_numero}'")
        return  # Skip processing this record

    numeroseao_str = escape_single_quotes(raw_numeroseao)
    numero_str     = escape_single_quotes(raw_numero)

    datefinale    = to_date(contrat_data.get('datefinale', ''))
    datepubfinale = to_date(contrat_data.get('datepublicationfinale', ''))
    montantfinal  = contrat_data.get('montantfinal', 'NULL')

    # NEQ might contain apostrophes, so we do a simple replace
    raw_neq = contrat_data.get('neqcontractant', '').strip()
    neqcontractant_str = raw_neq.replace("'", "''")

    nomc_str = escape_single_quotes(contrat_data.get('nomcontractant', ''))

    # Check if the record already exists
    sql_check = f"""
    SELECT numeroseao
    FROM contrats
    WHERE numeroseao = {numeroseao_str}
      AND numero = {numero_str}
    """
    cursor.execute(sql_check)
    row = cursor.fetchone()

    if row:
        # Move old row to history
        sql_move = f"""
        INSERT INTO contrats_history (
            numeroseao, numero,
            datefinale, datepublicationfinale,
            montantfinal, nomcontractant,
            neqcontractant, source_file, imported_at
        )
        SELECT
            numeroseao, numero,
            datefinale, datepublicationfinale,
            montantfinal, nomcontractant,
            neqcontractant, source_file, imported_at
        FROM contrats
        WHERE numeroseao = {numeroseao_str}
          AND numero = {numero_str};
        """
        cursor.execute(sql_move)

        # Update existing record
        sql_update = f"""
        UPDATE contrats
        SET
            datefinale = {datefinale},
            datepublicationfinale = {datepubfinale},
            montantfinal = {montantfinal},
            nomcontractant = {nomc_str},
            neqcontractant = '{neqcontractant_str}',
            source_file = {sf_str},
            imported_at = GETDATE()
        WHERE numeroseao = {numeroseao_str}
          AND numero = {numero_str};
        """
        cursor.execute(sql_update)
    else:
        # Insert new record
        sql_insert = f"""
        INSERT INTO contrats (
            numeroseao, numero,
            datefinale, datepublicationfinale,
            montantfinal, nomcontractant,
            neqcontractant, source_file
        )
        VALUES (
            {numeroseao_str}, {numero_str},
            {datefinale}, {datepubfinale},
            {montantfinal}, {nomc_str},
            '{neqcontractant_str}', {sf_str}
        );
        """
        cursor.execute(sql_insert)

def process_contrats_file(cursor, file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        xml_content = f.read()
    xml_content = xml_content.replace("&", "&amp;")
    tree = ET.ElementTree(ET.fromstring(xml_content))
    root = tree.getroot()

    contrat_nodes = root.findall('contrat')
    # Single root?
    if not contrat_nodes and root.tag == 'contrat':
        contrat_nodes = [root]

    for c_node in contrat_nodes:
        data = {
            'numeroseao':           safe_text(c_node, 'numeroseao'),
            'numero':               safe_text(c_node, 'numero'),
            'datefinale':           safe_text(c_node, 'datefinale'),
            'datepublicationfinale':safe_text(c_node, 'datepublicationfinale') or 'NULL',
            'montantfinal':         safe_text(c_node, 'montantfinal') or 'NULL',
            'nomcontractant':       safe_text(c_node, 'nomcontractant'),
            'neqcontractant':       safe_text(c_node, 'neqcontractant')
        }
        insert_or_update_contrats(cursor, data, file_path)

##############################################################################
# 6) Depenses (insert only)
##############################################################################

def insert_depense_and_ignore_history(cursor, depense_data, source_file):
    # Escape these
    sf_str = escape_single_quotes(source_file)

    # numeroseao, numero might contain apostrophes
    raw_numeroseao = depense_data.get('numeroseao','').strip()
    raw_numero     = depense_data.get('numero','').strip()

    numeroseao_str = escape_single_quotes(raw_numeroseao)
    numero_str     = escape_single_quotes(raw_numero)

    desc_str = escape_single_quotes(depense_data.get('description',''))
    nomc_str = escape_single_quotes(depense_data.get('nomcontractant',''))

    datedep  = to_date(depense_data.get('datedepense',''))
    datepub  = to_date(depense_data.get('datepublicationdepense',''))
    montant  = depense_data.get('montantdepense','NULL')

    # NEQ might contain apostrophes
    raw_neq  = depense_data.get('neqcontractant','').strip()
    neq_str  = raw_neq.replace("'", "''")

    sql_insert = f"""
    INSERT INTO depenses (
        numeroseao, numero,
        datedepense, datepublicationdepense,
        montantdepense, description,
        nomcontractant, neqcontractant, source_file
    )
    VALUES (
        {numeroseao_str},
        {numero_str},
        {datedep},
        {datepub},
        {montant},
        {desc_str},
        {nomc_str},
        '{neq_str}',
        {sf_str}
    );
    """
    cursor.execute(sql_insert)

def process_depenses_file(cursor, file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        xml_content = f.read()
    xml_content = xml_content.replace("&", "&amp;")
    tree = ET.ElementTree(ET.fromstring(xml_content))
    root = tree.getroot()

    avis_nodes = root.findall('avis')
    # Single root?
    if not avis_nodes and root.tag == 'avis':
        avis_nodes = [root]

    for a_node in avis_nodes:
        numeroseao = safe_text(a_node, 'numeroseao')
        numero     = safe_text(a_node, 'numero')
        depenses_parent = a_node.find('depenses')
        if depenses_parent is None:
            continue

        for d_node in depenses_parent.findall('depense'):
            data = {
                'numeroseao':             numeroseao,
                'numero':                 numero,
                'datedepense':           safe_text(d_node, 'datedepense'),
                'datepublicationdepense': safe_text(d_node, 'datepublicationdepense'),
                'montantdepense':         safe_text(d_node, 'montantdepense') or 'NULL',
                'description':            safe_text(d_node, 'description'),
                'nomcontractant':         safe_text(d_node, 'nomcontractant'),
                'neqcontractant':         safe_text(d_node, 'neqcontractant')
            }
            insert_depense_and_ignore_history(cursor, data, file_path)
