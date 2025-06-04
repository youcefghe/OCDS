"""
data_insertion.py
Module for inserting data into the SQL database from SEAO-style JSON files.

Changes in this version:
  - We store tender fields (including a single item) directly in 'releases'.
  - We also store exactly one 'additionalClassification' if multiple exist,
    preferring the one whose 'id' is alphanumeric (e.g. "S3").
  - The 'tender_items' table is removed.
  - We have added 'ocid' to 'contract_transactions'.
  - We also added columns in 'releases' for that single additionalClassification.
"""

import json
import logging

def escape_single_quotes(text):
    return text.replace("'", "''") if isinstance(text, str) else text

def parse_date(date_str):

    if not date_str:
        return "NULL"
    
    try:
        from datetime import datetime
        # Handle "Z" timezone (UTC) explicitly
        if date_str.endswith('Z'):
            date_str = date_str.replace('Z', '+00:00')
        
        # Parse ISO 8601 datetime with timezone
        dt = datetime.fromisoformat(date_str)
        
        # Format for SQL Server DATETIME2 (YYYY-MM-DD HH:MM:SS)
        return f"'{dt.strftime('%Y-%m-%d %H:%M:%S')}'"
    except ValueError:
        # Fallback: Try parsing date-only (YYYY-MM-DD)
        try:
            cleaned_date = date_str.strip().split('T')[0]
            year, month, day = cleaned_date.split('-')
            if len(year) != 4 or len(month) != 2 or len(day) != 2:
                return "NULL"
            datetime.strptime(cleaned_date, '%Y-%m-%d')
            return f"'{cleaned_date}'"
        except (ValueError, IndexError):
            return "NULL"

def insert_json_data(cursor, file_path):
    """Reads a JSON file and inserts/updates data in the database."""
    msg = f"  → Loading JSON data from: {file_path}"
    print(msg)
    logging.info(msg)

    with open(file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)

    if 'releases' not in data:
        warn_msg = f"  ⚠ WARNING: No 'releases' key found in {file_path}. Skipping."
        print(warn_msg)
        logging.warning(warn_msg)
        return

    for release in data['releases']:
        ocid = release.get('ocid', '')
        if not ocid:
            continue

        # -----------------------------------------------------
        # 1. Process 'releases' only if valid item exists
        # -----------------------------------------------------
        release_id_val = release.get('id', '')
        date_val       = parse_date(release.get('date', ''))
        tag_val        = escape_single_quotes(",".join(release.get('tag', [])))
        init_val       = escape_single_quotes(release.get('initiationType', ''))
        lang_val       = escape_single_quotes(release.get('language', ''))

        tender_data    = release.get('tender', {})
        tender_id_val  = escape_single_quotes(tender_data.get('id', ''))
        tender_title   = escape_single_quotes(tender_data.get('title', ''))
        tender_status  = escape_single_quotes(tender_data.get('status', ''))
        pm             = escape_single_quotes(tender_data.get('procurementMethod', ''))
        pm_details     = escape_single_quotes(tender_data.get('procurementMethodDetails', ''))
        pm_rationale   = escape_single_quotes(tender_data.get('procurementMethodRationale', ''))
        main_cat       = escape_single_quotes(tender_data.get('mainProcurementCategory', ''))
        addl_cats      = escape_single_quotes(",".join(tender_data.get('additionalProcurementCategories', [])))
        pe_id          = escape_single_quotes(tender_data.get('procuringEntity', {}).get('id', ''))

        tender_period  = tender_data.get('tenderPeriod', {})
        tstart         = parse_date(tender_period.get('startDate', ''))
        tend           = parse_date(tender_period.get('endDate', ''))
        tduration      = tender_period.get('durationInDay', 'NULL') or 'NULL'
        tnum           = tender_data.get('numberOfTenderers', 'NULL') or 'NULL'

        docs           = tender_data.get('documents', [])
        doc_urls       = [d.get('url', '') for d in docs if 'url' in d]
        docs_str       = escape_single_quotes(",".join(doc_urls))

        # ----- Select and validate item -----
        items = tender_data.get('items', [])
        selected_item = None
        allowed_descriptions = {
                'G12 - Moteurs, turbines, composants et accessoires connexes',
                'C02 - Ouvrages de génie civil',
                'G31 - Équipement de transport et pièces de rechange',
                'S8 - Contrôle de la qualité, essais et inspections et services de représentants techniques',
                'S5 - Services environnementaux',
                'G19 - Machinerie et outils',
                'S19 - Location à bail ou location d’installations immobilières',
                'G25 - Constructions préfabriquées',
                'G6 - Matériaux de construction',
                'C01 - Bâtiments',
                'Imm1 - Vente de biens immeubles',
                'G25 - Constructions préfabriqués',
                'S3 - Services d’architecture et d’ingénierie',
                'C03 - Autres travaux de construction'
        }

        # Check ALL items (don't filter out UNSPSC)
        for it in items:
            # FIRST: Check the ITEM'S MAIN DESCRIPTION
            if it.get('description', '') in allowed_descriptions:
                selected_item = it
                break  # Found via main description
            
            # SECOND: Check ADDITIONAL CLASSIFICATIONS (only if main didn't match)
            addcs = it.get('additionalClassifications', [])
            for ac in addcs:
                if ac.get('description', '') in allowed_descriptions:
                    selected_item = it
                    break  # Found via additional classification
            if selected_item:
                break  # Exit item loop if found

        # If no valid item found, skip entire release
        if not selected_item:
            logging.info(f"Skipped release {ocid} - no valid additional descriptions")
            continue

        # Extract fields from validated item
        item_id_val = str(selected_item.get('id', ''))
        desc_val = escape_single_quotes(selected_item.get('description', ''))
        classif = selected_item.get('classification', {})
        c_scheme = escape_single_quotes(classif.get('scheme', ''))
        c_id = escape_single_quotes(classif.get('id', ''))
        c_desc = escape_single_quotes(classif.get('description', ''))

        # Get the FIRST additional classification (regardless of description)
        addc_scheme, addc_id, addc_desc = '', '', ''
        add_classifications = selected_item.get('additionalClassifications', [])
        if add_classifications:
            # Take the FIRST entry, no description check
            first_addc = add_classifications[0]
            addc_scheme = escape_single_quotes(first_addc.get('scheme', ''))
            addc_id = escape_single_quotes(first_addc.get('id', ''))
            addc_desc = escape_single_quotes(first_addc.get('description', ''))

        # Build the SQL upsert for 'releases'
        sql_release = f"""
        IF EXISTS (SELECT 1 FROM releases WHERE ocid = '{escape_single_quotes(ocid)}')
        BEGIN
            UPDATE releases
            SET
                release_id   = '{escape_single_quotes(release_id_val)}',
                date         = {date_val},
                tag          = '{tag_val}',
                initiation_type = '{init_val}',
                language     = '{lang_val}',

                tender_id    = '{tender_id_val}',
                tender_title = '{tender_title}',
                tender_status= '{tender_status}',
                tender_procurement_method = '{pm}',
                tender_procurement_method_details = '{pm_details}',
                tender_procurement_method_rationale = '{pm_rationale}',
                tender_main_procurement_category = '{main_cat}',
                tender_additional_procurement_categories = '{addl_cats}',
                tender_procuring_entity_id = '{pe_id}',
                tender_start_date = {tstart},
                tender_end_date   = {tend},
                tender_duration_in_days = {tduration},
                tender_number_of_tenderers = {tnum},
                tender_documents = '{docs_str}',

                tender_item_id = '{item_id_val}',
                tender_item_description = '{desc_val}',
                tender_item_classification_scheme = '{c_scheme}',
                tender_item_classification_id = '{c_id}',
                tender_item_classification_description = '{c_desc}',

                tender_item_additional_scheme = '{addc_scheme}',
                tender_item_additional_id = '{addc_id}',
                tender_item_additional_description = '{addc_desc}'

            WHERE ocid = '{escape_single_quotes(ocid)}';
        END
        ELSE
        BEGIN
            INSERT INTO releases (
                ocid, release_id, date, tag, initiation_type, language,
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
                tender_item_additional_description
            )
            VALUES (
                '{escape_single_quotes(ocid)}',
                '{escape_single_quotes(release_id_val)}',
                {date_val},
                '{tag_val}',
                '{init_val}',
                '{lang_val}',

                '{tender_id_val}',
                '{tender_title}',
                '{tender_status}',
                '{pm}',
                '{pm_details}',
                '{pm_rationale}',
                '{main_cat}',
                '{addl_cats}',
                '{pe_id}',
                {tstart},
                {tend},
                {tduration},
                {tnum},
                '{docs_str}',

                '{item_id_val}',
                '{desc_val}',
                '{c_scheme}',
                '{c_id}',
                '{c_desc}',

                '{addc_scheme}',
                '{addc_id}',
                '{addc_desc}'
            );
        END;
        """
        cursor.execute(sql_release)

        # -----------------------------------------------------
        # 2. LOTS (to satisfy bids referencing relatedLot)
        # -----------------------------------------------------
        for lot in tender_data.get('lots', []):
            lot_id = str(lot.get('id', ''))
            if not lot_id:
                continue

            lot_title = lot.get('title', '')
            lot_status = lot.get('status', '')
            cp = lot.get('contractPeriod', {})
            start_date = parse_date(cp.get('startDate', ''))
            end_date   = parse_date(cp.get('endDate', ''))

            sql_check_lot = f"""
            SELECT 1 FROM lots
            WHERE lot_id = '{escape_single_quotes(lot_id)}'
            """
            cursor.execute(sql_check_lot)
            existing_lot = cursor.fetchone()

            if existing_lot:
                sql_update_lot = f"""
                UPDATE lots
                SET
                    ocid = '{escape_single_quotes(ocid)}',
                    title = '{escape_single_quotes(lot_title)}',
                    status = '{escape_single_quotes(lot_status)}',
                    contract_period_start_date = {start_date},
                    contract_period_end_date   = {end_date}
                WHERE lot_id = '{escape_single_quotes(lot_id)}';
                """
                cursor.execute(sql_update_lot)
            else:
                sql_insert_lot = f"""
                INSERT INTO lots (
                    lot_id, ocid, title, status,
                    contract_period_start_date, contract_period_end_date
                )
                VALUES (
                    '{escape_single_quotes(lot_id)}',
                    '{escape_single_quotes(ocid)}',
                    '{escape_single_quotes(lot_title)}',
                    '{escape_single_quotes(lot_status)}',
                    {start_date},
                    {end_date}
                );
                """
                cursor.execute(sql_insert_lot)

        # -----------------------------------------------------
        # 3. PARTIES + RELEASE_PARTIES
        # -----------------------------------------------------
        for party in release.get('parties', []):
            party_id = party.get('id', '').strip()
            address  = party.get('address', {})
            name     = party.get('name','').strip()
            street   = address.get('streetAddress','').strip()
            locality = address.get('locality','').strip()
            region   = address.get('region','').strip()
            postal   = address.get('postalCode','').strip()
            country  = address.get('countryName','').strip()
            # Storing "details" as JSON text
            import json as pyjson
            details  = pyjson.dumps(party.get('details',{}))

            new_alias = f"{name}|{street}|{locality}|{region}|{postal}|{country}"

            sql_check_party = f"""
            SELECT name, street_address, locality, region, postal_code, country_name, alias_parties
            FROM parties
            WHERE party_id = '{escape_single_quotes(party_id)}'
            """
            cursor.execute(sql_check_party)
            row_party = cursor.fetchone()

            if not row_party:
                sql_insert_party = f"""
                INSERT INTO parties (
                    party_id, name, street_address, locality, region, postal_code,
                    country_name, details, alias_parties
                )
                VALUES (
                    '{escape_single_quotes(party_id)}',
                    '{escape_single_quotes(name)}',
                    '{escape_single_quotes(street)}',
                    '{escape_single_quotes(locality)}',
                    '{escape_single_quotes(region)}',
                    '{escape_single_quotes(postal)}',
                    '{escape_single_quotes(country)}',
                    '{escape_single_quotes(details)}',
                    NULL
                );
                """
                cursor.execute(sql_insert_party)
            else:
                stored_name, stored_street, stored_loc, stored_reg, stored_post, stored_ctry, stored_alias = row_party
                stored_name  = stored_name  or ""
                stored_street= stored_street or ""
                stored_loc   = stored_loc   or ""
                stored_reg   = stored_reg   or ""
                stored_post  = stored_post  or ""
                stored_ctry  = stored_ctry  or ""
                stored_alias = stored_alias or ""

                stored_core  = f"{stored_name}|{stored_street}|{stored_loc}|{stored_reg}|{stored_post}|{stored_ctry}"

                sql_update_party = f"""
                UPDATE parties
                SET
                    name = '{escape_single_quotes(name)}',
                    street_address = '{escape_single_quotes(street)}',
                    locality       = '{escape_single_quotes(locality)}',
                    region         = '{escape_single_quotes(region)}',
                    postal_code    = '{escape_single_quotes(postal)}',
                    country_name   = '{escape_single_quotes(country)}',
                    details        = '{escape_single_quotes(details)}'
                """
                if new_alias != stored_core:
                    old_aliases = stored_alias.split(',') if stored_alias else []
                    if new_alias not in old_aliases:
                        updated_alias = (stored_alias + ',' + new_alias) if stored_alias else new_alias
                        sql_update_party += f", alias_parties = '{escape_single_quotes(updated_alias)}'"

                sql_update_party += f" WHERE party_id = '{escape_single_quotes(party_id)}';"
                cursor.execute(sql_update_party)

            # release_parties
            for role_val in party.get('roles', []):
                role_val = role_val.strip()
                sql_rp_check = f"""
                SELECT 1 FROM release_parties
                WHERE ocid = '{escape_single_quotes(ocid)}'
                  AND party_id = '{escape_single_quotes(party_id)}'
                  AND role = '{escape_single_quotes(role_val)}'
                """
                cursor.execute(sql_rp_check)
                rp_found = cursor.fetchone()
                if not rp_found:
                    sql_insert_rp = f"""
                    INSERT INTO release_parties (ocid, party_id, role)
                    VALUES (
                        '{escape_single_quotes(ocid)}',
                        '{escape_single_quotes(party_id)}',
                        '{escape_single_quotes(role_val)}'
                    );
                    """
                    cursor.execute(sql_insert_rp)

        # -----------------------------------------------------
        # 4. BIDS
        # -----------------------------------------------------
        for bid in release.get('bids', []):
            bid_party_id = str(bid.get('id',''))
            rel_lots     = bid.get('relatedLots', [])
            # Check if party is in parties
            sql_check_bid_party = f"SELECT 1 FROM parties WHERE party_id = '{escape_single_quotes(bid_party_id)}'"
            cursor.execute(sql_check_bid_party)
            if not cursor.fetchone():
                warn_b = f"⚠️ Missing party: {bid_party_id} in release {ocid}. Skipping bid."
                print(warn_b)
                logging.warning(warn_b)
                continue

            admissible = bid.get('admissible','NULL')
            conform    = bid.get('conform','NULL')
            value_b    = bid.get('value','NULL')
            value_u    = bid.get('valueUnit','')
            if not value_u:
                value_u = 'NULL'
            else:
                value_u = f"'{escape_single_quotes(value_u)}'"

            if not rel_lots:
                # single row, related_lot = NULL
                sql_exists_bid = f"""
                SELECT bid_row_id FROM bids
                WHERE party_id = '{escape_single_quotes(bid_party_id)}'
                  AND ocid = '{escape_single_quotes(ocid)}'
                  AND related_lot IS NULL
                """
                cursor.execute(sql_exists_bid)
                row_bid = cursor.fetchone()
                if row_bid:
                    # update
                    sql_update_bid = f"""
                    UPDATE bids
                    SET
                        admissible = {admissible},
                        conform    = {conform},
                        value      = {value_b},
                        value_unit = {value_u}
                    WHERE bid_row_id = {row_bid[0]};
                    """
                    cursor.execute(sql_update_bid)
                else:
                    # insert
                    sql_insert_bid = f"""
                    INSERT INTO bids (
                        party_id, ocid, related_lot,
                        admissible, conform, value, value_unit
                    )
                    VALUES (
                        '{escape_single_quotes(bid_party_id)}',
                        '{escape_single_quotes(ocid)}',
                        NULL,
                        {admissible},
                        {conform},
                        {value_b},
                        {value_u}
                    );
                    """
                    cursor.execute(sql_insert_bid)
            else:
                for l in rel_lots:
                    rl = escape_single_quotes(str(l))
                    sql_exists_bid_rl = f"""
                    SELECT bid_row_id FROM bids
                    WHERE party_id = '{escape_single_quotes(bid_party_id)}'
                      AND ocid = '{escape_single_quotes(ocid)}'
                      AND related_lot = '{rl}'
                    """
                    cursor.execute(sql_exists_bid_rl)
                    row_bid_rl = cursor.fetchone()
                    if row_bid_rl:
                        # update
                        sql_update_bid_rl = f"""
                        UPDATE bids
                        SET
                            admissible = {admissible},
                            conform = {conform},
                            value = {value_b},
                            value_unit = {value_u}
                        WHERE bid_row_id = {row_bid_rl[0]};
                        """
                        cursor.execute(sql_update_bid_rl)
                    else:
                        # insert
                        sql_insert_bid_rl = f"""
                        INSERT INTO bids (
                            party_id, ocid, related_lot,
                            admissible, conform, value, value_unit
                        )
                        VALUES (
                            '{escape_single_quotes(bid_party_id)}',
                            '{escape_single_quotes(ocid)}',
                            '{rl}',
                            {admissible},
                            {conform},
                            {value_b},
                            {value_u}
                        );
                        """
                        cursor.execute(sql_insert_bid_rl)

        # -----------------------------------------------------
        # 5. AWARDS + SUPPLIERS_AWARDS
        # -----------------------------------------------------
        for award in release.get('awards', []):
            award_id = str(award.get('id',''))
            val_aw   = award.get('value',{})
            a_check  = f"SELECT 1 FROM awards WHERE award_id = '{escape_single_quotes(award_id)}'"
            cursor.execute(a_check)
            award_exists = cursor.fetchone()

            if award_exists:
                sql_update_award = f"""
                UPDATE awards
                SET
                    ocid = '{escape_single_quotes(ocid)}',
                    status = '{escape_single_quotes(award.get('status',''))}',
                    date   = {parse_date(award.get('date',''))},
                    value_amount = {val_aw.get('amount','NULL')},
                    value_currency = '{escape_single_quotes(val_aw.get('currency',''))}',
                    value_total_amount = {val_aw.get('totalAmount','NULL')}
                WHERE award_id = '{escape_single_quotes(award_id)}';
                """
                cursor.execute(sql_update_award)
            else:
                sql_insert_award = f"""
                INSERT INTO awards (
                    award_id, ocid, status, date,
                    value_amount, value_currency, value_total_amount
                )
                VALUES (
                    '{escape_single_quotes(award_id)}',
                    '{escape_single_quotes(ocid)}',
                    '{escape_single_quotes(award.get('status',''))}',
                    {parse_date(award.get('date',''))},
                    {val_aw.get('amount','NULL')},
                    '{escape_single_quotes(val_aw.get('currency',''))}',
                    {val_aw.get('totalAmount','NULL')}
                );
                """
                cursor.execute(sql_insert_award)

            for supplier in award.get('suppliers', []):
                supp_id  = str(supplier.get('id',''))
                # ensure party
                s_check  = f"SELECT party_id FROM parties WHERE party_id = '{escape_single_quotes(supp_id)}'"
                cursor.execute(s_check)
                if not cursor.fetchone():
                    sup_name = escape_single_quotes(supplier.get('name',''))
                    s_insert = f"""
                    INSERT INTO parties (party_id, name)
                    VALUES ('{escape_single_quotes(supp_id)}', '{sup_name}');
                    """
                    cursor.execute(s_insert)

                # link in suppliers_awards
                s_aw_check = f"""
                SELECT 1 FROM suppliers_awards
                WHERE award_id = '{escape_single_quotes(award_id)}'
                  AND supplier_id = '{escape_single_quotes(supp_id)}'
                  AND supplier_ocid = '{escape_single_quotes(ocid)}'
                """
                cursor.execute(s_aw_check)
                if not cursor.fetchone():
                    s_aw_insert = f"""
                    INSERT INTO suppliers_awards (award_id, supplier_id, supplier_ocid)
                    VALUES (
                        '{escape_single_quotes(award_id)}',
                        '{escape_single_quotes(supp_id)}',
                        '{escape_single_quotes(ocid)}'
                    );
                    """
                    cursor.execute(s_aw_insert)

        # -----------------------------------------------------
        # 6. CONTRACTS + AMENDMENTS + TRANSACTIONS
        # -----------------------------------------------------
        for contract in release.get('contracts', []):
            con_id     = str(contract.get('id',''))
            award_id   = str(contract.get('awardID',''))
            period     = contract.get('period',{})
            val_c      = contract.get('value',{})

            # If award missing, insert placeholder
            aw_check = f"SELECT 1 FROM awards WHERE award_id = '{escape_single_quotes(award_id)}'"
            cursor.execute(aw_check)
            if not cursor.fetchone():
                s_insert_pl = f"""
                IF NOT EXISTS (SELECT * FROM awards WHERE award_id = '{escape_single_quotes(award_id)}')
                BEGIN
                    INSERT INTO awards (
                        award_id, ocid, status, date, value_amount,
                        value_currency, value_total_amount
                    )
                    VALUES (
                        '{escape_single_quotes(award_id)}',
                        '{escape_single_quotes(ocid)}',
                        'placeholder',
                        NULL,
                        NULL,
                        NULL,
                        NULL
                    );
                END;
                """
                cursor.execute(s_insert_pl)

            sql_check_contract = f"""
            SELECT 1 FROM contracts WHERE contract_id = '{escape_single_quotes(con_id)}'
            """
            cursor.execute(sql_check_contract)
            contract_exists = cursor.fetchone()

            if contract_exists:
                sql_update_contract = f"""
                UPDATE contracts
                SET
                    ocid = '{escape_single_quotes(ocid)}',
                    award_id = '{escape_single_quotes(award_id)}',
                    status = '{escape_single_quotes(contract.get('status',''))}',
                    period_end_date = {parse_date(period.get('endDate',''))},
                    value_amount = {val_c.get('amount','NULL')},
                    value_currency = '{escape_single_quotes(val_c.get('currency',''))}',
                    date_signed = {parse_date(contract.get('dateSigned',''))}
                WHERE contract_id = '{escape_single_quotes(con_id)}';
                """
                cursor.execute(sql_update_contract)
            else:
                sql_insert_contract = f"""
                INSERT INTO contracts (
                    contract_id, ocid, award_id, status, period_end_date,
                    value_amount, value_currency, date_signed
                )
                VALUES (
                    '{escape_single_quotes(con_id)}',
                    '{escape_single_quotes(ocid)}',
                    '{escape_single_quotes(award_id)}',
                    '{escape_single_quotes(contract.get('status',''))}',
                    {parse_date(period.get('endDate',''))},
                    {val_c.get('amount','NULL')},
                    '{escape_single_quotes(val_c.get('currency',''))}',
                    {parse_date(contract.get('dateSigned',''))}
                );
                """
                cursor.execute(sql_insert_contract)

            # 6a. Contract amendments
            for amendment in contract.get('amendments', []):
                amend_id = str(amendment.get('id',''))
                rationale= amendment.get('rationale','')
                a_date   = parse_date(amendment.get('date',''))
                check_am = f"""
                SELECT 1 FROM contract_amendments
                WHERE amendment_id = '{escape_single_quotes(amend_id)}'
                  AND contract_id  = '{escape_single_quotes(con_id)}'
                """
                cursor.execute(check_am)
                am_exists = cursor.fetchone()
                if am_exists:
                    sql_up_am = f"""
                    UPDATE contract_amendments
                    SET
                        rationale = '{escape_single_quotes(rationale)}',
                        amendment_date = {a_date}
                    WHERE
                        amendment_id = '{escape_single_quotes(amend_id)}'
                        AND contract_id = '{escape_single_quotes(con_id)}';
                    """
                    cursor.execute(sql_up_am)
                else:
                    sql_ins_am = f"""
                    INSERT INTO contract_amendments (
                        amendment_id, contract_id, rationale, amendment_date
                    )
                    VALUES (
                        '{escape_single_quotes(amend_id)}',
                        '{escape_single_quotes(con_id)}',
                        '{escape_single_quotes(rationale)}',
                        {a_date}
                    );
                    """
                    cursor.execute(sql_ins_am)

            # 6b. Contract transactions: PK is now (ocid, transaction_id)
            implementation = contract.get('implementation', {})
            for txn in implementation.get('transactions', []):
                txn_id = str(txn.get('id', ''))
                txn_source = txn.get('source','')
                txn_date   = parse_date(txn.get('date',''))
                txn_val    = txn.get('value', {})
                txn_amt    = txn_val.get('amount','NULL')
                txn_curr   = escape_single_quotes(txn_val.get('currency',''))

                # Check if this transaction (ocid, txn_id) already exists
                check_txn = f"""
                SELECT 1 FROM contract_transactions
                WHERE ocid = '{escape_single_quotes(ocid)}'
                  AND transaction_id = '{escape_single_quotes(txn_id)}'
                """
                cursor.execute(check_txn)
                txn_exists = cursor.fetchone()

                if txn_exists:
                    # We do an UPDATE
                    sql_up_txn = f"""
                    UPDATE contract_transactions
                    SET
                        contract_id     = '{escape_single_quotes(con_id)}',
                        source         = '{escape_single_quotes(txn_source)}',
                        date           = {txn_date},
                        value_amount   = {txn_amt},
                        value_currency = '{txn_curr}'
                    WHERE ocid = '{escape_single_quotes(ocid)}'
                      AND transaction_id = '{escape_single_quotes(txn_id)}';
                    """
                    cursor.execute(sql_up_txn)
                else:
                    # We do an INSERT
                    sql_ins_txn = f"""
                    INSERT INTO contract_transactions (
                        ocid, transaction_id, contract_id, source, date,
                        value_amount, value_currency
                    )
                    VALUES (
                        '{escape_single_quotes(ocid)}',
                        '{escape_single_quotes(txn_id)}',
                        '{escape_single_quotes(con_id)}',
                        '{escape_single_quotes(txn_source)}',
                        {txn_date},
                        {txn_amt},
                        '{txn_curr}'
                    );
                    """
                    cursor.execute(sql_ins_txn)

        # -----------------------------------------------------
        # 7. RELATED_PROCESSES
        # -----------------------------------------------------
        for process in release.get('relatedProcesses', []):
            rp_id = str(process.get('id',''))
            sql_check_proc = f"SELECT 1 FROM related_processes WHERE id = '{escape_single_quotes(rp_id)}'"
            cursor.execute(sql_check_proc)
            if cursor.fetchone():
                sql_update_proc = f"""
                UPDATE related_processes
                SET
                    ocid = '{escape_single_quotes(ocid)}',
                    identifier = '{escape_single_quotes(process.get('identifier',''))}',
                    uri = '{escape_single_quotes(process.get('uri',''))}',
                    relationship = '{escape_single_quotes(",".join(process.get('relationship',[])))}',
                    title = '{escape_single_quotes(process.get('title',''))}',
                    scheme= '{escape_single_quotes(process.get('scheme',''))}'
                WHERE id = '{escape_single_quotes(rp_id)}';
                """
                cursor.execute(sql_update_proc)
            else:
                sql_insert_proc = f"""
                INSERT INTO related_processes (
                    id, ocid, identifier, uri, relationship, title, scheme
                )
                VALUES (
                    '{escape_single_quotes(rp_id)}',
                    '{escape_single_quotes(ocid)}',
                    '{escape_single_quotes(process.get('identifier',''))}',
                    '{escape_single_quotes(process.get('uri',''))}',
                    '{escape_single_quotes(",".join(process.get('relationship',[])))}',
                    '{escape_single_quotes(process.get('title',''))}',
                    '{escape_single_quotes(process.get('scheme',''))}'
                );
                """
                cursor.execute(sql_insert_proc)

    done_msg = f"  → Finished inserting/updating data from: {file_path}"
    print(done_msg)
    logging.info(done_msg)
