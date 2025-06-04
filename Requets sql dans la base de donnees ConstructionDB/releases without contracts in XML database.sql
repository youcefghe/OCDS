SELECT 
    a.numeroseao,
    a.numero,
    a.organisme,
    a.titre,
    c.montantfinal,
    c.nomcontractant,
    CASE 
        WHEN c.numeroseao IS NOT NULL THEN 'Avec Contrat'
        ELSE 'Sans Contrat'
    END AS EtatContrat,
		a.source_file as "avis source file"
FROM [XMLData].[dbo].[avis] a
LEFT JOIN [XMLData].[dbo].[contrats] c
    ON a.numeroseao = c.numeroseao
	where a.organisme like '%SQI%'
	 and a.categorieseao IN (
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
