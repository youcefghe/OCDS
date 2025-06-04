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
        'C02 - Ouvrages de g�nie civil',
        'G31 - �quipement de transport et pi�ces de rechange',
        'S8 - Contr�le de la qualit�, essais et inspections et services de repr�sentants techniques',
        'S5 - Services environnementaux',
        'G19 - Machinerie et outils',
        'S19 - Location � bail ou location d''installations immobili�res',
        'G25 - Constructions pr�fabriqu�es',
        'G6 - Mat�riaux de construction',
        'C01 - B�timents',
        'IMM1 - Vente de biens immeubles',
        'G25 - Constructions pr�fabriqu�s',
        'C03 - Autres travaux de construction',
        'S3 - Services d''architecture et d''ing�nierie'
    )
