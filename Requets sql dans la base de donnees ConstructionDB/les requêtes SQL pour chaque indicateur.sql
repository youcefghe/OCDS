--Durée moyenne des contrats
SELECT 
    AVG(DATEDIFF(day, date_signed, period_end_date)) AS AvgContractDurationDays
FROM ConstructionDB.dbo.contracts
WHERE date_signed IS NOT NULL AND period_end_date IS NOT NULL;

--Durée moyenne des appels d’offres (tenders)
SELECT 
    AVG(DATEDIFF(day, tender_start_date, tender_end_date)) AS AvgTenderDurationDays
FROM ConstructionDB.dbo.releases
WHERE tender_start_date IS NOT NULL AND tender_end_date IS NOT NULL;

--Croissance moyenne des coûts (award vs contrat)
SELECT 
    AVG(CAST(c.value_amount - a.value_amount AS FLOAT)) AS AvgCostGrowth
FROM ConstructionDB.dbo.contracts c
JOIN ConstructionDB.dbo.awards a ON c.award_id = a.award_id
WHERE c.value_amount IS NOT NULL AND a.value_amount IS NOT NULL;

--Nombre moyen de soumissionnaires par appel d’offres
SELECT 
    AVG(BidCount) AS AvgBidders
FROM (
    SELECT 
        ocid,
        COUNT(DISTINCT party_id) AS BidCount
    FROM ConstructionDB.dbo.bids
    GROUP BY ocid
) AS BidStats;

-- nombre de bids par contrat
SELECT 
    b.ocid,
    COUNT(*) AS NumberOfBids
FROM ConstructionDB.dbo.bids b
GROUP BY b.ocid
ORDER BY NumberOfBids;


--Top 5 fournisseurs par montant total de contrats
SELECT TOP 10 
    p.name AS SupplierName,
    SUM(c.value_amount) AS TotalContractValue,
    COUNT(*) AS NumberOfContracts
FROM ConstructionDB.dbo.contracts c
JOIN ConstructionDB.dbo.release_parties rp ON c.ocid = rp.ocid AND rp.role = 'supplier'
JOIN ConstructionDB.dbo.parties p ON rp.party_id = p.party_id
GROUP BY p.name
ORDER BY TotalContractValue DESC;








