--avg contract duration by type 
SELECT 
    r.tender_procurement_method_details AS ContractType,
    AVG(DATEDIFF(day, c.date_signed, c.period_end_date)) AS AverageDurationDays
FROM [ConstructionDB].[dbo].[contracts] c
JOIN [ConstructionDB].[dbo].[releases] r
    ON c.ocid = r.ocid
WHERE c.date_signed IS NOT NULL 
  AND c.period_end_date IS NOT NULL
GROUP BY r.tender_procurement_method_details
ORDER BY ContractType;

--avg contract duration by client  (sup and buyer)
SELECT 
    p.party_id,
    p.name AS SupplierName,
    COUNT(DISTINCT c.contract_id) AS NumberOfContracts,
    AVG(DATEDIFF(day, c.date_signed, c.period_end_date)) AS AverageDurationDays
FROM [ConstructionDB].[dbo].[contracts] c
JOIN [ConstructionDB].[dbo].[releases] r
    ON c.ocid = r.ocid
JOIN [ConstructionDB].[dbo].[release_parties] rp
    ON r.ocid = rp.ocid
JOIN [ConstructionDB].[dbo].[parties] p
    ON rp.party_id = p.party_id
WHERE rp.role = 'supplier'
  AND c.date_signed IS NOT NULL 
  AND c.period_end_date IS NOT NULL
GROUP BY p.party_id, p.name
ORDER BY NumberOfContracts DESC;



