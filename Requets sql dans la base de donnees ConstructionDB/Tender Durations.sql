--avg tender duration by type
SELECT 
    r.tender_procurement_method_details AS TenderType,
    AVG(DATEDIFF(day, r.tender_start_date, r.tender_end_date)) AS AverageTenderDurationDays
FROM [ConstructionDB].[dbo].[releases] r
WHERE r.tender_start_date IS NOT NULL 
  AND r.tender_end_date IS NOT NULL
GROUP BY r.tender_procurement_method_details
ORDER BY TenderType;

--avg tender duration by client 
SELECT 

    p.name AS ClientName,
    COUNT(DISTINCT r.release_id) AS NumberOfTenders,
    AVG(DATEDIFF(day, r.tender_start_date, r.tender_end_date)) AS AverageTenderDurationDays
FROM [ConstructionDB].[dbo].[releases] r
JOIN [ConstructionDB].[dbo].[release_parties] rp
    ON r.ocid = rp.ocid
    AND rp.role = 'buyer'
JOIN [ConstructionDB].[dbo].[parties] p
    ON rp.party_id = p.party_id
WHERE r.tender_start_date IS NOT NULL 
  AND r.tender_end_date IS NOT NULL
GROUP BY  p.name
ORDER BY  AverageTenderDurationDays DESC;