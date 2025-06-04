--croissane des ontrats (doit + bg)
WITH InitialBids AS (
    SELECT 
        b.ocid,
        b.value AS InitialBid,
        rp.party_id AS SupplierID,
        ROW_NUMBER() OVER (PARTITION BY b.ocid ORDER BY b.bid_row_id ASC) AS rn
    FROM [ConstructionDB].[dbo].[bids] b
    INNER JOIN [ConstructionDB].[dbo].[release_parties] rp 
         ON b.ocid = rp.ocid AND b.party_id = rp.party_id
    WHERE rp.role = 'supplier'
),
FirstBids AS (
    SELECT ocid, InitialBid, SupplierID
    FROM InitialBids
    WHERE rn = 1
),
Expenses AS (
    SELECT 
        ocid, 
        ISNULL(SUM(value_amount), 0) AS AdditionalExpenses
    FROM [ConstructionDB].[dbo].[contract_transactions]
    GROUP BY ocid
),
FinalContractValues AS (
    SELECT 
        ocid, 
        value_amount AS FinalContract
    FROM [ConstructionDB].[dbo].[contracts]
)
SELECT 
    distinct fc.ocid,
    fb.SupplierID,
    p.name AS SupplierName,
    fb.InitialBid,
    e.AdditionalExpenses,
    fb.InitialBid + e.AdditionalExpenses AS ExpectedFinalContract,
    fc.FinalContract
FROM FinalContractValues fc
LEFT JOIN FirstBids fb ON fc.ocid = fb.ocid
LEFT JOIN Expenses e ON fc.ocid = e.ocid
LEFT JOIN [ConstructionDB].[dbo].[parties] p ON fb.SupplierID = p.party_id
ORDER BY fc.ocid;
