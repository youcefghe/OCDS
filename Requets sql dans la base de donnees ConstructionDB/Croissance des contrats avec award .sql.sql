-- croissane des contrats (doit + bg) avec "award" comme montant initial du contrat (pour les acheteurs)
WITH InitialAwards AS (
    SELECT 
        a.ocid,
        a.value_amount AS InitialAward,
        rp.party_id AS BuyerID,
        ROW_NUMBER() OVER (PARTITION BY a.ocid ORDER BY a.award_id ASC) AS rn
    FROM [ConstructionDB].[dbo].[awards] a
    INNER JOIN [ConstructionDB].[dbo].[release_parties] rp 
         ON a.ocid = rp.ocid AND rp.role = 'buyer'
),
FirstAwards AS (
    SELECT ocid, InitialAward, BuyerID
    FROM InitialAwards
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
    DISTINCT fc.ocid,
    fa.BuyerID,
    p.name AS BuyerName,
    fa.InitialAward,
    e.AdditionalExpenses,
    fa.InitialAward + e.AdditionalExpenses AS ExpectedFinalContract,
    fc.FinalContract,
    fc.FinalContract - fa.InitialAward AS Growth
FROM FinalContractValues fc
LEFT JOIN FirstAwards fa ON fc.ocid = fa.ocid
LEFT JOIN Expenses e ON fc.ocid = e.ocid
LEFT JOIN [ConstructionDB].[dbo].[parties] p ON fa.BuyerID = p.party_id
--WHERE p.name = 'SQI'
and AdditionalExpenses is not null
ORDER BY fa.InitialAward DESC;
