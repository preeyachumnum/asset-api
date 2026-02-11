/*
SP: dbo.spAssetsSapMismatch
Purpose: ดึงรายการทรัพย์สินที่ข้อมูลใน eAsset ไม่ตรงกับ SAP current
*/
CREATE OR ALTER PROCEDURE [dbo].[spAssetsSapMismatch]
  @TopRows INT = 1000,
  @Search NVARCHAR(100) = NULL
AS
BEGIN
  SET NOCOUNT ON;

  IF @TopRows IS NULL OR @TopRows < 1 SET @TopRows = 1000;
  IF @TopRows > 20000 SET @TopRows = 20000;
  SET @Search = NULLIF(LTRIM(RTRIM(@Search)), '');

  ;WITH ea AS (
    SELECT
      a.AssetId,
      a.AssetNo,
      a.AssetName,
      CAST(a.BookValue AS DECIMAL(19,4)) AS BookValue,
      c.CompanyCode,
      p.PlantCode,
      cc.CostCenterCode
    FROM dbo.Assets a
    LEFT JOIN dbo.Companies c ON c.CompanyId = a.CompanyId
    LEFT JOIN dbo.Plants p ON p.PlantId = a.PlantId
    LEFT JOIN dbo.CostCenters cc ON cc.CostCenterId = a.CostCenterId
    WHERE a.IsActive = 1
  ),
  sap AS (
    SELECT
      s.CoCd,
      s.AssetNo,
      s.AssetDescription,
      CAST(s.BookValue AS DECIMAL(19,4)) AS BookValue,
      s.PlantCode,
      s.CostCtrCode,
      s.LastSeenAt
    FROM dbo.SapAsset_Current s
    WHERE s.IsActive = 1
  ),
  joined_data AS (
    SELECT
      COALESCE(ea.AssetNo, sap.AssetNo) AS AssetNo,
      ea.AssetId,
      ea.AssetName,
      sap.AssetDescription AS SapAssetName,
      ea.BookValue AS AssetBookValue,
      sap.BookValue AS SapBookValue,
      ea.CompanyCode AS AssetCompanyCode,
      sap.CoCd AS SapCompanyCode,
      ea.PlantCode AS AssetPlantCode,
      sap.PlantCode AS SapPlantCode,
      ea.CostCenterCode AS AssetCostCenterCode,
      sap.CostCtrCode AS SapCostCenterCode,
      sap.LastSeenAt AS SapLastSeenAt,
      CASE
        WHEN ea.AssetNo IS NULL THEN 'MISSING_IN_EASSET'
        WHEN sap.AssetNo IS NULL THEN 'MISSING_IN_SAP'
        WHEN ISNULL(ea.PlantCode, '') <> ISNULL(sap.PlantCode, '') THEN 'PLANT_MISMATCH'
        WHEN ISNULL(ea.CostCenterCode, '') <> ISNULL(sap.CostCtrCode, '') THEN 'COSTCENTER_MISMATCH'
        WHEN ABS(ISNULL(ea.BookValue, 0) - ISNULL(sap.BookValue, 0)) > 0.01 THEN 'BOOKVALUE_MISMATCH'
        WHEN ISNULL(NULLIF(LTRIM(RTRIM(ea.AssetName)), ''), '') <> ISNULL(NULLIF(LTRIM(RTRIM(sap.AssetDescription)), ''), '') THEN 'ASSETNAME_MISMATCH'
        ELSE NULL
      END AS MismatchType
    FROM ea
    FULL OUTER JOIN sap
      ON sap.AssetNo = ea.AssetNo
      AND (ea.CompanyCode = sap.CoCd OR ea.CompanyCode IS NULL OR sap.CoCd IS NULL)
  )
  SELECT TOP (@TopRows)
    AssetNo,
    AssetId,
    AssetName,
    SapAssetName,
    AssetBookValue,
    SapBookValue,
    AssetCompanyCode,
    SapCompanyCode,
    AssetPlantCode,
    SapPlantCode,
    AssetCostCenterCode,
    SapCostCenterCode,
    SapLastSeenAt,
    MismatchType
  FROM joined_data
  WHERE MismatchType IS NOT NULL
    AND (
      @Search IS NULL
      OR AssetNo LIKE '%' + @Search + '%'
      OR AssetName LIKE '%' + @Search + '%'
      OR SapAssetName LIKE '%' + @Search + '%'
      OR AssetCostCenterCode LIKE '%' + @Search + '%'
      OR SapCostCenterCode LIKE '%' + @Search + '%'
      OR AssetPlantCode LIKE '%' + @Search + '%'
      OR SapPlantCode LIKE '%' + @Search + '%'
    )
  ORDER BY AssetNo;
END
GO
