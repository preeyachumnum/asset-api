/*
SP: dbo.spAssetsSapMismatchPaged
Purpose:
- Return SAP mismatch rows by page from DB (server-side pagination)
- Optional search by key text fields
*/
CREATE OR ALTER PROCEDURE [dbo].[spAssetsSapMismatchPaged]
  @Page INT = 1,
  @PageSize INT = 50,
  @Search NVARCHAR(100) = NULL
AS
BEGIN
  SET NOCOUNT ON;

  IF @Page IS NULL OR @Page < 1 SET @Page = 1;
  IF @PageSize IS NULL OR @PageSize < 1 SET @PageSize = 50;
  IF @PageSize > 500 SET @PageSize = 500;
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
  ),
  filtered_data AS (
    SELECT
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
      MismatchType,
      ROW_NUMBER() OVER (ORDER BY AssetNo ASC) AS RowNum
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
  )
  SELECT
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
  FROM filtered_data
  WHERE RowNum BETWEEN ((@Page - 1) * @PageSize) + 1 AND (@Page * @PageSize)
  ORDER BY RowNum;

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
      s.CostCtrCode
    FROM dbo.SapAsset_Current s
    WHERE s.IsActive = 1
  ),
  joined_data AS (
    SELECT
      COALESCE(ea.AssetNo, sap.AssetNo) AS AssetNo,
      ea.AssetName,
      sap.AssetDescription AS SapAssetName,
      ea.BookValue AS AssetBookValue,
      sap.BookValue AS SapBookValue,
      ea.PlantCode AS AssetPlantCode,
      sap.PlantCode AS SapPlantCode,
      ea.CostCenterCode AS AssetCostCenterCode,
      sap.CostCtrCode AS SapCostCenterCode,
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
  SELECT COUNT(1) AS TotalRows
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
    );
END
GO
