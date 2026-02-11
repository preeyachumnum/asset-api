/*
SP: dbo.spSapAsset_SyncToAssets
Purpose:
- Sync active rows from dbo.SapAsset_Current into dbo.Assets
- Auto-create required master data for FK mapping (Company/Plant/CostCenter/Location)
- Keep app layer SQL-free (service calls SP only)
*/
CREATE OR ALTER PROCEDURE [dbo].[spSapAsset_SyncToAssets]
  @DeactivateMissing BIT = 0
AS
BEGIN
  SET NOCOUNT ON;
  SET XACT_ABORT ON;

  DECLARE @utc DATETIME2(0) = SYSUTCDATETIME();
  DECLARE @defaultAssetGroupId UNIQUEIDENTIFIER;
  DECLARE @defaultAssetStatusId UNIQUEIDENTIFIER;
  DECLARE @deactivated INT = 0;

  BEGIN TRAN;

  /* Ensure default AssetGroup for SAP imported assets */
  SELECT @defaultAssetGroupId = ag.AssetGroupId
  FROM dbo.AssetGroups ag
  WHERE ag.AssetGroupCode = N'SAP_IMPORTED';

  IF @defaultAssetGroupId IS NULL
  BEGIN
    INSERT INTO dbo.AssetGroups (AssetGroupCode, AssetGroupName)
    VALUES (N'SAP_IMPORTED', N'SAP Imported');

    SELECT @defaultAssetGroupId = ag.AssetGroupId
    FROM dbo.AssetGroups ag
    WHERE ag.AssetGroupCode = N'SAP_IMPORTED';
  END

  /* Ensure default AssetStatus used by imported assets */
  SELECT @defaultAssetStatusId = st.AssetStatusId
  FROM dbo.AssetStatuses st
  WHERE st.StatusCode = N'ACTIVE';

  IF @defaultAssetStatusId IS NULL
  BEGIN
    INSERT INTO dbo.AssetStatuses (StatusCode, StatusName, SortOrder, IsActive)
    VALUES (N'ACTIVE', N'Active', 10, 1);

    SELECT @defaultAssetStatusId = st.AssetStatusId
    FROM dbo.AssetStatuses st
    WHERE st.StatusCode = N'ACTIVE';
  END

  ;WITH sap_raw AS (
    SELECT
      LTRIM(RTRIM(s.CoCd)) AS CoCd,
      LEFT(LTRIM(RTRIM(s.AssetNo)), 50) AS AssetNo,
      NULLIF(LTRIM(RTRIM(s.AssetDescription)), N'') AS AssetDescription,
      TRY_CONVERT(DECIMAL(18, 2), s.BookValue) AS BookValue,
      s.CapDate,
      NULLIF(LTRIM(RTRIM(s.PlantCode)), N'') AS PlantCode,
      NULLIF(LTRIM(RTRIM(s.CostCtrCode)), N'') AS CostCtrCode,
      NULLIF(LTRIM(RTRIM(s.CostCtrName)), N'') AS CostCtrName,
      s.LastSeenAt
    FROM dbo.SapAsset_Current s
    WHERE s.IsActive = 1
      AND NULLIF(LTRIM(RTRIM(s.CoCd)), N'') IS NOT NULL
      AND NULLIF(LTRIM(RTRIM(s.AssetNo)), N'') IS NOT NULL
  ),
  sap_dedup AS (
    SELECT *
    FROM (
      SELECT
        r.*,
        ROW_NUMBER() OVER (
          PARTITION BY r.AssetNo
          ORDER BY r.LastSeenAt DESC, r.CoCd ASC
        ) AS rn
      FROM sap_raw r
    ) d
    WHERE d.rn = 1
  )
  INSERT INTO dbo.Companies (CompanyCode, CompanyName)
  SELECT d.CoCd, d.CoCd
  FROM (SELECT DISTINCT CoCd FROM sap_dedup) d
  LEFT JOIN dbo.Companies c ON c.CompanyCode = d.CoCd
  WHERE c.CompanyId IS NULL;

  ;WITH sap_dedup AS (
    SELECT *
    FROM (
      SELECT
        LTRIM(RTRIM(s.CoCd)) AS CoCd,
        LEFT(LTRIM(RTRIM(s.AssetNo)), 50) AS AssetNo,
        NULLIF(LTRIM(RTRIM(s.PlantCode)), N'') AS PlantCode,
        s.LastSeenAt,
        ROW_NUMBER() OVER (
          PARTITION BY LEFT(LTRIM(RTRIM(s.AssetNo)), 50)
          ORDER BY s.LastSeenAt DESC, LTRIM(RTRIM(s.CoCd)) ASC
        ) AS rn
      FROM dbo.SapAsset_Current s
      WHERE s.IsActive = 1
        AND NULLIF(LTRIM(RTRIM(s.CoCd)), N'') IS NOT NULL
        AND NULLIF(LTRIM(RTRIM(s.AssetNo)), N'') IS NOT NULL
    ) d
    WHERE d.rn = 1
  )
  INSERT INTO dbo.Plants (CompanyId, PlantCode, PlantName)
  SELECT DISTINCT
    c.CompanyId,
    COALESCE(d.PlantCode, N'UNK') AS PlantCode,
    COALESCE(d.PlantCode, N'UNK') AS PlantName
  FROM sap_dedup d
  JOIN dbo.Companies c ON c.CompanyCode = d.CoCd
  LEFT JOIN dbo.Plants p
    ON p.CompanyId = c.CompanyId
   AND p.PlantCode = COALESCE(d.PlantCode, N'UNK')
  WHERE p.PlantId IS NULL;

  ;WITH sap_dedup AS (
    SELECT *
    FROM (
      SELECT
        LTRIM(RTRIM(s.CoCd)) AS CoCd,
        LEFT(LTRIM(RTRIM(s.AssetNo)), 50) AS AssetNo,
        NULLIF(LTRIM(RTRIM(s.PlantCode)), N'') AS PlantCode,
        NULLIF(LTRIM(RTRIM(s.CostCtrCode)), N'') AS CostCtrCode,
        NULLIF(LTRIM(RTRIM(s.CostCtrName)), N'') AS CostCtrName,
        s.LastSeenAt,
        ROW_NUMBER() OVER (
          PARTITION BY LEFT(LTRIM(RTRIM(s.AssetNo)), 50)
          ORDER BY s.LastSeenAt DESC, LTRIM(RTRIM(s.CoCd)) ASC
        ) AS rn
      FROM dbo.SapAsset_Current s
      WHERE s.IsActive = 1
        AND NULLIF(LTRIM(RTRIM(s.CoCd)), N'') IS NOT NULL
        AND NULLIF(LTRIM(RTRIM(s.AssetNo)), N'') IS NOT NULL
    ) d
    WHERE d.rn = 1
  )
  INSERT INTO dbo.CostCenters (CompanyId, PlantId, CostCenterCode, CostCenterName)
  SELECT DISTINCT
    c.CompanyId,
    p.PlantId,
    COALESCE(d.CostCtrCode, N'UNK') AS CostCenterCode,
    COALESCE(d.CostCtrName, COALESCE(d.CostCtrCode, N'UNK')) AS CostCenterName
  FROM sap_dedup d
  JOIN dbo.Companies c ON c.CompanyCode = d.CoCd
  JOIN dbo.Plants p
    ON p.CompanyId = c.CompanyId
   AND p.PlantCode = COALESCE(d.PlantCode, N'UNK')
  LEFT JOIN dbo.CostCenters cc
    ON cc.CompanyId = c.CompanyId
   AND cc.PlantId = p.PlantId
   AND cc.CostCenterCode = COALESCE(d.CostCtrCode, N'UNK')
  WHERE cc.CostCenterId IS NULL;

  ;WITH company_plant AS (
    SELECT DISTINCT
      c.CompanyId,
      p.PlantId
    FROM dbo.SapAsset_Current s
    JOIN dbo.Companies c
      ON c.CompanyCode = LTRIM(RTRIM(s.CoCd))
    JOIN dbo.Plants p
      ON p.CompanyId = c.CompanyId
     AND p.PlantCode = COALESCE(NULLIF(LTRIM(RTRIM(s.PlantCode)), N''), N'UNK')
    WHERE s.IsActive = 1
      AND NULLIF(LTRIM(RTRIM(s.CoCd)), N'') IS NOT NULL
      AND NULLIF(LTRIM(RTRIM(s.AssetNo)), N'') IS NOT NULL
  )
  INSERT INTO dbo.Locations (CompanyId, PlantId, LocationCode, LocationName)
  SELECT cp.CompanyId, cp.PlantId, N'AUTO', N'Auto from SAP'
  FROM company_plant cp
  LEFT JOIN dbo.Locations l
    ON l.CompanyId = cp.CompanyId
   AND l.PlantId = cp.PlantId
   AND l.LocationCode = N'AUTO'
  WHERE l.LocationId IS NULL;

  DECLARE @mergeActions TABLE (ActionName NVARCHAR(10) NOT NULL);

  ;WITH sap_raw AS (
    SELECT
      LTRIM(RTRIM(s.CoCd)) AS CoCd,
      LEFT(LTRIM(RTRIM(s.AssetNo)), 50) AS AssetNo,
      NULLIF(LTRIM(RTRIM(s.AssetDescription)), N'') AS AssetDescription,
      TRY_CONVERT(DECIMAL(18, 2), s.BookValue) AS BookValue,
      s.CapDate,
      NULLIF(LTRIM(RTRIM(s.PlantCode)), N'') AS PlantCode,
      NULLIF(LTRIM(RTRIM(s.CostCtrCode)), N'') AS CostCtrCode,
      s.LastSeenAt
    FROM dbo.SapAsset_Current s
    WHERE s.IsActive = 1
      AND NULLIF(LTRIM(RTRIM(s.CoCd)), N'') IS NOT NULL
      AND NULLIF(LTRIM(RTRIM(s.AssetNo)), N'') IS NOT NULL
  ),
  sap_dedup AS (
    SELECT *
    FROM (
      SELECT
        r.*,
        ROW_NUMBER() OVER (
          PARTITION BY r.AssetNo
          ORDER BY r.LastSeenAt DESC, r.CoCd ASC
        ) AS rn
      FROM sap_raw r
    ) d
    WHERE d.rn = 1
  ),
  src_mapped AS (
    SELECT
      d.AssetNo,
      LEFT(COALESCE(d.AssetDescription, d.AssetNo), 200) AS AssetName,
      COALESCE(d.BookValue, CONVERT(DECIMAL(18, 2), 0)) AS BookValue,
      d.CapDate AS ReceiveDate,
      c.CompanyId,
      p.PlantId,
      cc.CostCenterId,
      l.LocationId
    FROM sap_dedup d
    JOIN dbo.Companies c
      ON c.CompanyCode = d.CoCd
    JOIN dbo.Plants p
      ON p.CompanyId = c.CompanyId
     AND p.PlantCode = COALESCE(d.PlantCode, N'UNK')
    JOIN dbo.CostCenters cc
      ON cc.CompanyId = c.CompanyId
     AND cc.PlantId = p.PlantId
     AND cc.CostCenterCode = COALESCE(d.CostCtrCode, N'UNK')
    JOIN dbo.Locations l
      ON l.CompanyId = c.CompanyId
     AND l.PlantId = p.PlantId
     AND l.LocationCode = N'AUTO'
  )
  MERGE dbo.Assets AS T
  USING src_mapped AS S
     ON T.AssetNo = S.AssetNo
  WHEN MATCHED THEN
    UPDATE SET
      T.CompanyId = S.CompanyId,
      T.PlantId = S.PlantId,
      T.CostCenterId = S.CostCenterId,
      T.LocationId = S.LocationId,
      T.AssetGroupId = @defaultAssetGroupId,
      T.AssetStatusId = @defaultAssetStatusId,
      T.AssetName = S.AssetName,
      T.BookValue = S.BookValue,
      T.ReceiveDate = S.ReceiveDate,
      T.IsActive = 1,
      T.UpdatedAt = @utc
  WHEN NOT MATCHED THEN
    INSERT (
      CompanyId,
      PlantId,
      CostCenterId,
      LocationId,
      AssetGroupId,
      AssetStatusId,
      AssetNo,
      AssetName,
      BookValue,
      ReceiveDate,
      IsActive
    )
    VALUES (
      S.CompanyId,
      S.PlantId,
      S.CostCenterId,
      S.LocationId,
      @defaultAssetGroupId,
      @defaultAssetStatusId,
      S.AssetNo,
      S.AssetName,
      S.BookValue,
      S.ReceiveDate,
      1
    )
  OUTPUT $action INTO @mergeActions(ActionName);

  IF @DeactivateMissing = 1
  BEGIN
    UPDATE a
      SET a.IsActive = 0,
          a.UpdatedAt = @utc
    FROM dbo.Assets a
    LEFT JOIN dbo.SapAsset_Current s
      ON s.AssetNo = a.AssetNo
     AND s.IsActive = 1
    WHERE a.IsActive = 1
      AND s.AssetNo IS NULL;

    SET @deactivated = @@ROWCOUNT;
  END

  COMMIT;

  SELECT
    (SELECT COUNT(*) FROM dbo.SapAsset_Current s
      WHERE s.IsActive = 1
        AND NULLIF(LTRIM(RTRIM(s.CoCd)), N'') IS NOT NULL
        AND NULLIF(LTRIM(RTRIM(s.AssetNo)), N'') IS NOT NULL) AS SourceActiveRows,
    (SELECT COUNT(*) FROM @mergeActions WHERE ActionName = N'INSERT') AS InsertedAssets,
    (SELECT COUNT(*) FROM @mergeActions WHERE ActionName = N'UPDATE') AS UpdatedAssets,
    @deactivated AS DeactivatedAssets,
    (SELECT COUNT(*) FROM dbo.Assets WHERE IsActive = 1) AS ActiveAssetsTotal;
END
GO
