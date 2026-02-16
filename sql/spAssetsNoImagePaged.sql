/*
SP: dbo.spAssetsNoImagePaged
Purpose:
- Return active assets without image by page from DB (server-side pagination)
- Optional search by AssetNo/AssetName
*/
CREATE OR ALTER PROCEDURE [dbo].[spAssetsNoImagePaged]
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

  ;WITH base_data AS (
    SELECT
      a.AssetId,
      a.CompanyId,
      a.PlantId,
      a.CostCenterId,
      a.LocationId,
      a.AssetGroupId,
      a.AssetStatusId,
      a.AssetNo,
      a.AssetName,
      a.BookValue,
      a.ReceiveDate,
      a.QrValue,
      a.QrTypeCode,
      a.IsActive,
      a.CreatedAt,
      a.UpdatedAt,
      c.CompanyCode,
      c.CompanyName,
      p.PlantCode,
      p.PlantName,
      cc.CostCenterCode,
      cc.CostCenterName,
      l.LocationCode,
      l.LocationName,
      st.StatusCode,
      st.StatusName,
      CAST(0 AS BIT) AS HasImage,
      ROW_NUMBER() OVER (ORDER BY a.AssetNo ASC, a.AssetId ASC) AS RowNum
    FROM dbo.Assets a
    LEFT JOIN dbo.Companies c ON c.CompanyId = a.CompanyId
    LEFT JOIN dbo.Plants p ON p.PlantId = a.PlantId
    LEFT JOIN dbo.CostCenters cc ON cc.CostCenterId = a.CostCenterId
    LEFT JOIN dbo.Locations l ON l.LocationId = a.LocationId
    LEFT JOIN dbo.AssetStatuses st ON st.AssetStatusId = a.AssetStatusId
    LEFT JOIN dbo.AssetImages i
      ON i.AssetId = a.AssetId
     AND i.IsActive = 1
    WHERE a.IsActive = 1
      AND i.AssetImageId IS NULL
      AND (
        @Search IS NULL
        OR a.AssetNo LIKE '%' + @Search + '%'
        OR a.AssetName LIKE '%' + @Search + '%'
      )
  )
  SELECT
    AssetId,
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
    QrValue,
    QrTypeCode,
    IsActive,
    CreatedAt,
    UpdatedAt,
    CompanyCode,
    CompanyName,
    PlantCode,
    PlantName,
    CostCenterCode,
    CostCenterName,
    LocationCode,
    LocationName,
    StatusCode,
    StatusName,
    HasImage
  FROM base_data
  WHERE RowNum BETWEEN ((@Page - 1) * @PageSize) + 1 AND (@Page * @PageSize)
  ORDER BY RowNum;

  ;WITH count_data AS (
    SELECT a.AssetId
    FROM dbo.Assets a
    LEFT JOIN dbo.AssetImages i
      ON i.AssetId = a.AssetId
     AND i.IsActive = 1
    WHERE a.IsActive = 1
      AND i.AssetImageId IS NULL
      AND (
        @Search IS NULL
        OR a.AssetNo LIKE '%' + @Search + '%'
        OR a.AssetName LIKE '%' + @Search + '%'
      )
  )
  SELECT COUNT(1) AS TotalRows
  FROM count_data;
END
GO
