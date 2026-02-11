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
      a.*,
      ROW_NUMBER() OVER (ORDER BY a.AssetNo ASC, a.AssetId ASC) AS RowNum
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
    UpdatedAt
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
