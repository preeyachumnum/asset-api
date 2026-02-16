/*
SP: dbo.spAssetImageAdd
Purpose:
- Insert a new active image row for an asset
- Set primary image safely (first image auto-primary, or force primary by input)
*/
CREATE OR ALTER PROCEDURE [dbo].[spAssetImageAdd]
  @AssetId UNIQUEIDENTIFIER,
  @FileUrl NVARCHAR(1000),
  @IsPrimary BIT = 0
AS
BEGIN
  SET NOCOUNT ON;
  SET XACT_ABORT ON;

  IF @AssetId IS NULL
    THROW 50001, 'AssetId is required', 1;

  SET @FileUrl = NULLIF(LTRIM(RTRIM(@FileUrl)), '');
  IF @FileUrl IS NULL
    THROW 50002, 'FileUrl is required', 1;

  IF NOT EXISTS (SELECT 1 FROM dbo.Assets WHERE AssetId = @AssetId AND IsActive = 1)
    THROW 50003, 'Asset not found or inactive', 1;

  BEGIN TRAN;

  DECLARE @hasActiveImage BIT = CASE
    WHEN EXISTS (SELECT 1 FROM dbo.AssetImages WHERE AssetId = @AssetId AND IsActive = 1) THEN 1
    ELSE 0
  END;

  DECLARE @finalPrimary BIT = CASE
    WHEN @IsPrimary = 1 THEN 1
    WHEN @hasActiveImage = 0 THEN 1
    ELSE 0
  END;

  IF @finalPrimary = 1
  BEGIN
    UPDATE dbo.AssetImages
    SET IsPrimary = 0
    WHERE AssetId = @AssetId
      AND IsActive = 1
      AND IsPrimary = 1;
  END

  DECLARE @nextSort INT = ISNULL((
    SELECT MAX(SortOrder)
    FROM dbo.AssetImages
    WHERE AssetId = @AssetId
      AND IsActive = 1
  ), 0) + 1;

  INSERT INTO dbo.AssetImages (
    AssetId,
    FileUrl,
    IsPrimary,
    SortOrder,
    IsActive
  )
  VALUES (
    @AssetId,
    @FileUrl,
    @finalPrimary,
    @nextSort,
    1
  );

  DECLARE @newId UNIQUEIDENTIFIER = (
    SELECT TOP 1 AssetImageId
    FROM dbo.AssetImages
    WHERE AssetId = @AssetId
      AND FileUrl = @FileUrl
    ORDER BY UploadedAt DESC
  );

  COMMIT;

  SELECT
    AssetImageId,
    AssetId,
    FileUrl,
    IsPrimary,
    SortOrder,
    UploadedAt,
    IsActive
  FROM dbo.AssetImages
  WHERE AssetImageId = @newId;
END
GO
