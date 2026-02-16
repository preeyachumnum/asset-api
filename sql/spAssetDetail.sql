/*
SP: dbo.spAssetDetail
Purpose:
- Return one asset detail with readable master labels (code/name)
- Return active image list for the asset
*/
CREATE OR ALTER PROCEDURE [dbo].[spAssetDetail]
  @AssetId UNIQUEIDENTIFIER
AS
BEGIN
  SET NOCOUNT ON;

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
    st.StatusName
  FROM dbo.Assets a
  LEFT JOIN dbo.Companies c ON c.CompanyId = a.CompanyId
  LEFT JOIN dbo.Plants p ON p.PlantId = a.PlantId
  LEFT JOIN dbo.CostCenters cc ON cc.CostCenterId = a.CostCenterId
  LEFT JOIN dbo.Locations l ON l.LocationId = a.LocationId
  LEFT JOIN dbo.AssetStatuses st ON st.AssetStatusId = a.AssetStatusId
  WHERE a.AssetId = @AssetId;

  SELECT
    i.AssetImageId,
    i.AssetId,
    i.FileUrl,
    i.IsPrimary,
    i.SortOrder,
    i.UploadedAt,
    i.IsActive
  FROM dbo.AssetImages i
  WHERE i.AssetId = @AssetId
    AND i.IsActive = 1
  ORDER BY i.IsPrimary DESC, i.SortOrder ASC, i.UploadedAt DESC;
END
GO
