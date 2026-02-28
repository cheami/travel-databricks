CREATE OR REFRESH MATERIALIZED VIEW silver.transaction (
  CONSTRAINT non_null_date EXPECT (Date IS NOT NULL),
  CONSTRAINT non_null_type EXPECT (Type IS NOT NULL),
  CONSTRAINT non_null_amount EXPECT (Amount IS NOT NULL)
)
AS
SELECT Country, Date, Name, Type, Amount, Comments
FROM bronze.transaction;