-- 方法1：使用十六进制地址查询USDT交易
-- 使用Dune API v1参数语法
-- 注意: 使用from_hex()函数将十六进制字符串转换为varbinary进行比较
SELECT
    DATE_TRUNC('day', evt_block_time) AS dt,
    'USDT' as asset,
    SUM(
      CASE 
        WHEN "to" = from_hex('{{address_hex}}') THEN value
        WHEN "from" = from_hex('{{address_hex}}') THEN -value
      END
      / 1e6) AS net_inflow
FROM tether_tron.Tether_USD_evt_Transfer
WHERE contract_address = 0xa614f803b6fd780986a42c78ec9c7f77e6ded13c  -- USDT在TRON上的合约地址
  AND ("to" = from_hex('{{address_hex}}')
    OR "from" = from_hex('{{address_hex}}'))
GROUP BY 1, 2
ORDER BY 1 DESC
LIMIT 10;