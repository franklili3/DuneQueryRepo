-- 方法1：尝试使用十六进制地址
SELECT
    DATE_TRUNC('day', evt_block_time) AS dt,
    'USDT' as asset,
    SUM(
        CASE
            WHEN "from" = {{address_hex}} THEN value -- OR "from" = 0x0000000000000000000000000000000000000000
            WHEN "to" = {{address_hex}} THEN -value --OR "to" = 0x0000000000000000000000000000000000000000 
            ELSE 0
        END
    ) / 1e6 AS net_amount
FROM erc20_tron.evt_transfer
WHERE contract_address = {{contract_address}}  -- USDT在TRON上的合约地址
  AND ("from" = {{address_hex}}  -- 替换为你的TRON地址的十六进制形式
       OR "to" = {{address_hex}})  -- 替换为你的TRON地址的十六进制形式
GROUP BY 1, 2
ORDER BY 1 DESC
LIMIT 100;