import os
import csv
import time
import json
import shutil
import requests
from tqdm import tqdm
from dotenv import load_dotenv
from tqdm import tqdm

# Load environment variables
load_dotenv()

# 从环境变量中获取API密钥
API_KEY = os.getenv('DUNE_API_KEY')
if not API_KEY:
    raise ValueError("请设置DUNE_API_KEY环境变量")

# Dune查询ID
QUERY_ID = 5237195

# File paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_CSV = os.path.join(BASE_DIR, 'exchange_usdt_T_address_verified.csv')
OUTPUT_CSV = os.path.join(BASE_DIR, 'exchange_usdt_T_address_verified_updated.csv')

# Field names for the CSV
fieldnames = ['exchange', 'symbol', 'address', 'address_hex', 'is_verified']

def execute_dune_query(address_hex):
    """
    使用Dune API执行查询并返回结果
    """
    # 确保地址格式正确
    # 保存原始地址用于调试
    original_hex = address_hex
    
    # 打印原始地址信息
    print(f"\n=== 原始地址信息 ===")
    print(f"原始地址: {original_hex}")
    print(f"原始地址长度: {len(original_hex)} 字符")
    print(f"原始地址是否以0x开头: {original_hex.lower().startswith('0x')}")
    
    # 确保地址格式正确
    if not original_hex.lower().startswith('0x'):
        print("警告: 地址没有0x前缀，将自动添加")
        address_hex = f"0x{original_hex}"
    else:
        address_hex = original_hex
    
    # 检查地址长度（包括0x）
    if len(address_hex) != 42:  # 0x + 40位十六进制
        print(f"错误: 地址长度不正确，期望42个字符(包括0x)，实际为{len(address_hex)}个字符")
        return False
    
    # 确保地址是有效的十六进制
    hex_part = address_hex[2:]  # 去掉0x
    if not all(c in '0123456789abcdefABCDEF' for c in hex_part):
        print(f"错误: 地址包含无效的十六进制字符: {hex_part}")
        return False
    
    # 打印地址处理信息
    print(f"\n=== 地址处理结果 ===")
    print(f"最终使用的地址: {address_hex}")
    print(f"地址长度: {len(address_hex)} 字符")
    print(f"是否为有效十六进制: {all(c in '0123456789abcdefABCDEFxX' for c in address_hex)}")
    
    # 构建SQL查询，直接嵌入地址
    sql_query = f"""
    -- 使用直接嵌入的地址进行查询
    SELECT
        DATE_TRUNC('day', evt_block_time) AS dt,
        'USDT' as asset,
        SUM(
          CASE 
            WHEN "to" = from_hex('{address_hex[2:].lower()}') THEN value
            WHEN "from" = from_hex('{address_hex[2:].lower()}') THEN -value
          END
          / 1e6) AS net_inflow
    FROM tether_tron.Tether_USD_evt_Transfer
    WHERE contract_address = 0xa614f803b6fd780986a42c78ec9c7f77e6ded13c
      AND ("to" = from_hex('{address_hex[2:].lower()}')
        OR "from" = from_hex('{address_hex[2:].lower()}'))
    GROUP BY 1, 2
    ORDER BY 1 DESC
    LIMIT 10;
    """
    
    # 准备请求体 - 使用原始SQL查询
    params = {
        "query_sql": sql_query,
        "parameters": []
    }
    
    # 使用API v2的原始SQL执行端点
    DUNE_API_BASE_URL = "https://api.dune.com/api/v2"
    
    # 打印SQL查询用于调试
    print("\n" + "="*50)
    print("SQL查询:")
    print(sql_query)
    print("="*50)
    
    try:
        # 使用Dune API v1的参数格式
        params = {
            "parameters": [
                {
                    "key": "address_hex",
                    "type": "text",
                    "value": original_hex  # 使用原始地址
                }
            ]
        }
        
        # 执行查询
        DUNE_API_BASE_URL = "https://api.dune.com/api/v1"
        url = f"{DUNE_API_BASE_URL}/query/{QUERY_ID}/execute"
        headers = {
            "X-Dune-API-Key": API_KEY,
            "Content-Type": "application/json"
        }
        
        # 添加调试信息
        print("\n" + "="*50)
        print("执行查询...")
        print(f"URL: {url}")
        print(f"Headers: {json.dumps(headers, indent=2)}")
        print(f"Params: {json.dumps(params, indent=2)}")
        print("="*50)
        
        print(f"\n=== 正在执行查询 ===")
        print(f"地址: {address_hex}")
        print(f"请求URL: {url}")
        print(f"请求参数: {params}")
        
        response = requests.post(url, headers=headers, json=params)
        
        print("\n=== 收到响应 ===")
        print(f"状态码: {response.status_code}")
        print(f"响应内容: {response.text}")
        
        if response.status_code != 200:
            print(f"查询执行失败，状态码: {response.status_code}")
            print(f"错误信息: {response.text}")
            return False
            
        result = response.json()
        execution_id = result.get('execution_id')
        state = result.get('state')
        
        if not execution_id or state == 'QUERY_STATE_FAILED':
            print(f"查询执行失败: {result.get('error', '未知错误')}")
            return False
            
        print(f"查询已提交，执行ID: {execution_id}")
        
        # 轮询查询状态
        max_attempts = 30
        for attempt in range(1, max_attempts + 1):
            wait_time = min(2 ** attempt, 60)  # 指数退避，最大60秒
            print(f"Checking status in {wait_time} seconds... (attempt {attempt}/{max_attempts})")
            time.sleep(wait_time)
            
            status_url = f"https://api.dune.com/api/v1/execution/{execution_id}/status"
            print(f"\n检查执行状态 (尝试 {attempt}/{max_attempts})")
            print(f"状态URL: {status_url}")
            
            status_response = requests.get(status_url, headers=headers)
            print(f"状态响应: {status_response.status_code} - {status_response.text}")
            
            if status_response.status_code != 200:
                print(f"获取状态失败: {status_response.status_code}")
                print(f"错误信息: {status_response.text}")
                return False
                
            status_data = status_response.json()
            print(f"状态数据: {status_data}")
            
            state = status_data.get('state')
            print(f"当前状态: {state}")
            
            if state == 'QUERY_STATE_COMPLETED':
                # 获取查询结果
                results_url = f"https://api.dune.com/api/v1/execution/{execution_id}/results"
                results_response = requests.get(results_url, headers=headers)
                
                if results_response.status_code == 200:
                    results = results_response.json()
                    result_rows = results.get('result', {}).get('rows', [])
                    has_results = bool(result_rows)
                    print(f"查询成功，找到 {len(result_rows)} 条记录")
                    return has_results
                else:
                    print(f"获取结果失败: {results_response.status_code}")
                    print(f"错误信息: {results_response.text}")
                    return False
                    
            elif state in ['QUERY_STATE_FAILED', 'QUERY_STATE_CANCELED']:
                print(f"Execution {execution_id} 状态: {state}")
                print(f"完整状态响应: {status_data}")
                
                # 尝试获取更详细的错误信息
                error_url = f"https://api.dune.com/api/v1/execution/{execution_id}/results"
                error_response = requests.get(error_url, headers=headers)
                
                print(f"\n查询执行 {execution_id} 失败")
                print(f"错误信息: 未知错误")
                print(f"错误详情响应状态: {error_response.status_code}")
                print(f"错误详情: {error_response.text}")
                
                return False
                
            print(f"查询仍在执行，{wait_time}秒后再次检查...")
            
            results_response = requests.get(results_url, headers=headers)
            print(f"结果响应: {results_response.status_code}")
            
            results_response.raise_for_status()
            results = results_response.json()
            print(f"完整结果: {results}")
            
            result_rows = results.get('result', {}).get('rows', [])
            has_results = bool(result_rows)
            print(f"找到结果: {has_results}, 结果行数: {len(result_rows)}")
            
            if has_results:
                print(f"结果示例: {result_rows[0]}")
            
            return has_results
        
    except Exception as e:
        print(f"Error executing Dune query: {e}")
        return False

def process_csv():
    """处理CSV文件并更新is_verified字段"""
    input_file = 'exchange_usdt_T_address_verified.csv'
    output_file = 'exchange_usdt_T_address_verified_updated.csv'
    
    # 读取CSV文件
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)
        
    print(f"成功读取 {len(rows)} 条记录")
    
    # 创建输出文件的副本，如果不存在
    if not os.path.exists(output_file):
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
    
    # 只处理第一行
    if not rows:
        print("错误: CSV文件为空")
        return
        
    row = rows[0]  # 只处理第一行
    print("\n" + "="*50)
    print("=== 开始处理第一个地址 ===")
    
    # 获取地址的十六进制形式
    address_hex = row.get('address_hex', '').strip()
    if not address_hex:
        print("错误: 第一个地址为空")
        return
        
    # 打印当前行信息
    print(f"\n=== 当前处理行 ===")
    for key, value in row.items():
        print(f"{key}: {value}")
    
    
    original_hex = address_hex
    address_hex = address_hex.strip()
    
    # 确保地址是有效的十六进制
    try:
        # 移除0x前缀（如果存在）
        if address_hex.lower().startswith('0x'):
            address_hex = address_hex[2:]
        
        # 验证十六进制格式和长度（不区分大小写）
        if not all(c in '0123456789abcdefABCDEF' for c in address_hex):
            print(f"!!! 错误: 无效的十六进制地址: {original_hex} - 包含非十六进制字符")
            return
        if len(address_hex) != 40:
            raise ValueError(f"地址长度必须为40个字符，当前为{len(address_hex)}个字符")
    except Exception as e:
        error_msg = f"无效的十六进制地址: {original_hex} - {str(e)}"
        print(f"\n!!! 错误: {error_msg}")
        row['is_verified'] = 'error'
        # 更新输出文件
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows([row])  # 只写入当前行
        return
        
    print(f"\n=== 地址处理 ===")
    print(f"原始十六进制: {original_hex}")
    print(f"处理后的十六进制: {address_hex}")
    print(f"长度: {len(address_hex)} 字符")
    
    # 执行Dune查询
    print("\n" + "="*50)
    print(f"开始处理十六进制地址: {original_hex}")
    print("="*50 + "\n")
    
    try:
        has_results = execute_dune_query(original_hex)
        
        print("\n" + "="*50)
        print(f"十六进制地址 {original_hex} 验证完成")
        print(f"交易记录: {'有' if has_results else '无'}")
        print("="*50 + "\n")
        
        # 更新验证状态
        row['is_verified'] = 'yes' if has_results else 'no'
        
        # 更新输出文件
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows([row])  # 只写入当前行
            
    except Exception as e:
        import traceback
        print(f"\n{'!'*50}")
        print(f"处理十六进制地址 {address_hex} 时出错:")
        traceback.print_exc()
        print("!"*50 + "\n")
        row['is_verified'] = 'error'
        # 更新输出文件
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows([row])  # 只写入当前行
    
    print("\n处理完成！")

if __name__ == "__main__":
    process_csv()
