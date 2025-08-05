import requests
from bs4 import BeautifulSoup

url = "https://developers.google.com/earth-engine/datasets/catalog/NOAA_CFSV2_FOR6H?hl=zh-cn#bands"
# url = "https://developers.google.com/earth-engine/datasets/catalog/NOAA_CFSR"
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
proxies = {
    'http': 'http://127.0.0.1:10809',  # HTTP代理
    'https': 'http://127.0.0.1:10809',  # HTTPS代理
}
# 发送请求获取页面内容
response = requests.get(url, proxies=proxies, headers=headers)
if response.status_code != 200:
    raise Exception(f"请求失败，状态码：{response.status_code}")

# 解析 HTML
soup = BeautifulSoup(response.text, 'html.parser')
table = soup.find("table", class_="eecat")
if table is None:
    raise Exception("未找到 class 为 'eecat' 的表格。")

# 5. 提取所有数据行
result_dict = {}

# 遍历每一行
for row in table.find_all("tr"):
    cells = row.find_all(["td", "th"])
    if len(cells) < 2:
        continue  # 至少要有2列（key + value部分）
    key = cells[0].get_text(strip=True)
    value_parts = [cell.get_text(strip=True) for cell in cells[1:]]
    value = " | ".join(value_parts)  # 可用其他分隔符如 "," 或 "\t"
    result_dict[key] = value

# 打印结果
a = ','.join([k for k, v in result_dict.items() if k != '名称'])

print(a)



