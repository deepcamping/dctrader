import requests
from bs4 import BeautifulSoup


# company_code를 입력받아 bs_obj를 출력
def get_bs_obj(company_code):
    url = "https://finance.naver.com/item/main.nhn?code=" + company_code
    result = requests.get(url)
    bs_obj = BeautifulSoup(result.content, "html.parser")
    return bs_obj


# company_code를 입력받아 now_price를 출력
def get_price(company_code):
    bs_obj = get_bs_obj(company_code)
    no_today = bs_obj.find("p", {"class": "no_today"})
    # no_today = bs_obj.find("div", {"class": "content_wrap"})
    # print(no_today)
    blind = no_today.find("span", {"class": "blind"})
    now_price = blind.text
    return now_price


# 펄어비스 회사 코드는 ”263750”
# 삼성전자 회사 코드는 ”005930”
# 셀트리온 회사 코드는 ”068270”
company_codes = ["263750", "005930", "068270"]

for item in company_codes:
    now_price = get_price(item)
    print(now_price)
