import requests
import json
import duckdb
import urllib3
import copy
import ast
urllib3.disable_warnings()

DB = "db.duckdb"
URL = "https://api.inventoro.com/v1"
AUTH = {"clientId": "myteam0_GdqWenu",
        "secret": "93jNDC1heK4wieF3T8fNrUwKl04mqNqenX54ZXUv"}

def get_token():
    url = URL + "/login"
    headers = {"accept": "application/json",
               "Content-Type": "application/json"}
    data = AUTH
    response = requests.post(url, headers=headers, json=data).content.decode("utf-8")
    return json.loads(response)["access_token"]

def get_warehouse_info(token: str):
    url = URL + "/import/warehouse-products"
    headers = {"Authorization": "Bearer {}".format(token),
               "accept": "application/json"}
    response = requests.get(url, headers=headers)
    print(response.content)

def put_warehouse_info(token: str, inputpayload:list):
    url = URL + "/import/warehouse-products"
    headers = {"Authorization": "Bearer {}".format(token),
               "accept": "application/json",
               "Content-Type": "application/json"}
    response = requests.put(url, headers=headers, json=inputpayload)
    print(response.content)

def put_transaction_info(token: str, inputpayload:list):
    url = URL + "/import/transactions"
    headers = {"Authorization": "Bearer {}".format(token),
               "accept": "application/json",
               "Content-Type": "application/json"}
    response = requests.put(url, headers=headers, json=inputpayload)
    print(response.content)

def create_payload_from_db_warehouse() -> list:
    con = duckdb.connect(database=DB)
    data = con.execute("SELECT id, name, category, DENSE_RANK() OVER (ORDER BY category) AS group_id FROM (SELECT id, name, category, ROW_NUMBER() OVER (PARTITION BY category ORDER BY id) AS rn FROM Products)").fetchall()

    templist = []
    template = {"warehouse": {
                    "id": "1",
                    "name": "Warehouse"},
                "product": {
                    "id": "",
                    "name": "",
                    "category": {
                        "id": "",
                        "name": ""}},
                "availableSupply": 10,
                "salePrice": 10,
                "stockPrice": 10}

    for item in data:
        ndict = copy.deepcopy(template)
        ndict["product"]["id"] = str(item[0])
        ndict["product"]["name"] = str(item[1])
        ndict["product"]["category"]["id"] = str(item[3])
        ndict["product"]["category"]["name"] = str(item[2])
        templist.append(ndict)

    con.close()
    return templist


def create_payload_from_db_transactions() -> list:
    con = duckdb.connect(database=DB)

    reslist = []
    data = con.execute("SELECT lineItems FROM Sales").fetchall()
    data2 = con.execute("SELECT modifiedDate FROM Sales").fetchall()

    for item1, item2 in zip(data, data2):
        dt = item2[0].isoformat() + "Z"
        vals = ast.literal_eval((item1[0]))
        reqs = ["id", "transactionId", "productId", "createdDate", "qty", "unitPrice"]
        res = [{key: str(d[key]) for key in reqs} for d in vals]
        for d in res:
            d["modifiedDate"] = dt
            reslist.append(d)

    templist = []
    template = {"warehouseId": "1",
                "productId": "",
                "id": "",
                "transactionTypeId": "CLIENT_TRANSACTION",
                "dateOfTransaction": "",
                "amount": "",
                "price": "",
                "dateOfOrder": "",
                "promoSale": False,
                "extremeSale": False}

    for item in reslist:
        ndict = copy.deepcopy(template)
        ndict["productId"] = str(item["productId"])
        ndict["id"] = str(item["id"])
        ndict["dateOfTransaction"] = str(item["modifiedDate"])
        ndict["amount"] = float(item["qty"])
        ndict["price"] = float(item["unitPrice"])
        ndict["dateOfOrder"] = str(item["createdDate"])
        templist.append(ndict)

    con.close()
    return templist


if __name__ == "__main__":
    token = get_token()
    whdata = create_payload_from_db_warehouse()
    tsdata = create_payload_from_db_transactions()
    put_warehouse_info(token, whdata)
    put_transaction_info(token, tsdata)

    
