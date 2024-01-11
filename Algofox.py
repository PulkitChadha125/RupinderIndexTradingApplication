import requests
import json
signal=0



mainurl=None
authenticate=None
placeorder=None

def createurl (url):
    global mainurl,placeorder,authenticate
    mainurl=f'https://{url}'
    authenticate=f'https://api.{url}/api/Trade/v1/authenticate'
    placeorder=f'http://api.{url}/api/Trade/v1/placeorder'


def login_algpfox(username,password,role):
    global mainurl, placeorder, authenticate
    req = requests.post(url=authenticate,
                        json={"username": username, "password": password,
                              "role": role})

    req = req.json()

    print(req['message'])


    return req['code']


def Cover_order_algofox(symbol, quantity,instrumentType, direction,product,strategy,order_typ,price,username,password,role,signal=signal,trigger=None,sll_price=None):
    global mainurl, placeorder, authenticate
    req = requests.get(mainurl)
    # algfx = json.loads(req)

    req = requests.post(url=authenticate,
                        json={"username": username, "password": password,
                              "role": role})
    t = req.json()

    algofox_token = t['data']['token']
    headers = "Bearer " + algofox_token
    headers = {"Authorization": headers, 'Content-Type': 'application/json'}

    data = {
        "signalId": int(signal),
        "triggerPrice": 0,
        "price":  price,
        "symbol": symbol,
        "signalType": direction,
        "orderType": order_typ,
        "productType": product,
        "instrumentType": instrumentType,
        "quantity": str(quantity),
        "strategy": strategy,
    }

    json_data = json.dumps(data)
    print(f'Sent Order= {json_data}')

    response = requests.post(url=placeorder, headers=headers, data=json_data)

    print(response.text)
    signal = signal + 1


def Short_order_algofox(symbol, quantity,instrumentType, direction,product,strategy,order_typ,price,username,password,role,signal=signal,trigger=None,sll_price=None):
    global mainurl, placeorder, authenticate
    req = requests.get(mainurl)
    # algfx = json.loads(req)

    req = requests.post(url=authenticate,
                        json={"username": username, "password": password,
                              "role": role})
    t = req.json()

    algofox_token = t['data']['token']
    headers = "Bearer " + algofox_token
    headers = {"Authorization": headers, 'Content-Type': 'application/json'}

    data = {
        "signalId": int(signal),
        "triggerPrice": 0,
        "price": price,
        "symbol": symbol,
        "signalType": direction,
        "orderType": order_typ,
        "productType": product,
        "instrumentType": instrumentType,
        "quantity": str(quantity),
        "strategy": strategy,
    }

    json_data = json.dumps(data)
    print(f'Sent Order= {json_data}')

    response = requests.post(url=placeorder, headers=headers, data=json_data)

    print(response.text)
    signal = signal + 1


def Sell_order_algofox(symbol, quantity,instrumentType, direction,product,strategy,order_typ,price,username,password,role,signal=signal,trigger=None,sll_price=None):
    global mainurl, placeorder, authenticate
    req = requests.get(mainurl)
    # algfx = json.loads(req)

    req = requests.post(url=authenticate,
                        json={"username": username, "password": password,
                              "role": role})
    t = req.json()

    algofox_token = t['data']['token']
    headers = "Bearer " + algofox_token
    headers = {"Authorization": headers, 'Content-Type': 'application/json'}

    data = {
        "signalId": int(signal),
        "triggerPrice": 0,
        "price":  price,
        "symbol": symbol,
        "signalType": direction,
        "orderType": order_typ,
        "productType": product,
        "instrumentType": instrumentType,
        "quantity": str(quantity),
        "strategy": strategy,
    }

    json_data = json.dumps(data)
    print(f'Sent Order= {json_data}')

    response = requests.post(url=placeorder, headers=headers, data=json_data)

    print(response.text)
    signal = signal + 1


def Buy_order_algofox(symbol, quantity,instrumentType, direction,product,strategy,order_typ,price,username,password,role,signal=signal,trigger=None,sll_price=None):
    global mainurl, placeorder, authenticate
    req = requests.get(mainurl)
    # algfx = json.loads(req)

    req = requests.post(url=authenticate,
                        json={"username": username, "password": password,
                              "role": role})
    t = req.json()

    algofox_token = t['data']['token']
    headers = "Bearer " + algofox_token
    headers = {"Authorization": headers, 'Content-Type': 'application/json'}

    data = {
        "signalId": int(signal),
        "triggerPrice": 0,
        "price":  price,
        "symbol": symbol,
        "signalType": direction,
        "orderType": order_typ,
        "productType": product,
        "instrumentType": instrumentType,
        "quantity": str(quantity),
        "strategy": strategy,
    }

    json_data = json.dumps(data)
    print(f'Sent Order= {json_data}')

    response = requests.post(url=placeorder, headers=headers, data=json_data)

    print(response.text)
    signal = signal + 1


def Check_Buy_order_algofox(symbol="RELIANCE", quantity="1",instrumentType="EQ", direction= "SELL",product="MIS",strategy="TEST",order_typ="MARKET",price=None,username="admin125",password="admin125",role="USER",signal=signal,trigger=None,sll_price=None):
    req = requests.get('https://api.algofox.in')
    # algfx = json.loads(req)



    req = requests.post(url='https://api.algofox.in/api/Trade/v1/authenticate',
                         json={"username":username, "password":password,
                               "role": role})
    t = req.json()

    algofox_token = t['data']['token']
    headers="Bearer "+algofox_token
    headers={"Authorization": headers,'Content-Type': 'application/json'}

    data = {
        "signalId": int(signal),
        "triggerPrice": 0,
        "price": 0 if order_typ == "MARKET" else price,
        "symbol": symbol,
        "signalType": direction,
        "orderType": order_typ,
        "productType": product,
        "instrumentType": instrumentType,
        "quantity": str(quantity),
        "strategy": strategy,
    }

    json_data = json.dumps(data)
    print(f'Sent Order= {json_data}')


    response = requests.post(url= "http://api.algofox.in/api/Trade/v1/placeorder", headers=headers, data=json_data)

    print(response.text)
    signal = signal + 1



