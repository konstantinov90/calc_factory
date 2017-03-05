"""equilibrium starter module"""
from requests import Request, Session


url = r'http://vm-ts-blk-app3:180/RunService.asmx?op=EquilRun'
run_type = 3

headers = {
    'Host': 'vm-ts-blk-app3',
    'Content-Type': 'text/xml; charset=utf-8',
    'SOAPAction': '"http://nicotech.ru/services/EquilRun"'
}

body = u"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xmlns:xsd="http://www.w3.org/2001/XMLSchema"
               xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <EquilRun xmlns="http://nicotech.ru/services">
      <runType>%i</runType>
    </EquilRun>
  </soap:Body>
</soap:Envelope>
""" % run_type

s = Session()

req = Request('POST', url, data=body, headers=headers)

prepped = s.prepare_request(req)

proxies = {'http': ''}

resp = s.send(prepped, proxies=proxies)

print(resp)
