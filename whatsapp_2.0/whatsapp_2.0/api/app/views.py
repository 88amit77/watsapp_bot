from rest_framework import viewsets
from rest_framework import status
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework import status

import datetime
import os
from io import BytesIO
import csv
import pandas as pd
import requests
import json
import psycopg2
import pymongo
import dropbox
access_token = '4joGxl-yofIAAAAAAAAAAW0Wa_qjsmOhQ6NYfWtkG0mNefNaTsIx8hD8BVgkavphaaa'
def db_credential(db_name,typ):
    url = "http://ec2-13-234-21-2298.ap-south-1.compute.amazonaws.com/db_credentials/"
    payload = json.dumps({
        "data_base_name": db_name
    })
    headers = {
        'Content-Type': 'application/json'
    }
    response = dict(requests.post(url, data=payload, headers=headers).json())
    print("response===>", response)
    status = response['status']
    print("payload", payload)
    print(response,type(response))
    if status == True:
        return response['db_detail'][typ]
    else:
        return
db_creds=db_credential('postgres','db_detail_for_psycopg2')
rds_host = db_creds['endPoint']
name = db_creds['userName']
password = db_creds['passWord']

class whatsappBot(APIView):
    def post(self, request):
        def send_wahts_msg(sender_number, text_to_be_sent):
            url = "https://panel.rapiwha.com/send_message.php"

            querystring = {"apikey": "6TP037GSFGW1YMJKDPHK", "number": sender_number, "text": text_to_be_sent}

            response = requests.request("GET", url, params=querystring)

            return response.text
        def update_number(oid, number):
            db_name = "orders"
            conn = psycopg2.connect(host=rds_host, database=db_name, user=name, password=password)
            cur = conn.cursor()
            qry = "UPDATE api_dispatchdetails SET phone='" + str(number)[2:12] + "' WHERE dd_id_id in (SELECT dd_id from api_neworder WHERE order_id='" + str(oid) + "')"
            print(qry)
            cur.execute(qry)
            conn.commit()
            cur.close()
            conn.close()
        def check_order_presence(oid):
            db_name = "orders"
            conn = psycopg2.connect(host=rds_host, database=db_name, user=name, password=password)
            cur = conn.cursor()
            qry = "SELECT no.order_id,no.product_id,dd.name,dd_id,warehouse_id from api_neworder no,api_dispatchdetails dd WHERE no.dd_id=dd.dd_id_id AND order_id='" + str(
                oid).replace("'","''") + "' order by dd_id desc"
            print(qry)
            cur.execute(qry)
            orders = cur.fetchall()
            cur.close()
            conn.close()
            if len(orders) > 0:
                return orders[0]
            else:
                return []

        def get_whats_msg(qr):
            url1 = "http://panel.rapiwha.com/get_messages.php"
            response = requests.request("GET", url1, params=qr)
            result = response.text[1:-1].split("}")
            resp = []
            for r in result:
                if r.startswith("{"):
                    # r+="}"
                    # print(json.loads(r))
                    resp.append(json.loads(r + "}"))
                elif r.startswith(",{"):
                    # r=r[1:]+"}"
                    # print(json.loads(r))
                    resp.append(json.loads(r[1:] + "}"))
                else:
                    pass
            return resp

        temp_d = str(request.data).replace("<QueryDict: ","").replace("['","").replace("']","").replace(">","").replace("'data'",'"data"')
        recieved_msgs =json.loads(temp_d)["data"]# temp_d[0:9].replace("'",'"')+temp_d[10:len(temp_d)-2]+"}"
        ev = recieved_msgs["event"]
        if ev == 'INBOX':
            if True:
                sender = recieved_msgs['from']
                msg_type = ev
                recieved_msg = recieved_msgs['text']
                qr = {"apikey": "6TP037GSFGW1YMJKDPHK", "number": sender, "type": "OUT", "markaspulled": "1",
                      "getnotpulledonly": "0"}
                all_rec_messages=get_whats_msg(qr)[-5:]
                all_rc_messsages=[]
                for am in all_rec_messages:
                    all_rc_messsages.append(am['text'])
                rvmsg = all_rec_messages[-1:]
                sent_msg = ''
                if len(rvmsg) > 0:
                    sent_msg = rvmsg[0]['text']
                print(all_rc_messsages)
                if all_rc_messsages.count(sent_msg)>2:
                    rpl_msg='Sorry for the incovinience we think there is some trouble in conversation'
                    print('sending ' + sender + ' an automated msg - ' + rpl_msg)
                    print(send_wahts_msg(sender, rpl_msg))
                    print('response is a below')
                    return Response({"statusCode":"200","body":"SUCCESS"})
                if msg_type == 'INBOX':
                    check_box = [
                        'How was your shopping experience.\n\n1. Very good \n2. It was average \n3. Not happy - want to return the product \n\nPlease type any number to allow us to know your experience. \n\nEg : type 1 if you were very happy with your shopping experience',
                        'Can you please give us your order id as mentioned in your invoice','Please let us know what went wrong so that we can ensure better services next time around' ]
                    if recieved_msg == 'Hi !! I just purchased one of your products.' and (
                            sent_msg == '' or sent_msg not in check_box):
                        rpl_msg = "It’s an honour , that you preferred Buymore as your choice of seller for purchasing the product.\n\nThere are so many options out there but you singled us out and we really appreciate your business."
                        print('sending ' + sender + ' an automated msg - ' + rpl_msg)
                        print('response is a below')
                        print(send_wahts_msg(sender, rpl_msg))
                        rpl_msg = "Can you please give us your order id as mentioned in your invoice"
                        print('sending ' + sender + ' an automated msg - ' + rpl_msg)
                        print('response is a below')
                        print(send_wahts_msg(sender, rpl_msg))
                    else:
                        if len(rvmsg) > 0:
                            if sent_msg in [
                                'How was your shopping experience.\n\n1. Very good \n2. It was average \n3. Not happy - want to return the product \n\nPlease type any number to allow us to know your experience. \n\nEg : type 1 if you were very happy with your shopping experience']:
                                if recieved_msg in ["1", "2", "3"]:
                                    if recieved_msg == "1":
                                        warehouse_id_links={3:'https://g.co/kgs/EfjYFd',4:'https://g.co/kgs/kAGprm',7:'https://g.co/kgs/kAGprm',10:'https://g.co/kgs/TXop76'}
                                        qr["type"] = "IN"
                                        convo_msgs = get_whats_msg(qr)[-4:]
                                        ord_details = []
                                        for cnm in convo_msgs:
                                            ord_details = check_order_presence(cnm['text'])
                                            if ord_details != []:
                                                break
                                        if ord_details == []:
                                            ord_details = ['', 0, 'N/A', sender, 4]
                                        try:
                                            warehouse_id_link=warehouse_id_links[ord_details[4]]
                                        except:
                                            warehouse_id_link = warehouse_id_links[4]
                                        rpl_msg = "We are greatful that you liked our product and the service. \n\nWe would request you to rate the product using the link below \n\nLink : "+warehouse_id_link+" \n\nCounfreedise Retail Services came into existence in 2018 to bring about a technological revolution in Retail industry , today over 200 brands trusts us to run there entire e-commerce business. Your reviews on the link below will help us get noticed and we will strive harder to make a noticeable change in the Retail industry. \n\nLink google review link \n\nIf you want to know more about us please visit us on our website \n\nhttps://app.sellerbuymore.com/ \n\nWe are giving away 100 prizes every week. Our representative will call you if you are selected in the lucky draw. Your lucky draw number is '"+str(ord_details[3])+"'."
                                    elif recieved_msg == "2":
                                        rpl_msg = 'Please let us know what went wrong so that we can ensure better services next time around'
                                    else:
                                        rpl_msg = 'Kindly elloborate on your issue so that we can arrange a call back from our representative to initiate pickup of our product as soon as possible'
                                    print('sending ' + sender + ' an automated msg - ' + rpl_msg)
                                    print('response is a below')
                                    print(send_wahts_msg(sender, rpl_msg))
                                else:
                                    rpl_msg = 'OOPS,we think you have entered a wrong value'
                                    print('sending ' + sender + ' an automated msg - ' + rpl_msg)
                                    print('response is a below')
                                    print(send_wahts_msg(sender, rpl_msg))
                                    rpl_msg = 'How was your shopping experience.\n\n1. Very good \n2. It was average \n3. Not happy - want to return the product \n\nPlease type any number to allow us to know your experience. \n\nEg : type 1 if you were very happy with your shopping experience'
                                    print('sending ' + sender + ' an automated msg - ' + rpl_msg)
                                    print('response is a below')
                                    print(send_wahts_msg(sender, rpl_msg))
                            elif sent_msg in ['Can you please give us your order id as mentioned in your invoice']:
                                ord_det = check_order_presence(recieved_msg)
                                if ord_det != []:
                                    update_number(recieved_msg,sender)
                                    rpl_msg = str(ord_det[2])
                                    if rpl_msg != 'N/A' and rpl_msg != 'None':
                                        pass
                                    else:
                                        rpl_msg = ""
                                    print('sending ' + sender + ' an automated msg - ' + rpl_msg)
                                    print('response is a below')
                                    print(send_wahts_msg(sender, "Hi, " + rpl_msg))
                                    rpl_msg = 'How was your shopping experience.\n\n1. Very good \n2. It was average \n3. Not happy - want to return the product \n\nPlease type any number to allow us to know your experience. \n\nEg : type 1 if you were very happy with your shopping experience'
                                    print('sending ' + sender + ' an automated msg - ' + rpl_msg)
                                    print('response is a below')
                                    print(send_wahts_msg(sender, rpl_msg))
                                else:
                                    rpl_msg = "Sorry we couldnot find the order id you entered"
                                    print('sending ' + sender + ' an automated msg - ' + rpl_msg)
                                    print('response is a below')
                                    print(send_wahts_msg(sender, rpl_msg))
                                    rpl_msg = "Can you please give us your order id as mentioned in your invoice"
                                    print('sending ' + sender + ' an automated msg - ' + rpl_msg)
                                    print('response is a below')
                                    print(send_wahts_msg(sender, rpl_msg))
                            elif sent_msg in ['Please let us know what went wrong so that we can ensure better services next time around']:
                                rpl_msg = "We will surely look into this and get back to you within 2 working days. \n\nHave a great day. Hope you and your family are safe"
                                print('sending ' + sender + ' an automated msg - ' + rpl_msg)
                                print('response is a below')
                                print(send_wahts_msg(sender, rpl_msg))
                else:
                    print(recieved_msgs)
        else:
            print('no new messages')
        return Response({"statusCode":"200","body":"SUCCESS"})


class paytmPaymentDifference(APIView):
    def post(self, request):
        Paytm_mongo_payment = "/tmp/Paytm_mongo_payment.csv"
        Paytm_portal_payment = "/tmp/Paytm_portal_payment.csv"
        Paytm_process_file = "/tmp/Paytm_process_file.csv"
        Paytm_vendor_details = "/tmp/Paytm_vendor_details.csv"
        Paytm_order_with_difference = "/tmp/Paytm_order with difference.csv"
        Paytm_p_and_l_file = "/tmp/Paytm_P&L_file_final.csv"
        Paytm_portal_details = '/tmp/Paytm_portal_details.csv'
        Paytm_reimbursment_file = '/tmp/Paytm_reimbursment.csv'
        Paytm_mongo_payment_1 = "/tmp/Paytm_mongo_payment_1.csv"
        order_detail_order_date = "/tmp/order_date_details.csv"
        with open(Paytm_mongo_payment_1, 'w', newline='')as RD1:
            RD = csv.writer(RD1)
            RD.writerow(['order id', 'item id', 'sku', 'portal id', 'vendor_id', 'vendorpayable amount', 'product id'])

        with open(Paytm_portal_payment, 'w', newline='')as RD2:
            RD_1 = csv.writer(RD2)
            RD_1.writerow(['item id', 'portal payment'])

        with open(Paytm_process_file, 'w', newline='')as RD3:
            RD_2 = csv.writer(RD3)
            RD_2.writerow(
                ['order id', 'item id', 'sku', 'portal name', 'vendor id', 'vendor payment amount', 'portal payment',
                 'difference', 'product id'])

        with open(Paytm_vendor_details, 'w', newline='')as RD4:
            RD_3 = csv.writer(RD4)
            RD_3.writerow(['vendor id', 'vendor_name', 'vendor type'])

        with open(Paytm_p_and_l_file, 'w', newline='')as RD6:
            RD_5 = csv.writer(RD6)
            RD_5.writerow(
                ['order id', 'portal name', 'vendor payment amount', 'portal payment', 'difference', 'endor_id',
                 'vendor_name', 'vendor type'])
        with open(Paytm_portal_details, 'w', newline='')as RD6:
            RD_5 = csv.writer(RD6)
            RD_5.writerow(['item id', 'selling price', 'mrp', 'qty'])

        with open(Paytm_mongo_payment, 'w', newline='')as RD8:
            RD_7 = csv.writer(RD8)
            RD_7.writerow(
                ['order id', 'item id', 'sku', 'portal id', 'vendor_id', 'vendorpayable amount', 'product id'])

        with open(Paytm_reimbursment_file, 'w', newline='')as RD7:
            RD_6 = csv.writer(RD7)
            RD_6.writerow(['item id', 'reimbursment amount'])

        try:
            client = pymongo.MongoClient("mongodb+srv://Counfreedise:buymore123@cluster0-tq9zt.mongodb.net/buymore")
            print("connected")
        except Exception as e:
            print(e)

        db = client.wms

        cursor = db.buymore_two_payments.aggregate(
            [{'$match': {'portal_id': 6}}, {'$addFields': {'payable_amount': {'$toDouble': '$payable_amount'}}}, {
                '$group': {
                    '_id': {"order_id": "$order_id", "order_item_id": "$order_item_id", "portal_sku": "$portal_sku",
                            "vendor_id": "$vendor_id", "portal_id": "$portal_id", "product_id": "$product_id"},
                    'total': {'$sum': "$payable_amount"}}}])
        for item in cursor:
            print(item)
            try:
                order_id1 = item['_id']['order_id']
                item_id = item['_id']['order_item_id']
                sku = item['_id']['portal_sku']
                vendor_id = item['_id']['vendor_id']
                portal_id = item['_id']['portal_id']
                # selling_price = item['_id']['selling_price']
                payable_amount1 = item['total']
                product_id = item['_id']['product_id']
                # print(order_id1,sku,vendor_id,payable_amount1)
            except Exception as E:
                print(E)

            # print(order_id1,payable_amount1)

            with open(Paytm_mongo_payment_1, 'a', newline='') as f1:
                t1 = csv.writer(f1)
                t1.writerow([order_id1, item_id, sku, portal_id, vendor_id, payable_amount1, product_id])
        print("mongo file generated")

        try:
            client = pymongo.MongoClient("mongodb+srv://Counfreedise:buymore123@cluster0-tq9zt.mongodb.net/buymore")
            print("connected")
        except Exception as e:
            print(e)

        db = client.wms

        result = client['wms']['buymore_two_payments'].aggregate([
            {'$match':
                 {'portal_id': 6}},
            {
                '$match': {
                    'reimbursement_list': {
                        '$nin': [
                            [], None
                        ]
                    }
                }
            }, {
                '$unwind': {
                    'path': '$reimbursement_list'
                }
            }, {
                '$addFields': {
                    'reimbursement_amount': {
                        '$toDouble': '$reimbursement_list.reimbursement_amount'
                    }
                }
            }, {
                '$group': {
                    '_id': {
                        'order_item_id': '$order_item_id'
                    },
                    'reimbusermentamount': {
                        '$sum': '$reimbursement_amount'
                    }
                }
            }
        ])

        for item2 in result:
            # print(item2)

            item_id2 = item2['_id']['order_item_id']

            reimbursment_amount = float(item2['reimbusermentamount'])

            print(item_id2, reimbursment_amount)

            with open(Paytm_reimbursment_file, 'a', newline='') as f1:
                t1 = csv.writer(f1)
                t1.writerow([item_id2, reimbursment_amount])

        df10 = pd.read_csv(Paytm_mongo_payment_1, encoding='unicode_escape')
        df11 = pd.read_csv(Paytm_reimbursment_file, encoding='unicode_escape')
        df12 = pd.merge(left=df10, right=df11, how='left', left_on='item id', right_on='item id')
        df12.to_csv(r'/tmp/Paytm_mongo_reimbursment.csv', index=False)

        df13 = pd.read_csv('/tmp/Paytm_mongo_reimbursment.csv')
        df14 = df13.fillna(0)
        df14.to_csv(r'/tmp/Paytm_mongo_reimbursment_final.csv', index=False)

        with open('/tmp/Paytm_mongo_reimbursment_final.csv', 'r', newline='')as OD_2:
            OD_3 = list(csv.reader(OD_2))
        #
        for d3 in range(1, len(OD_3)):
            order_id_2 = OD_3[d3][0]
            item_id_2 = OD_3[d3][1]
            sku = OD_3[d3][2]
            portal_id_2 = OD_3[d3][3]
            venor_id_2 = OD_3[d3][4]
            # selling_price_2 = OD_3[d3][6]
            vendor_payable_amount_2 = float(OD_3[d3][5])
            reim_amount_2 = float(OD_3[d3][7])
            product_id_2 = OD_3[d3][6]

            paybale_amount = vendor_payable_amount_2 + reim_amount_2

            with open(Paytm_mongo_payment, 'a', newline='') as f1:
                t1 = csv.writer(f1)
                t1.writerow([order_id_2, item_id_2, sku, portal_id_2, venor_id_2, paybale_amount, product_id_2])

        db_name = "orders"
        conn = psycopg2.connect(host=rds_host, database=db_name, user=name, password=password)
        qry = "select item_id,sum(settlement_value) as sett from api_paymentdetailscsv where item_id is not null and portal = 'paytm'  group by item_id"
        cur = conn.cursor()
        cur.execute(qry)
        result = cur.fetchall()
        cur.close()
        conn.close()
        for r1 in result:
            item_id3 = "#" + str(r1[0])
            payable_amount2 = str(r1[1])

            with open(Paytm_portal_payment, 'a', newline='') as f1:
                t1 = csv.writer(f1)
                t1.writerow([item_id3, payable_amount2])
        print("portal payment file generated")

        df1 = pd.read_csv(Paytm_mongo_payment, encoding='unicode_escape')
        df2 = pd.read_csv(Paytm_portal_payment, encoding='unicode_escape')
        df3 = pd.merge(left=df1, right=df2, left_on="item id", right_on="item id")
        df3.to_csv(r'/tmp/Paytm_mongo_portal.csv', index=False)

        print('mongo_portal file generated')

        with open('/tmp/Paytm_mongo_portal.csv', 'r', newline='')as payment_data:
            payment_data1 = list(csv.reader(payment_data))

        for i in range(1, len(payment_data1)):

            order_id_id = str((payment_data1[i][0]))
            item_id_id = str((payment_data1[i][1]))
            bsku = str((payment_data1[i][2]))
            vendor_id_id = str((payment_data1[i][4]))
            # selling_price_price = str((payment_data1[i][6]))
            vendor_amount_amount = float((payment_data1[i][5]))
            portal_payment_payment = float((payment_data1[i][7]))
            produc_id_id_3 = payment_data1[i][6]

            portal_id_id = str((payment_data1[i][3]))
            # selling_price_price = str((payment_data1[i][3]))
            if portal_id_id == '1':
                portal_id_id = 'Amazon'

            differcence = portal_payment_payment - vendor_amount_amount

            with open(Paytm_process_file, 'a', newline='') as f1:
                t1 = csv.writer(f1)
                t1.writerow([order_id_id, item_id_id, bsku, portal_id_id, vendor_id_id, vendor_amount_amount,
                             portal_payment_payment, differcence, produc_id_id_3])
        print('process file generated')
        with open(Paytm_process_file, 'r', newline='')as OD:
            OD1 = list(csv.reader(OD))

        venid = "(-1"
        for d in range(1, len(OD1)):
            vendor_id_id1 = OD1[d][4]
            if str(vendor_id_id1) not in venid:
                venid += "," + str(vendor_id_id1)
        venid += ")"
        db_name = "vendors"
        conn = psycopg2.connect(host=rds_host, database=db_name, user=name, password=password)
        qry = "select id,vendor_name,vendor_type from api_newvendordetails where id in" + venid
        cur = conn.cursor()
        cur.execute(qry)
        result2 = cur.fetchall()
        cur.close()
        conn.close()
        for r2 in result2:
            vendor_id_id2 = str(r2[0])
            vendor_name = str(r2[1])
            vendor_type = str(r2[2])
            if vendor_type == "1":
                vendor_type = "Non SOR order"
            if vendor_type == "2":
                vendor_type = "SOR order"

            with open(Paytm_vendor_details, 'a', newline='') as f1:
                t1 = csv.writer(f1)
                t1.writerow([vendor_id_id2, vendor_name, vendor_type])
        print('vendor details file generated')
        db_name = "orders"
        conn = psycopg2.connect(host=rds_host, database=db_name, user=name, password=password)
        qry = "select order_item_id,sum(selling_price) as sp,sum(mrp) as mrp,sum(qty) as qty from api_neworder where order_item_id is not null and portal_id = 6 group by order_item_id"
        cur = conn.cursor()
        cur.execute(qry)
        result3 = cur.fetchall()
        cur.close()
        conn.close()

        # print(result3)

        for r3 in result3:
            oditem_1 = '#' + str(r3[0])
            sum_sp = str(r3[1])
            sum_mrp = str(r3[2])
            sum_qty = str(r3[3])

            with open(Paytm_portal_details, 'a', newline='') as f1:
                t1 = csv.writer(f1)
                t1.writerow(
                    [oditem_1, sum_sp, sum_mrp, sum_qty])

        with open(order_detail_order_date, 'w', newline='')as RD7:
            RD_6 = csv.writer(RD7)
            RD_6.writerow(['item id', 'order date'])
        print('portal details file generated')
        db_name = "orders"
        conn = psycopg2.connect(host=rds_host, database=db_name, user=name, password=password)
        qry = "select distinct(order_item_id),order_date from api_neworder where portal_id = 6"
        cur = conn.cursor()
        cur.execute(qry)
        result3 = cur.fetchall()
        cur.close()
        conn.close()

        # print(result3)

        for r3 in result3:
            oditem_1 = '#' + str(r3[0])
            order_date = str(r3[1])

            with open(order_detail_order_date, 'a', newline='') as f1:
                t1 = csv.writer(f1)
                t1.writerow(
                    [oditem_1, order_date])
        print('portal details with date file generated')

        df4 = pd.read_csv(Paytm_process_file, encoding='unicode_escape')
        df5 = pd.read_csv(Paytm_vendor_details, encoding='unicode_escape')
        df6 = pd.merge(left=df4, right=df5, left_on="vendor id", right_on="vendor id")
        df6.to_csv(r'/tmp/Paytm_P&L_file.csv', index=False)

        print('P&L file generated')

        with open('/tmp/Paytm_P&L_file.csv', 'r', newline='')as OD_1:
            OD_2 = list(csv.reader(OD_1))

        df7 = pd.read_csv('/tmp/Paytm_P&L_file.csv', encoding='unicode_escape')
        df8 = pd.read_csv(Paytm_portal_details, encoding='unicode_escape')
        df9 = pd.merge(left=df7, right=df8, left_on='item id', right_on='item id')
        df9.to_csv(r'/tmp/Paytm_P&L_file_final_data.csv', index=False)

        df15 = pd.read_csv(r'/tmp/Paytm_P&L_file_final_data.csv', encoding='unicode_escape')
        df16 = pd.read_csv(Paytm_reimbursment_file, encoding='unicode_escape')
        df17 = pd.merge(left=df15, right=df16, how='left', left_on='item id', right_on='item id')
        df17.to_csv(r'/tmp/Paytm_P&L_file_final_data_reim.csv', index=False)

        df18 = pd.read_csv('/tmp/Paytm_P&L_file_final_data_reim.csv')
        df19 = df18.fillna(0)
        df19.to_csv(r'/tmp/Paytm_P&L_file_final_data_reim_final.csv', index=False)
        db_name = "products"
        conn = psycopg2.connect(host=rds_host, database=db_name, user=name, password=password)
        qry = "select distinct(product_id),paytm_upload_selling_price from paytm_paytmproducts"
        cur = conn.cursor()
        cur.execute(qry)
        result_usp = cur.fetchall()
        cur.close()
        conn.close()

        filepath = '/tmp/Paytm_uploadsellingprice.csv'  # add the file path here
        header = ['product id', 'upload selling price']
        with open(filepath, 'w', newline='') as f:
            wr = csv.writer(f)
            wr.writerow(header)
            wr.writerows(result_usp)

        db_name = "products"
        conn = psycopg2.connect(host=rds_host, database=db_name, user=name, password=password)
        qry = "select distinct(product_id),product_mrp from master_masterproduct"
        cur = conn.cursor()
        cur.execute(qry)
        result_mrp = cur.fetchall()
        cur.close()
        conn.close()

        filepath = '/tmp/Paytm_mrpprice.csv'  # add the file path here
        header = ['product id', 'mastr_product_mrp']
        with open(filepath, 'w', newline='') as f:
            wr = csv.writer(f)
            wr.writerow(header)
            wr.writerows(result_mrp)

        db_name = "products"
        conn = psycopg2.connect(host=rds_host, database=db_name, user=name, password=password)
        qry = "select distinct(product_id),billing_price from purchase_invoice_sorbilling"
        cur = conn.cursor()
        cur.execute(qry)
        result_sor = cur.fetchall()
        cur.close()
        conn.close()

        filepath = '/tmp/Paytm_sorbilling.csv'  # add the file path here
        header = ['product id', 'sor billing']
        with open(filepath, 'w', newline='') as f:
            wr = csv.writer(f)
            wr.writerow(header)
            wr.writerows(result_sor)

        df19 = pd.read_csv(r'/tmp/Paytm_mrpprice.csv', encoding='unicode_escape')
        df20 = pd.read_csv('/tmp/Paytm_uploadsellingprice.csv', encoding='unicode_escape')
        df21 = pd.merge(left=df19, right=df20, how='left', left_on='product id', right_on='product id')
        df21.to_csv(r'/tmp/Paytm_mrp_upload.csv', index=False)

        df22 = pd.read_csv(r'/tmp/Paytm_mrp_upload.csv', encoding='unicode_escape')
        df23 = pd.read_csv('/tmp/Paytm_sorbilling.csv', encoding='unicode_escape')
        df24 = pd.merge(left=df22, right=df23, how='left', left_on='product id', right_on='product id')
        df24.to_csv(r'/tmp/Paytm_mrp_upload_sor.csv', index=False)

        df27 = pd.read_csv(r'/tmp/Paytm_P&L_file_final_data_reim_final.csv', encoding='unicode_escape')
        df28 = pd.read_csv('/tmp/Paytm_mrp_upload.csv', encoding='unicode_escape')
        df29 = pd.merge(left=df27, right=df28, how='left', left_on='product id', right_on='product id')
        df29.to_csv(r'/tmp/P&L_file_final_data_reim_final_Paytm.csv', index=False)

        df30 = pd.read_csv(r'/tmp/P&L_file_final_data_reim_final_Paytm.csv', encoding='unicode_escape')
        df31 = pd.read_csv(order_detail_order_date, encoding='unicode_escape')
        df32 = pd.merge(left=df30, right=df31, how='left', left_on='item id', right_on='item id')
        df32.to_csv(r'/tmp/P&L_file_final_data_reim_final_Paytm_1.csv', index=False)

        df_2 = pd.read_csv('/tmp/P&L_file_final_data_reim_final_Paytm_1.csv')
        df_filtered = df_2[~df_2['vendor type'].str.contains('3')]
        df_filtered.to_csv('/tmp/P&L_file_final_data_reim_final_Paytm_1.csv', index=False)

        paytm_p_l = '/tmp/Paytm_p & L file .csv'
        with open(paytm_p_l, 'w', newline='')as RD1:
            RD = csv.writer(RD1)
            RD.writerow(
                ['order id', 'item id', 'sku', 'portal name', 'vendor id', 'vendor payment amount', 'portal payment',
                 'difference', 'product id', 'vendor_name', 'vendor type', 'selling price', 'mrp', 'qty',
                 'reimbursment amount', 'mastr_product_mrp', 'upload selling price', 'order date'])

        with open('/tmp/P&L_file_final_data_reim_final_Paytm_1.csv', 'r', newline='', encoding="utf8")as f1:
            D1 = list(csv.reader(f1))
            # print(D1)

        for i in range(1, len(D1)):

            order_id = str(D1[i][0])
            item_id = str(D1[i][1])
            sku = str(D1[i][2])
            portal_name = str(D1[i][3])
            vendor_id = str(D1[i][4])
            vendor_payment_amount = int(float(str(D1[i][5])))
            portal_payment = int(float(str(D1[i][6])))
            difference = int(float(D1[i][7]))
            product_id = str(D1[i][8])
            vendor_name = str(D1[i][9])
            vendor_type = str(D1[i][10])
            selling_price = str(D1[i][11])
            mrp = str(D1[i][12])
            qty = int(str(D1[i][13]))
            reimbursment_amount = float(D1[i][14])
            try:
                mastr_product_mrp = int(float(D1[i][15]))
            except:
                mastr_product_mrp = 0
            try:
                upload_selling_price = float(D1[i][16])
            except:
                upload_selling_price = 0
            order_date = str(D1[i][17])
            mastr_product_mrp_1 = mastr_product_mrp * qty
            upload_selling_price_1 = upload_selling_price * qty

            with open(paytm_p_l, 'a', newline='') as f1:
                t1 = csv.writer(f1)
                t1.writerow(
                    [order_id, item_id, sku, portal_name, vendor_id, vendor_payment_amount, portal_payment, difference,
                     product_id, vendor_name, vendor_type, selling_price, mrp, qty, reimbursment_amount,
                     mastr_product_mrp_1, upload_selling_price_1, order_date])
        print('writing to dropbox')
        dbx = dropbox.Dropbox(access_token)
        file_from = paytm_p_l
        file_to = '/buymore2/PaymentDifference/paytm_payment_difference.csv'
        with open(file_from, 'rb') as f:
            data = dbx.files_upload(f.read(), file_to, mode=dropbox.files.WriteMode.overwrite)
        print('writing done')
        return Response({"statusCode": "200", "body": "SUCCESS"})


class amazonPaymentDifference(APIView):
    def post(self, request):
        Amazon_mongo_payment = "/tmp/Amazon_mongo_payment.csv"
        Amazon_portal_payment = "/tmp/Amazon_portal_payment.csv"
        Amazon_process_file = "/tmp/Amazon_process_file.csv"
        Amazon_vendor_details = "/tmp/Amazon_vendor_details.csv"
        Amazon_order_with_difference = "/tmp/Amazon_order with difference.csv"
        Amazon_p_and_l_file = "/tmp/Amazon_P&L_file_final.csv"
        Amazon_portal_details = '/tmp/Amazon_portal_details.csv'
        Amazon_reimbursment_file = '/tmp/Amazon_reimbursment.csv'
        Amazon_mongo_payment_1 = "/tmp/Amazon_mongo_payment_1.csv"
        order_detail_order_date = "/tmp/order_date_details.csv"
        with open(Amazon_mongo_payment_1, 'w', newline='')as RD1:
            RD = csv.writer(RD1)
            RD.writerow(['order id', 'item id', 'sku', 'portal id', 'vendor_id', 'vendorpayable amount', 'product id'])

        with open(Amazon_portal_payment, 'w', newline='')as RD2:
            RD_1 = csv.writer(RD2)
            RD_1.writerow(['item id', 'portal payment'])

        with open(Amazon_process_file, 'w', newline='')as RD3:
            RD_2 = csv.writer(RD3)
            RD_2.writerow(
                ['order id', 'item id', 'sku', 'portal name', 'vendor id', 'vendor payment amount', 'portal payment',
                 'difference', 'product id'])

        with open(Amazon_vendor_details, 'w', newline='')as RD4:
            RD_3 = csv.writer(RD4)
            RD_3.writerow(['vendor id', 'vendor_name', 'vendor type'])

        with open(Amazon_p_and_l_file, 'w', newline='')as RD6:
            RD_5 = csv.writer(RD6)
            RD_5.writerow(
                ['order id', 'portal name', 'vendor payment amount', 'portal payment', 'difference', 'endor_id',
                 'vendor_name', 'vendor type'])
        with open(Amazon_portal_details, 'w', newline='')as RD6:
            RD_5 = csv.writer(RD6)
            RD_5.writerow(['item id', 'selling price', 'mrp', 'qty'])

        with open(Amazon_mongo_payment, 'w', newline='')as RD8:
            RD_7 = csv.writer(RD8)
            RD_7.writerow(
                ['order id', 'item id', 'sku', 'portal id', 'vendor_id', 'vendorpayable amount', 'product id'])

        with open(Amazon_reimbursment_file, 'w', newline='')as RD7:
            RD_6 = csv.writer(RD7)
            RD_6.writerow(['item id', 'reimbursment amount'])

        try:
            client = pymongo.MongoClient("mongodb+srv://Counfreedise:buymore123@cluster0-tq9zt.mongodb.net/buymore")
            print("connected")
        except Exception as e:
            print(e)

        db = client.wms

        cursor = db.buymore_two_payments.aggregate(
            [{'$match': {'portal_id': 1}}, {'$addFields': {'payable_amount': {'$toDouble': '$payable_amount'}}}, {
                '$group': {
                    '_id': {"order_id": "$order_id", "order_item_id": "$order_item_id", "portal_sku": "$portal_sku",
                            "vendor_id": "$vendor_id", "portal_id": "$portal_id", "product_id": "$product_id"},
                    'total': {'$sum': "$payable_amount"}}}])
        for item in cursor:
            print(item)
            try:
                order_id1 = item['_id']['order_id']
                item_id = item['_id']['order_item_id']
                sku = item['_id']['portal_sku']
                vendor_id = item['_id']['vendor_id']
                portal_id = item['_id']['portal_id']
                # selling_price = item['_id']['selling_price']
                payable_amount1 = item['total']
                product_id = item['_id']['product_id']
                # print(order_id1,sku,vendor_id,payable_amount1)
            except Exception as E:
                print(E)

            # print(order_id1,payable_amount1)

            with open(Amazon_mongo_payment_1, 'a', newline='') as f1:
                t1 = csv.writer(f1)
                t1.writerow([order_id1, item_id, sku, portal_id, vendor_id, payable_amount1, product_id])
        print("mongo file generated")

        try:
            client = pymongo.MongoClient("mongodb+srv://Counfreedise:buymore123@cluster0-tq9zt.mongodb.net/buymore")
            print("connected")
        except Exception as e:
            print(e)

        db = client.wms

        result = client['wms']['buymore_two_payments'].aggregate([
            {'$match':
                 {'portal_id': 1}},
            {
                '$match': {
                    'reimbursement_list': {
                        '$nin': [
                            [], None
                        ]
                    }
                }
            }, {
                '$unwind': {
                    'path': '$reimbursement_list'
                }
            }, {
                '$addFields': {
                    'reimbursement_amount': {
                        '$toDouble': '$reimbursement_list.reimbursement_amount'
                    }
                }
            }, {
                '$group': {
                    '_id': {
                        'order_item_id': '$order_item_id'
                    },
                    'reimbusermentamount': {
                        '$sum': '$reimbursement_amount'
                    }
                }
            }
        ])

        for item2 in result:
            # print(item2)

            item_id2 = item2['_id']['order_item_id']

            reimbursment_amount = float(item2['reimbusermentamount'])

            print(item_id2, reimbursment_amount)

            with open(Amazon_reimbursment_file, 'a', newline='') as f1:
                t1 = csv.writer(f1)
                t1.writerow([item_id2, reimbursment_amount])

        df10 = pd.read_csv(Amazon_mongo_payment_1, encoding='unicode_escape')
        df11 = pd.read_csv(Amazon_reimbursment_file, encoding='unicode_escape')
        df12 = pd.merge(left=df10, right=df11, how='left', left_on='item id', right_on='item id')
        df12.to_csv(r'/tmp/Amazon_mongo_reimbursment.csv', index=False)

        df13 = pd.read_csv('/tmp/Amazon_mongo_reimbursment.csv')
        df14 = df13.fillna(0)
        df14.to_csv(r'/tmp/Amazon_mongo_reimbursment_final.csv', index=False)

        with open('/tmp/Amazon_mongo_reimbursment_final.csv', 'r', newline='')as OD_2:
            OD_3 = list(csv.reader(OD_2))
        #
        for d3 in range(1, len(OD_3)):
            order_id_2 = OD_3[d3][0]
            item_id_2 = OD_3[d3][1]
            sku = OD_3[d3][2]
            portal_id_2 = OD_3[d3][3]
            venor_id_2 = OD_3[d3][4]
            # selling_price_2 = OD_3[d3][6]
            vendor_payable_amount_2 = float(OD_3[d3][5])
            reim_amount_2 = float(OD_3[d3][7])
            product_id_2 = OD_3[d3][6]

            paybale_amount = vendor_payable_amount_2 + reim_amount_2

            with open(Amazon_mongo_payment, 'a', newline='') as f1:
                t1 = csv.writer(f1)
                t1.writerow([order_id_2, item_id_2, sku, portal_id_2, venor_id_2, paybale_amount, product_id_2])

        db_name = "orders"
        conn = psycopg2.connect(host=rds_host, database=db_name, user=name, password=password)
        qry = "select item_id,sum(settlement_value) as sett from api_paymentdetailscsv where item_id is not null and portal = 'amazon'  group by item_id"
        cur = conn.cursor()
        cur.execute(qry)
        result = cur.fetchall()
        cur.close()
        conn.close()
        for r1 in result:
            item_id3 = str(r1[0])
            payable_amount2 = str(r1[1])

            with open(Amazon_portal_payment, 'a', newline='') as f1:
                t1 = csv.writer(f1)
                t1.writerow([item_id3, payable_amount2])
        print("portal payment file generated")

        df1 = pd.read_csv("/tmp/Amazon_mongo_payment.csv", encoding='unicode_escape')
        df2 = pd.read_csv("/tmp/Amazon_portal_payment.csv", encoding='unicode_escape')
        df3 = pd.merge(left=df1, right=df2, left_on="item id", right_on="item id")
        df3.to_csv(r'/tmp/Amazon_mongo_portal.csv', index=False)

        print('mongo_portal file generated')

        with open('/tmp/Amazon_mongo_portal.csv', 'r', newline='')as payment_data:
            payment_data1 = list(csv.reader(payment_data))

        for i in range(1, len(payment_data1)):

            order_id_id = str((payment_data1[i][0]))
            item_id_id = str((payment_data1[i][1]))
            bsku = str((payment_data1[i][2]))
            vendor_id_id = str((payment_data1[i][4]))
            # selling_price_price = str((payment_data1[i][6]))
            vendor_amount_amount = float((payment_data1[i][5]))
            portal_payment_payment = float((payment_data1[i][7]))
            produc_id_id_3 = payment_data1[i][6]

            portal_id_id = str((payment_data1[i][3]))
            # selling_price_price = str((payment_data1[i][3]))
            if portal_id_id == '1':
                portal_id_id = 'Amazon'

            differcence = portal_payment_payment - vendor_amount_amount

            with open(Amazon_process_file, 'a', newline='') as f1:
                t1 = csv.writer(f1)
                t1.writerow([order_id_id, item_id_id, bsku, portal_id_id, vendor_id_id, vendor_amount_amount,
                             portal_payment_payment, differcence, produc_id_id_3])
        print('process file generated')
        with open(Amazon_process_file, 'r', newline='')as OD:
            OD1 = list(csv.reader(OD))

        venid = "(-1"
        for d in range(1, len(OD1)):
            vendor_id_id1 = OD1[d][4]
            if str(vendor_id_id1) not in venid:
                venid += "," + str(vendor_id_id1)
        venid += ")"

        db_name = "vendors"
        conn = psycopg2.connect(host=rds_host, database=db_name, user=name, password=password)
        qry = "select id,vendor_name,vendor_type from api_newvendordetails where id in" + venid
        cur = conn.cursor()
        cur.execute(qry)
        result2 = cur.fetchall()
        cur.close()
        conn.close()
        for r2 in result2:
            vendor_id_id2 = str(r2[0])
            vendor_name = str(r2[1])
            vendor_type = str(r2[2])
            if vendor_type == "1":
                vendor_type = "Non SOR order"
            if vendor_type == "2":
                vendor_type = "SOR order"

            with open(Amazon_vendor_details, 'a', newline='') as f1:
                t1 = csv.writer(f1)
                t1.writerow([vendor_id_id2, vendor_name, vendor_type])
        print('vendor details file generated')

        db_name = "orders"
        conn = psycopg2.connect(host=rds_host, database=db_name, user=name, password=password)
        qry = "select order_item_id,sum(selling_price) as sp,sum(mrp) as mrp,sum(qty) as qty from api_neworder where order_item_id is not null and portal_id = 1 group by order_item_id"
        cur = conn.cursor()
        cur.execute(qry)
        result3 = cur.fetchall()
        cur.close()
        conn.close()

        # print(result3)

        for r3 in result3:
            oditem_1 = '#' + str(r3[0])
            sum_sp = str(r3[1])
            sum_mrp = str(r3[2])
            sum_qty = str(r3[3])

            with open(Amazon_portal_details, 'a', newline='') as f1:
                t1 = csv.writer(f1)
                t1.writerow(
                    [oditem_1, sum_sp, sum_mrp, sum_qty])

        with open(order_detail_order_date, 'w', newline='')as RD7:
            RD_6 = csv.writer(RD7)
            RD_6.writerow(['item id', 'order date'])
        print('portal details file generated')

        db_name = "orders"
        conn = psycopg2.connect(host=rds_host, database=db_name, user=name, password=password)
        qry = "select distinct(order_item_id),order_date from api_neworder where portal_id = 1"
        cur = conn.cursor()
        cur.execute(qry)
        result3 = cur.fetchall()
        cur.close()
        conn.close()

        # print(result3)

        for r3 in result3:
            oditem_1 = '#' + str(r3[0])
            order_date = str(r3[1])

            with open(order_detail_order_date, 'a', newline='') as f1:
                t1 = csv.writer(f1)
                t1.writerow(
                    [oditem_1, order_date])
        print('portal details with date file generated')

        df4 = pd.read_csv(Amazon_process_file, encoding='unicode_escape')
        df5 = pd.read_csv(Amazon_vendor_details, encoding='unicode_escape')
        df6 = pd.merge(left=df4, right=df5, left_on="vendor id", right_on="vendor id")
        df6.to_csv(r'/tmp/Amazon_P&L_file.csv', index=False)

        print('P&L file generated')

        with open('/tmp/Amazon_P&L_file.csv', 'r', newline='')as OD_1:
            OD_2 = list(csv.reader(OD_1))

        df7 = pd.read_csv('/tmp/Amazon_P&L_file.csv', encoding='unicode_escape')
        df8 = pd.read_csv(Amazon_portal_details, encoding='unicode_escape')
        df9 = pd.merge(left=df7, right=df8, left_on='item id', right_on='item id')
        df9.to_csv(r'/tmp/Amazon_P&L_file_final_data.csv', index=False)

        df15 = pd.read_csv(r'/tmp/Amazon_P&L_file_final_data.csv', encoding='unicode_escape')
        df16 = pd.read_csv(Amazon_reimbursment_file, encoding='unicode_escape')
        df17 = pd.merge(left=df15, right=df16, how='left', left_on='item id', right_on='item id')
        df17.to_csv(r'/tmp/Amazon_P&L_file_final_data_reim.csv', index=False)

        df18 = pd.read_csv('/tmp/Amazon_P&L_file_final_data_reim.csv')
        df19 = df18.fillna(0)
        df19.to_csv(r'/tmp/Amazon_P&L_file_final_data_reim_final.csv', index=False)

        db_name = "products"
        conn = psycopg2.connect(host=rds_host, database=db_name, user=name, password=password)
        qry = "select distinct(product_id),amazon_upload_selling_price from amazon_amazonproducts"
        cur = conn.cursor()
        cur.execute(qry)
        result_usp = cur.fetchall()
        cur.close()
        conn.close()

        filepath = '/tmp/Amazon_uploadsellingprice.csv'  # add the file path here
        header = ['product id', 'upload selling price']
        with open(filepath, 'w', newline='') as f:
            wr = csv.writer(f)
            wr.writerow(header)
            wr.writerows(result_usp)

        db_name = "products"
        conn = psycopg2.connect(host=rds_host, database=db_name, user=name, password=password)
        qry = "select distinct(product_id),product_mrp from master_masterproduct"
        cur = conn.cursor()
        cur.execute(qry)
        result_mrp = cur.fetchall()
        cur.close()
        conn.close()

        filepath = '/tmp/Amazon_mrpprice.csv'  # add the file path here
        header = ['product id', 'mastr_product_mrp']
        with open(filepath, 'w', newline='') as f:
            wr = csv.writer(f)
            wr.writerow(header)
            wr.writerows(result_mrp)

        db_name = "products"
        conn = psycopg2.connect(host=rds_host, database=db_name, user=name, password=password)
        qry = "select distinct(product_id),billing_price from purchase_invoice_sorbilling"
        cur = conn.cursor()
        cur.execute(qry)
        result_sor = cur.fetchall()
        cur.close()
        conn.close()

        filepath = '/tmp/Amazon_sorbilling.csv'  # add the file path here
        header = ['product id', 'sor billing']
        with open(filepath, 'w', newline='') as f:
            wr = csv.writer(f)
            wr.writerow(header)
            wr.writerows(result_sor)

        df19 = pd.read_csv(r'/tmp/Amazon_mrpprice.csv', encoding='unicode_escape')
        df20 = pd.read_csv('/tmp/Amazon_uploadsellingprice.csv', encoding='unicode_escape')
        df21 = pd.merge(left=df19, right=df20, how='left', left_on='product id', right_on='product id')
        df21.to_csv(r'/tmp/Amazon_mrp_upload.csv', index=False)

        df22 = pd.read_csv(r'/tmp/Amazon_mrp_upload.csv', encoding='unicode_escape')
        df23 = pd.read_csv('/tmp/Amazon_sorbilling.csv', encoding='unicode_escape')
        df24 = pd.merge(left=df22, right=df23, how='left', left_on='product id', right_on='product id')
        df24.to_csv(r'/tmp/Amazon_mrp_upload_sor.csv', index=False)

        df27 = pd.read_csv(r'/tmp/Amazon_P&L_file_final_data_reim_final.csv', encoding='unicode_escape')
        df28 = pd.read_csv('/tmp/Amazon_mrp_upload.csv', encoding='unicode_escape')
        df29 = pd.merge(left=df27, right=df28, how='left', left_on='product id', right_on='product id')
        df29.to_csv(r'/tmp/P&L_file_final_data_reim_final_amazon.csv', index=False)

        df30 = pd.read_csv(r'/tmp/P&L_file_final_data_reim_final_amazon.csv', encoding='unicode_escape')
        df31 = pd.read_csv(order_detail_order_date, encoding='unicode_escape')
        df32 = pd.merge(left=df30, right=df31, how='left', left_on='item id', right_on='item id')
        df32.to_csv(r'/tmp/P&L_file_final_data_reim_final_amazon_1.csv', index=False)

        df_2 = pd.read_csv('/tmp/P&L_file_final_data_reim_final_amazon_1.csv')
        df_filtered = df_2[~df_2['vendor type'].str.contains('3')]
        df_filtered.to_csv('/tmp/P&L_file_final_data_reim_final_amazon_1.csv', index=False)

        amazon_p_l = '/tmp/Amazon_p & L file .csv'
        with open(amazon_p_l, 'w', newline='')as RD1:
            RD = csv.writer(RD1)
            RD.writerow(
                ['order id', 'item id', 'sku', 'portal name', 'vendor id', 'vendor payment amount', 'portal payment',
                 'difference', 'product id', 'vendor_name', 'vendor type', 'selling price', 'mrp', 'qty',
                 'reimbursment amount', 'mastr_product_mrp', 'upload selling price', 'order date'])

        with open('/tmp/P&L_file_final_data_reim_final_amazon_1.csv', 'r', newline='', encoding="utf8")as f1:
            D1 = list(csv.reader(f1))
            # print(D1)

        for i in range(1, len(D1)):

            order_id = str(D1[i][0])
            item_id = str(D1[i][1])
            sku = str(D1[i][2])
            portal_name = str(D1[i][3])
            vendor_id = str(D1[i][4])
            vendor_payment_amount = int(float(str(D1[i][5])))
            portal_payment = int(float(str(D1[i][6])))
            difference = int(float(D1[i][7]))
            product_id = str(D1[i][8])
            vendor_name = str(D1[i][9])
            vendor_type = str(D1[i][10])
            selling_price = str(D1[i][11])
            mrp = str(D1[i][12])
            qty = int(str(D1[i][13]))
            reimbursment_amount = float(D1[i][14])
            try:
                mastr_product_mrp = int(float(D1[i][15]))
            except:
                mastr_product_mrp = 0
            try:
                upload_selling_price = float(D1[i][16])
            except:
                upload_selling_price = 0
            order_date = str(D1[i][17])
            mastr_product_mrp_1 = mastr_product_mrp * qty
            upload_selling_price_1 = upload_selling_price * qty

            with open(amazon_p_l, 'a', newline='') as f1:
                t1 = csv.writer(f1)
                t1.writerow(
                    [order_id, item_id, sku, portal_name, vendor_id, vendor_payment_amount, portal_payment, difference,
                     product_id, vendor_name, vendor_type, selling_price, mrp, qty, reimbursment_amount,
                     mastr_product_mrp_1, upload_selling_price_1, order_date])
        print('writing to dropbox')
        dbx = dropbox.Dropbox(access_token)
        file_from = amazon_p_l
        file_to = '/buymore2/PaymentDifference/amazon_payment_difference.csv'
        with open(file_from, 'rb') as f:
            data = dbx.files_upload(f.read(), file_to, mode=dropbox.files.WriteMode.overwrite)
        print('writing done')
        return Response({"statusCode": "200", "body": "SUCCESS"})

class flipkartPaymentDifference(APIView):
    def post(self, request):
        mongo_payment = "/tmp/mongo_payment.csv"
        portal_payment = "/tmp/portal_payment.csv"
        process_file = "/tmp/process_file.csv"
        vendor_details = "/tmp/vendor_details.csv"
        order_with_difference = "/tmp/order with difference.csv"
        p_and_l_file = "/tmp/P&L_file_final.csv"
        portal_details = '/tmp/portal_details.csv'
        reimbursment_file = '/tmp/reimbursment.csv'
        mongo_payment_1 = "/tmp/mongo_payment_1.csv"
        order_detail_order_date = "/tmp/order_date_details.csv"
        smart_data = "/tmp/smart_data.csv"
        with open(mongo_payment_1, 'w', newline='')as RD1:
            RD =csv.writer(RD1)
            RD.writerow(['order id','item id','sku','portal id','vendor_id','vendorpayable amount','product id'])

        with open(portal_payment, 'w', newline='')as RD2:
            RD_1 =csv.writer(RD2)
            RD_1.writerow(['item id','portal payment'])

        with open(process_file, 'w', newline='')as RD3:
            RD_2 =csv.writer(RD3)
            RD_2.writerow(['order id','item id','sku','portal name','vendor id','vendor payment amount','portal payment','difference','product id'])

        with open(vendor_details, 'w', newline='')as RD4:
            RD_3 =csv.writer(RD4)
            RD_3.writerow(['vendor id','vendor_name','vendor type'])

        with open(p_and_l_file, 'w', newline='')as RD6:
            RD_5 =csv.writer(RD6)
            RD_5.writerow(['order id','portal name','vendor payment amount','portal payment','difference','endor_id',	'vendor_name','vendor type'])
        with open(portal_details, 'w', newline='')as RD6:
            RD_5 =csv.writer(RD6)
            RD_5.writerow(['item id','selling price','mrp','qty'])

        with open(mongo_payment, 'w', newline='')as RD8:
            RD_7 =csv.writer(RD8)
            RD_7.writerow(['order id','item id','sku','portal id','vendor_id','vendorpayable amount','product id'])


        with open(reimbursment_file, 'w', newline='')as RD7:
            RD_6 =csv.writer(RD7)
            RD_6.writerow(['item id','reimbursment amount'])

        try:
            client = pymongo.MongoClient("mongodb+srv://Counfreedise:buymore123@cluster0-tq9zt.mongodb.net/buymore")
            print("connected")
        except Exception as e:
            print(e)

        db = client.wms

        cursor = db.buymore_two_payments.aggregate([{'$match': {'portal_id': 2}},{'$addFields': {'payable_amount': {'$toDouble': '$payable_amount'}}},{ '$group': { '_id': {"order_id":"$order_id","order_item_id":"$order_item_id","portal_sku":"$portal_sku","vendor_id":"$vendor_id","portal_id":"$portal_id","product_id":"$product_id"}, 'total': { '$sum': "$payable_amount" } }}])
        for item in cursor:
            print(item)
            try:
                order_id1 = item['_id']['order_id']
                item_id = item['_id']['order_item_id']
                sku = item['_id']['portal_sku']
                vendor_id = item['_id']['vendor_id']
                portal_id = item['_id']['portal_id']
                #selling_price = item['_id']['selling_price']
                payable_amount1 = item['total']
                product_id = item['_id']['product_id']
                #print(order_id1,sku,vendor_id,payable_amount1)
            except Exception as E:
                print(E)



            #print(order_id1,payable_amount1)

            with open(mongo_payment_1, 'a', newline='') as f1:
                t1 = csv.writer(f1)
                t1.writerow([order_id1,item_id,sku,portal_id,vendor_id,payable_amount1,product_id])
        print("mongo file generated")

        try:
            client = pymongo.MongoClient("mongodb+srv://Counfreedise:buymore123@cluster0-tq9zt.mongodb.net/buymore")
            print("connected")
        except Exception as e:
            print(e)

        db = client.wms

        result = client['wms']['buymore_two_payments'].aggregate([
            {'$match':
                 {'portal_id': 2}},
           {
               '$match': {
                   'reimbursement_list': {
                       '$nin': [
                           [], None
                       ]
                   }
               }
           }, {
               '$unwind': {
                   'path': '$reimbursement_list'
               }
           }, {
               '$addFields': {
                   'reimbursement_amount': {
                       '$toDouble': '$reimbursement_list.reimbursement_amount'
                   }
               }
           }, {
               '$group': {
                   '_id': {
                       'order_item_id': '$order_item_id'
                   },
                   'reimbusermentamount': {
                       '$sum': '$reimbursement_amount'
                   }
               }
           }
        ])

        for item2 in result:
            #print(item2)

            item_id2 = item2['_id']['order_item_id']

            reimbursment_amount = float(item2['reimbusermentamount'])

            print(item_id2,reimbursment_amount)

            with open(reimbursment_file, 'a', newline='') as f1:
                t1 = csv.writer(f1)
                t1.writerow([item_id2,reimbursment_amount])

        df10 = pd.read_csv(mongo_payment_1, encoding='unicode_escape')
        df11 = pd.read_csv(reimbursment_file,encoding= 'unicode_escape')
        df12 = pd.merge(left=df10,right=df11,how='left',left_on='item id',right_on='item id')
        df12.to_csv(r'/tmp/mongo_reimbursment.csv',index= False)

        df13 = pd.read_csv('/tmp/mongo_reimbursment.csv')
        df14 = df13.fillna(0)
        df14.to_csv(r'/tmp/mongo_reimbursment_final.csv',index= False)

        with open('/tmp/mongo_reimbursment_final.csv','r' , newline='')as OD_2:
            OD_3 = list(csv.reader(OD_2))
        #
        for d3 in range (1, len(OD_3)):
            order_id_2 = OD_3[d3][0]
            item_id_2 = OD_3[d3][1]
            sku = OD_3[d3][2]
            portal_id_2 = OD_3[d3][3]
            venor_id_2 = OD_3[d3][4]
            # selling_price_2 = OD_3[d3][6]
            vendor_payable_amount_2 =float(OD_3[d3][5])
            reim_amount_2 = float(OD_3[d3][7])
            product_id_2 = OD_3[d3][6]

            paybale_amount = vendor_payable_amount_2 + reim_amount_2

            with open(mongo_payment, 'a', newline='') as f1:
                t1 = csv.writer(f1)
                t1.writerow([order_id_2,item_id_2,sku,portal_id_2,venor_id_2,paybale_amount,product_id_2])

        db_name = "orders"
        conn = psycopg2.connect(host=rds_host, database=db_name, user=name, password=password)
        qry = "select item_id,sum(settlement_value) as sett from api_paymentdetailscsv where item_id is not null and portal = 'flipkart'  group by item_id"
        cur = conn.cursor()
        cur.execute(qry)
        result = cur.fetchall()
        cur.close()
        conn.close()
        for r1 in result:
            item_id3 = str(r1[0])
            payable_amount2 = str(r1[1])

            with open(portal_payment, 'a', newline='') as f1:
                t1 = csv.writer(f1)
                t1.writerow([item_id3,payable_amount2])
        print("portal payment file generated")

        df1 = pd.read_csv("/tmp/mongo_payment.csv",encoding= 'unicode_escape')
        df2 = pd.read_csv("/tmp/portal_payment.csv",encoding= 'unicode_escape')
        df3 = pd.merge(left=df1,right=df2,left_on="item id",right_on="item id")
        df3.to_csv(r'/tmp/mongo_portal.csv',index= False)

        print('mongo_portal file generated')

        with open('/tmp/mongo_portal.csv','r' , newline='')as payment_data:
            payment_data1 = list(csv.reader(payment_data))

        for i in range(1,len(payment_data1)):

            order_id_id = str((payment_data1[i][0]))
            item_id_id = str((payment_data1[i][1]))
            bsku = str((payment_data1[i][2]))
            vendor_id_id = str((payment_data1[i][4]))
            # selling_price_price = str((payment_data1[i][6]))
            vendor_amount_amount = float((payment_data1[i][5]))
            portal_payment_payment = float((payment_data1[i][7]))
            produc_id_id_3 = payment_data1[i][6]

            portal_id_id = str((payment_data1[i][3]))
            #selling_price_price = str((payment_data1[i][3]))
            if portal_id_id == '2':
                portal_id_id = 'Flipkart'

            differcence = portal_payment_payment - vendor_amount_amount



            with open(process_file, 'a', newline='') as f1:
                t1 = csv.writer(f1)
                t1.writerow([order_id_id,item_id_id,bsku,portal_id_id,vendor_id_id,vendor_amount_amount,portal_payment_payment,differcence,produc_id_id_3])
        print('process file generated')
        with open(process_file, 'r' , newline='')as OD:
            OD1 = list(csv.reader(OD))

        venid= "(-1"
        for d in range (1,len(OD1)):
            vendor_id_id1 = OD1[d][4]
            if str(vendor_id_id1) not in venid:
                venid+= "," + str(vendor_id_id1)
        venid+= ")"

        db_name = "vendors"
        conn = psycopg2.connect(host=rds_host, database=db_name, user=name, password=password)
        qry = "select id,vendor_name,vendor_type from api_newvendordetails where id in" + venid
        cur = conn.cursor()
        cur.execute(qry)
        result2 = cur.fetchall()
        cur.close()
        conn.close()
        for r2 in result2:
            vendor_id_id2 = str(r2[0])
            vendor_name = str(r2[1])
            vendor_type = str(r2[2])
            if vendor_type == "1":
                vendor_type = "Non SOR order"
            if vendor_type == "2":
                vendor_type = "SOR order"

            with open(vendor_details, 'a', newline='') as f1:
                t1 = csv.writer(f1)
                t1.writerow([vendor_id_id2,vendor_name,vendor_type])
        print('vendor details file generated')


        db_name = "orders"
        conn = psycopg2.connect(host=rds_host, database=db_name, user=name, password=password)
        qry = "select order_item_id,sum(selling_price) as sp,sum(mrp) as mrp,sum(qty) as qty from api_neworder where order_item_id is not null and portal_id = 2 group by order_item_id"
        cur = conn.cursor()
        cur.execute(qry)
        result3 = cur.fetchall()
        cur.close()
        conn.close()

        #print(result3)

        for r3 in result3:
            oditem_1 = '#'+str(r3[0])
            sum_sp = str(r3[1])
            sum_mrp = str(r3[2])
            sum_qty = str(r3[3])



            with open(portal_details, 'a', newline='') as f1:
                t1 = csv.writer(f1)
                t1.writerow(
                [oditem_1,sum_sp,sum_mrp,sum_qty])
        print('portal details file generated')

        with open(order_detail_order_date, 'w', newline='')as RD7:
            RD_6 =csv.writer(RD7)
            RD_6.writerow(['item id','order date'])
        print('portal details file generated')
        db_name = "orders"
        conn = psycopg2.connect(host=rds_host, database=db_name, user=name, password=password)
        qry = "select distinct(order_item_id),order_date from api_neworder where portal_id = 2"
        cur = conn.cursor()
        cur.execute(qry)
        result3 = cur.fetchall()
        cur.close()
        conn.close()

        #print(result3)

        for r3 in result3:
            oditem_1 = '#'+str(r3[0])
            order_date = str(r3[1])




            with open(order_detail_order_date, 'a', newline='') as f1:
                t1 = csv.writer(f1)
                t1.writerow(
                [oditem_1,order_date])
        print('portal details with date file generated')


        df4 = pd.read_csv(process_file, encoding='unicode_escape')
        df5 = pd.read_csv(vendor_details,encoding= 'unicode_escape')
        df6 = pd.merge(left=df4,right=df5,left_on="vendor id",right_on="vendor id")
        df6.to_csv(r'/tmp/P&L_file.csv',index= False)

        print('P&L file generated')

        with open('/tmp/P&L_file.csv','r' , newline='')as OD_1:
            OD_2 = list(csv.reader(OD_1))






        df7 = pd.read_csv('/tmp/P&L_file.csv', encoding='unicode_escape')
        df8 = pd.read_csv(portal_details,encoding= 'unicode_escape')
        df9 = pd.merge(left=df7,right=df8,left_on='item id',right_on='item id')
        df9.to_csv(r'/tmp/P&L_file_final_data.csv',index= False)

        df15 = pd.read_csv(r'/tmp/P&L_file_final_data.csv', encoding='unicode_escape')
        df16 = pd.read_csv(reimbursment_file,encoding= 'unicode_escape')
        df17 = pd.merge(left=df15,right=df16,how='left',left_on='item id',right_on='item id')
        df17.to_csv(r'/tmp/P&L_file_final_data_reim.csv',index= False)

        df18 = pd.read_csv('/tmp/P&L_file_final_data_reim.csv')
        df19 = df18.fillna(0)
        df19.to_csv(r'/tmp/P&L_file_final_data_reim_final.csv',index= False)

        db_name = "products"
        conn = psycopg2.connect(host=rds_host, database=db_name, user=name, password=password)
        qry = "select distinct(product_id),flipkart_upload_selling_price from flipkart_flipkartproducts"
        cur = conn.cursor()
        cur.execute(qry)
        result_usp = cur.fetchall()
        cur.close()
        conn.close()

        filepath = '/tmp/uploadsellingprice.csv'  # add the file path here
        header = ['product id','upload selling price']
        with open(filepath, 'w', newline='') as f:
            wr = csv.writer(f)
            wr.writerow(header)
            wr.writerows(result_usp)

        db_name = "products"
        conn = psycopg2.connect(host=rds_host, database=db_name, user=name, password=password)
        qry = "select distinct(product_id),product_mrp from master_masterproduct"
        cur = conn.cursor()
        cur.execute(qry)
        result_mrp = cur.fetchall()
        cur.close()
        conn.close()

        filepath = '/tmp/mrpprice.csv'  # add the file path here
        header = ['product id','mastr_product_mrp']
        with open(filepath, 'w', newline='') as f:
            wr = csv.writer(f)
            wr.writerow(header)
            wr.writerows(result_mrp)

        db_name = "products"
        conn = psycopg2.connect(host=rds_host, database=db_name, user=name, password=password)
        qry = "select distinct(product_id),billing_price from purchase_invoice_sorbilling"
        cur = conn.cursor()
        cur.execute(qry)
        result_sor = cur.fetchall()
        cur.close()
        conn.close()

        filepath = '/tmp/sorbilling.csv'  # add the file path here
        header = ['product id','sor billing']
        with open(filepath, 'w', newline='') as f:
            wr = csv.writer(f)
            wr.writerow(header)
            wr.writerows(result_sor)



        df19 = pd.read_csv(r'/tmp/mrpprice.csv', encoding='unicode_escape')
        df20 = pd.read_csv('/tmp/uploadsellingprice.csv',encoding= 'unicode_escape')
        df21 = pd.merge(left=df19,right=df20,how='left',left_on='product id',right_on='product id')
        df21.to_csv(r'/tmp/mrp_upload.csv',index= False)

        df22 = pd.read_csv(r'/tmp/mrp_upload.csv', encoding='unicode_escape')
        df23 = pd.read_csv('/tmp/sorbilling.csv',encoding= 'unicode_escape')
        df24 = pd.merge(left=df22,right=df23,how='left',left_on='product id',right_on='product id')
        df24.to_csv(r'/tmp/mrp_upload_sor.csv',index= False)


        df27 = pd.read_csv(r'/tmp/P&L_file_final_data_reim_final.csv', encoding='unicode_escape')
        df28 = pd.read_csv('/tmp/mrp_upload.csv',encoding= 'unicode_escape')
        df29 = pd.merge(left=df27,right=df28,how='left' ,left_on='product id',right_on='product id')
        df29.to_csv(r'/tmp/P&L_file_final_data_reim_final_flipkart.csv',index= False)

        df30 = pd.read_csv(r'/tmp/P&L_file_final_data_reim_final_flipkart.csv', encoding='unicode_escape')
        df31 = pd.read_csv(order_detail_order_date,encoding= 'unicode_escape')
        df32 = pd.merge(left=df30,right=df31,how='left' ,left_on='item id',right_on='item id')
        df32.to_csv(r'/tmp/P&L_file_final_data_reim_final_flipkart_1.csv',index= False)



        df_2 = pd.read_csv('/tmp/P&L_file_final_data_reim_final_flipkart_1.csv')
        df_filtered = df_2[~df_2['vendor type'].str.contains('3')]
        df_filtered.to_csv('/tmp/P&L_file_final_data_reim_final_flipkart_1.csv', index=False)

        flipkart_p_l = '/tmp/Flipkart_p & L file .csv'
        with open(flipkart_p_l, 'w', newline='')as RD1:
            RD =csv.writer(RD1)
            RD.writerow(['order id',	'item id',	'sku',	'portal name',	'vendor id',	'vendor payment amount',	'portal payment',	'difference',	'product id',	'vendor_name',	'vendor type',	'selling price',	'mrp',	'qty',	'reimbursment amount',	'mastr_product_mrp',	'upload selling price','order date'])


        with open('/tmp/P&L_file_final_data_reim_final_flipkart_1.csv','r' , newline='',encoding= "utf8")as f1:
            D1 = list(csv.reader(f1))
            #print(D1)

        for i in range(1,len(D1)):


            order_id = str(D1[i][0])
            item_id = str(D1[i][1])
            sku = str(D1[i][2])
            portal_name = str(D1[i][3])
            vendor_id = str(D1[i][4])
            vendor_payment_amount = int(float(str(D1[i][5])))
            portal_payment = int(float(str(D1[i][6])))
            difference = int(float(D1[i][7]))
            product_id = str(D1[i][8])
            vendor_name = str(D1[i][9])
            vendor_type = str(D1[i][10])
            selling_price = str(D1[i][11])
            mrp = str(D1[i][12])
            qty = int(str(D1[i][13]))
            reimbursment_amount = float(D1[i][14])
            try:
                mastr_product_mrp = int(float(D1[i][15]))
            except:
                mastr_product_mrp = 0
            try:
                upload_selling_price = float(D1[i][16])
            except:
                upload_selling_price = 0
            order_date = str(D1[i][17])
            # order_type = str(D1[i][18])
            mastr_product_mrp_1 = mastr_product_mrp * qty
            upload_selling_price_1 = upload_selling_price * qty



            with open(flipkart_p_l, 'a', newline='') as f1:
                t1 = csv.writer(f1)
                t1.writerow([order_id,	item_id,	sku,	portal_name,	vendor_id,	vendor_payment_amount,	portal_payment,	difference,	product_id,	vendor_name,	vendor_type,	selling_price,	mrp,	qty,	reimbursment_amount,	mastr_product_mrp_1,	upload_selling_price_1,order_date])
        print('writing to dropbox')
        dbx = dropbox.Dropbox(access_token)
        file_from =flipkart_p_l
        file_to = '/buymore2/PaymentDifference/flipkart_payment_difference.csv'
        with open(file_from, 'rb') as f:
            data = dbx.files_upload(f.read(), file_to, mode=dropbox.files.WriteMode.overwrite)
        print('writing done')
        return Response({"statusCode": "200", "body": "SUCCESS"})


