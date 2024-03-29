from flask import Flask, request, jsonify
from pymongo import MongoClient
import datetime
from configparser import ConfigParser

app = Flask(__name__)

# Load configuration file
config = ConfigParser()
config.read('config.ini')

# Setup MongoDB Client
#client = MongoClient(config.get('DEAL_API_VALUATION', 'MongoClient'))
client = MongoClient("mongodb://localhost:27017/")
db = client['Deal_Transactions']
    
def get_deals(db, keyword, inactive_flag):
    deals_collection = db["Deals"]
    deals = deals_collection.find({"Keyword": keyword, "InactiveFlag": inactive_flag})
    return list(deals)

def update_deal(db, deal_uid, version_seq, new_inactive_flag):
    deals_collection = db["Deals"]
    deals_collection.update_one(
        {"DealUID": deal_uid, "VersionSEQ": version_seq},
        {"$set": {"InactiveFlag": new_inactive_flag}}
    )

def create_deal(db, deal_uid, deal_keyword, bav_date, bav_volume, bav_price, bav_amount, factor, version_seq):
    deals_collection = db["Deals"]
    new_deal = {
        "DealUID": deal_uid,
        "Keyword": deal_keyword,
        "Date": bav_date,
        "Volume": bav_volume,
        "Price": bav_price,
        "Amount": bav_amount,
        "Factor": factor,
        "VersionSEQ": version_seq,
        "InactiveFlag": "N"
    }
    deals_collection.insert_one(new_deal)

def get_sell_transactions(db, deal_uid):
    transactions_collection = db["Transactions"]
    query = {"DealUID": deal_uid, "Type": "Sell", "InactiveFlag": "N"}
    sell_transactions = transactions_collection.find(query)
    
    sold_volume = 0
    sold_amount = 0
    for transaction in sell_transactions:
        sold_volume += transaction.get("Volume", 0)
        sold_amount += transaction.get("Amount", 0)
    return sold_volume, sold_amount

def update_sell_transactions(db, deal_uid):
    transactions_collection = db["Transactions"]
    result = transactions_collection.update_many(
        {"DealUID": deal_uid, "Type": "Sell", "InactiveFlag": "N"},
        {"$set": {"InactiveFlag": "Y"}}
    )
    return True

def create_transaction(db, deal_uid, transaction_type, date, volume, price, amount):
    transactions_collection = db["Transactions"]
    transaction = {
        "DealUID": deal_uid,
        "Type": transaction_type,
        "Date": date,
        "Volume": volume,
        "Price": price,
        "Amount": amount,
        "InactiveFlag": "N",
    }
    transactions_collection.insert_one(transaction)

def get_last_valuation(db, deal_uid):
    from pymongo import DESCENDING
    valuations_collection = db["Valuations"]
    latest_valuation = valuations_collection.find_one(
        {"DealUID": deal_uid},
        sort=[("Date", DESCENDING)]
    )
    return latest_valuation

def create_valuation(db, deal_uid, deal_keyword, bav_date, bav_volume, bav_price, bav_amount, init_volume, init_price, sold_volume, sold_amount):
    valuations_collection = db["Valuations"]
    valuation = {
        "DealUID": deal_uid,
        "Keyword": deal_keyword,
        "Date": bav_date,
        "Volume": bav_volume,
        "Price": bav_price,
        "Amount": bav_amount,
        "Init_Volume": init_volume,
        "Init_Price": init_price,
        "Sold_Volume": sold_volume,
        "Sold_Amount": sold_amount
    }
    valuations_collection.insert_one(valuation)

@app.route('/Valuate_Deals', methods=['POST'])
def valuate_deals():
    content = request.json
    keyword = content.get('keyword')
    date = content.get('date')
    bav_date = datetime.datetime.strptime(date, "%d-%m-%Y")
    bav_price = content.get('price')

    if not keyword or not bav_date or not bav_price:
        return jsonify({"error": "Parameters are required"}), 400

    try:
        deals = get_deals(db, keyword, "N")
        for deal in deals:
            deal_uid = deal.get("DealUID")
            deal_versionSEQ = deal.get("VersionSEQ")
            deal_Volume = deal.get("Volume") 
            deal_Amount = deal.get("Amount")
            deal_factor = deal.get("Factor")
            factor = 1 + float(deal_factor)
 
            last_valuation = get_last_valuation(db, deal_uid)
            init_volume = last_valuation.get("Init_Volume")
            init_price = last_valuation.get("Init_Price")
            if last_valuation.get("Date") < bav_date:
                
                bav_volume = deal_Volume
                bav_amount = bav_volume * bav_price
                sold_volume, sold_amount = get_sell_transactions(db, deal_uid)

                if bav_amount >= deal_Amount * factor:
                    sell_amount = bav_amount - deal_Amount
                    sell_volume = sell_amount / bav_price
                    bav_volume = bav_volume - sell_volume
                    bav_amount = bav_volume * bav_price
                    deal_price = deal_Amount / bav_volume
                    sold_volume = sold_volume + sell_volume
                    sold_amount = sold_amount + sell_amount
                    create_transaction(db, deal_uid, "Sell", bav_date, sell_volume, bav_price, sell_amount)
                    update_deal(db, deal_uid, deal_versionSEQ, "Y")
                    create_deal(db, deal_uid, keyword, bav_date, bav_volume, deal_price, deal_Amount, deal_factor, deal_versionSEQ + 1)
                else:
                    if bav_price <= init_price: 
                        if sold_amount > 0:
                            buy_amount = sold_amount
                            buy_volume = buy_amount / bav_price
                            bav_volume = bav_volume + buy_volume
                            bav_amount = bav_volume * bav_price
                            deal_Amount = ((buy_volume - sold_volume) * init_price) + deal_Amount
                            deal_price = deal_Amount / bav_volume
                            sold_volume = 0
                            sold_amount = 0
                            update_sell_transactions(db, deal_uid)
                            create_transaction(db, deal_uid, "Buy", bav_date, buy_volume, bav_price, buy_amount)
                            update_deal(db, deal_uid, deal_versionSEQ, "Y")
                            create_deal(db, deal_uid, keyword, bav_date, bav_volume, deal_price, deal_Amount, deal_factor, deal_versionSEQ + 1)
                
                create_valuation(db, deal_uid, keyword, bav_date, bav_volume, bav_price, bav_amount, init_volume, init_price, sold_volume, sold_amount)
        
        return jsonify({"message": "Updated documents and inserted transactions successfully"}), 200
    except ValueError:
        return jsonify({"error": "Invalid date format. Please use DD-MM-YYYY."}), 400

if __name__ == '__main__':
    app.run(debug=True)