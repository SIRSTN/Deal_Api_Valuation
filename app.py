from flask import Flask, request, jsonify
import pymongo
import requests
import datetime

app = Flask(__name__)

def get_bitcoin_price_at_date(date):
    url = f"https://api.coingecko.com/api/v3/coins/bitcoin/history?date={date}&localization=false"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return data["market_data"]["current_price"]["usd"]
    except requests.RequestException as e:
        return None
    
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

def create_deal(db, deal_uid, deal_keyword, bav_date, bav_volume, bav_price, bav_amount, version_seq):
    deals_collection = db["Deals"]
    new_deal = {
        "DealUID": deal_uid,
        "Keyword": deal_keyword,
        "Date": bav_date,
        "Volume": bav_volume,
        "Price": bav_price,
        "Amount": bav_amount,
        "VersionSEQ": version_seq,
        "InactiveFlag": "N"
    }
    deals_collection.insert_one(new_deal)

def get_last_transaction(db, deal_uid, type):
    transactions_collection = db["Transactions"]
    last_sell_transaction = transactions_collection.find_one(
        {"DealUID": deal_uid, "Type": type, "InactiveFlag": "N"},
        sort=[("Date", pymongo.DESCENDING)]
    )
    return last_sell_transaction

def update_last_transaction(db, deal_uid, type):
    last_sell_transaction = get_last_transaction(db, deal_uid, type)
    if last_sell_transaction:
        transactions_collection = db["Transactions"]
        transaction_id = last_sell_transaction.get("_id")
        transactions_collection.update_one(
            {"_id": transaction_id},
            {"$set": {"InactiveFlag": "Y"}}
        )
        return True
    else:
        return False

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
    valuations_collection = db["Valuations"]
    latest_valuation = valuations_collection.find_one(
        {"DealUID": deal_uid},
        sort=[("Date", pymongo.DESCENDING)]
    )
    return latest_valuation

def create_valuation(db, deal_uid, bav_date, bav_volume, bav_price, bav_amount):
    valuations_collection = db["Valuations"]
    valuation = {
        "DealUID": deal_uid,
        "Date": bav_date,
        "Volume": bav_volume,
        "Price": bav_price,
        "Amount": bav_amount
    }
    valuations_collection.insert_one(valuation)

def is_greater(bav_amount, last_buy_amount):
    threshold = 1.05
    return bav_amount >= last_buy_amount * threshold

@app.route('/Valuate_Deals', methods=['POST'])
def valuate_deals():
    content = request.json
    valuation_date = content.get('date')
    keyword = content.get('keyword')

    if not valuation_date or not keyword:
        return jsonify({"error": "Date is required"}), 400

    try:
        client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = client["Deal_Transactions"]
        
        valuation_date_formatted = datetime.datetime.strptime(valuation_date, "%d-%m-%Y").strftime("%d-%m-%Y")
        bav_price = 27000#get_bitcoin_price_at_date(valuation_date_formatted)
        if bav_price is not None:

            deals = get_deals(db, keyword, "N")
            for deal in deals:
                deal_uid = deal.get("DealUID")
                deal_versionSEQ = deal.get("VersionSEQ")
                deal_Volume = deal.get("Volume") 
                deal_Amount = deal.get("Amount") 
                bav_date = datetime.datetime.strptime(valuation_date, "%d-%m-%Y")

                last_valuation = get_last_valuation(db, deal_uid)
                if last_valuation and last_valuation.get("Date") < bav_date:
                    bav_volume = deal_Volume
                    bav_amount = bav_volume * bav_price

                    if is_greater(bav_amount, deal_Amount):
                        sell_amount = bav_amount - deal_Amount
                        sell_volume = sell_amount / bav_price
                        bav_volume = bav_volume - sell_volume
                        bav_amount = bav_volume * bav_price
                        deal_price = deal_Amount / bav_volume
                        create_transaction(db, deal_uid, "Sell", bav_date, sell_volume, bav_price, sell_amount)
                        update_deal(db, deal_uid, deal_versionSEQ, "Y")
                        create_deal(db, deal_uid, keyword, bav_date, bav_volume, deal_price, deal_Amount, deal_versionSEQ + 1)
                    else:
                        last_sell_transaction = get_last_transaction(db, deal_uid, "Sell")
                        if last_sell_transaction:
                            last_sell_Amount = last_sell_transaction.get("Amount")
                            if (deal_Amount - bav_amount) > last_sell_Amount:
                                buy_amount = last_sell_Amount
                                buy_volume = buy_amount / bav_price
                                bav_volume = bav_volume + buy_volume
                                bav_amount = bav_volume * bav_price
                                deal_price = deal_Amount / bav_volume
                                update_last_transaction(db, deal_uid, "Sell")
                                create_transaction(db, deal_uid, "Buy", bav_date, buy_volume, bav_price, buy_amount)
                                update_deal(db, deal_uid, deal_versionSEQ, "Y")
                                create_deal(db, deal_uid, keyword, bav_date, bav_volume, deal_price, deal_Amount, deal_versionSEQ + 1)
                    
                    create_valuation(db, deal_uid, bav_date, bav_volume, bav_price, bav_amount)

            client.close()
            return jsonify({"message": "Updated documents and inserted transactions successfully"}), 200
        else:
            return jsonify({"error": "Failed to fetch the Bitcoin price for the specified date"}), 500
    except ValueError:
        return jsonify({"error": "Invalid date format. Please use DD-MM-YYYY."}), 400

if __name__ == '__main__':
    app.run(debug=True)