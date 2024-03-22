from fastapi import FastAPI
from typing import Optional
from pydantic import BaseModel
import requests
import os
import sqlite3
from random import randint
import json
import datetime
from fastapi_utils.cbv import cbv
from fastapi_utils.inferring_router import InferringRouter
import uvicorn

class Item(BaseModel):
    name: str
    description: Optional[str] = None
    price: float
    tax: Optional[float] = None

class UserID(BaseModel):
    userID: str

class ProductName(BaseModel):
    productName: str
    sku: str

class UniqueSale(BaseModel):
    productName: str
    sku: str
    price: int
    user: str
    dateDay: str
    dateMonth: str
    dateYear: str

class CheckUser(BaseModel):
    userName: str

class CheckSKU(BaseModel):
    sku: str



app = FastAPI()
router = InferringRouter()

# class DatabaseCreate():

#     def __init__(self):
#         pass

#     def dbcreate(self):
        
#         print('Creating Database Now')
#         conn = sqlite3.connect('laced.db')
#         c = conn.cursor()

#         create_table_users = """CREATE TABLE users (
#                 rowid integer PRIMARY KEY,
#                 userID text,
#                 sales integer
#         )"""

#         create_table_productNames = """CREATE TABLE productNames (
#                     productName text,
#                     SKU text,
#                     sales integer
#         )"""

#         create_table_uniqueSales = """CREATE TABLE uniqueSales (
#                     productName text,
#                     sku text,
#                     lacedID integer,
#                     price integer,
#                     dateSold text,
#                     userName text,
#                     userID integer,
#                     FOREIGN KEY (userID)
#                     REFERENCES users (rowid)
#         )"""

#         c.execute(create_table_users)
#         c.execute(create_table_productNames)
#         c.execute(create_table_uniqueSales)

#         conn.commit()
#         conn.close()

@cbv(router)
class Leaderboard:
        
    def dbcreate(self):
        
        print('Creating Database Now')
        conn = sqlite3.connect('laced.db')
        c = conn.cursor()

        create_table_users = """CREATE TABLE users (
                rowid integer PRIMARY KEY,
                userID text,
                sales integer
        )"""

        create_table_productNames = """CREATE TABLE productNames (
                    productName text,
                    SKU text,
                    sales integer
        )"""

        create_table_uniqueSales = """CREATE TABLE uniqueSales (
                    productName text,
                    sku text,
                    price integer,
                    dateSold text,
                    userName text,
                    userID integer,
                    FOREIGN KEY (userID)
                    REFERENCES users (rowid)
        )"""

        c.execute(create_table_users)
        c.execute(create_table_productNames)
        c.execute(create_table_uniqueSales)

        conn.commit()
        conn.close()

    def dateNow(self):

        # Get current date

        dateNow = datetime.datetime.now()

        day = dateNow.strftime("%d")
        month = dateNow.strftime("%m")
        year = dateNow.strftime("%Y")

        dateToday = year + '-' + month + '-' + day

        return dateToday

    def date30Delta(self):

        # Create datetime object 30 days in past

        dateDelta30 = datetime.datetime.now() - datetime.timedelta(30)

        delta30day = dateDelta30.strftime("%d")
        delta30month = dateDelta30.strftime("%m")
        delta30year = dateDelta30.strftime("%Y")

        date30DaysAgo = delta30year + '-' + delta30month + '-' + delta30day

        return date30DaysAgo
    
    def highestSeller(self, sqlResult):
        
        print(sqlResult)
        userSales = {}
        highestSaleQty = 0
        highestSaleUser = ''

        for i in range(0, len(sqlResult)):
            userName = sqlResult[i][5]
            print(userName)

            if userName in userSales:
                
                print('True')
                sales = userSales[f'{userName}']['periodSales']
                sales += 1
                userSales[f'{userName}']['periodSales'] = sales
            
            else:

                userSales[f'{userName}'] = {}
                userSales[f'{userName}']['periodSales'] = 1
        
        print(userSales)
        for user in userSales:
            if userSales[f'{user}']['periodSales'] >= highestSaleQty:
                highestSaleQty = userSales[f'{user}']['periodSales']
                highestSaleUser = user
            
            else:
                print('User did not have higher sales than current top seller')
        
        return highestSaleQty, highestSaleUser

    @router.post("/usersale/")
    async def receive_user(self, user: UserID):
        user = user.userID

        if os.path.exists('laced.db'):
            pass
        else:
            self.dbcreate()

        conn = sqlite3.connect('laced.db')

        c = conn.cursor()

        # UserSale Update #
        # Searching if user exists already
        userSearch = c.execute("""SELECT EXISTS(SELECT 1 FROM users WHERE userID=? LIMIT 1)""", (user, )).fetchone()[0]

        if userSearch != 0:
            print('User exists')

            # Finding primary key of the user
            primaryKeyUsers = c.execute("SELECT rowid FROM users WHERE userID=?", (user, )).fetchone()[0]

            # Finding sales quantity to be updated
            usersoldQty = c.execute("SELECT sales FROM users WHERE rowid=?", (primaryKeyUsers, )).fetchone()[0]
            usersoldQty += 1

            # Updating sales quantity
            c.execute("UPDATE users SET sales=? WHERE rowid=?", (usersoldQty, primaryKeyUsers, ))

            conn.commit()
            conn.close()

            return 'User Score Updated' + ' / ' + f'Sales to Date: {usersoldQty}'

        else:
            print(f'New userID Added - {user}')

            # Add user and setting sales qty as 1 
            c.execute("INSERT INTO users (rowid, userID, sales) VALUES (?, ?, ?)", (1, user, 1))

            conn.commit()
            conn.close()

            return 'User Added'

    @router.post("/productsale/")
    async def add_productName(self, product: ProductName):
        productName = product.productName
        productSKU = product.sku

        if os.path.exists('laced.db'):
            pass
        else:
            self.dbcreate()

        conn = sqlite3.connect('laced.db')
        c = conn.cursor()

        # Searching if product exists
        productNameSearch = c.execute("""SELECT EXISTS(SELECT 1 FROM productNames WHERE productName=? LIMIT 1)""", (productName, )).fetchone()[0]

        if productNameSearch != 0:
            print('Product Name exists')

            # Finding primary key of the product name
            primaryKeyProductName = c.execute("SELECT rowid FROM productNames WHERE productName=?", (productName, )).fetchone()[0]

            # Finding sales quantity to be updated
            productNamesoldQty = c.execute("SELECT sales FROM productNames WHERE rowid=?", (primaryKeyProductName, )).fetchone()[0]
            productNamesoldQty += 1

            # Updating sales quantity
            c.execute("UPDATE productNames SET sales=? WHERE rowid=?", (productNamesoldQty, primaryKeyProductName, ))

            conn.commit()
            conn.close()

            return f'The total sales of {productName} / {productSKU} is now {productNamesoldQty}'

        else:
            print('New Product Name Added')

            # Add product
            c.execute("INSERT INTO productNames (productName, SKU, sales) VALUES (?, ?, ?)", (productName, productSKU, 1))

            conn.commit()
            conn.close()

            return f"You're the first seller of {productName}!"

    @router.post("/newsale/")
    async def add_uniqueSale(self, uniqueSale: UniqueSale):
        productName = uniqueSale.productName
        sku = uniqueSale.sku
        price = uniqueSale.price
        user = uniqueSale.user
        day = uniqueSale.dateDay
        month = uniqueSale.dateMonth
        year = uniqueSale.dateYear

        if os.path.exists('laced.db'):
            pass
        else:
            self.dbcreate()

        # Formatting date
        dateObj = year + '-' + month + '-' + day

        # Making connection to database

        ### UPDATING USER SALES TABLE ### 

        conn = sqlite3.connect('laced.db')
        c = conn.cursor()

        # Searching if user exists already
        userSearch = c.execute("""SELECT EXISTS(SELECT 1 FROM users WHERE userID=? LIMIT 1)""", (user, )).fetchone()[0]

        if userSearch != 0:
            print('User exists')

            # Finding primary key of the user
            primaryKeyUsers = c.execute("SELECT rowid FROM users WHERE userID=?", (user, )).fetchone()[0]

            # Finding sales quantity to be updated
            usersoldQty = c.execute("SELECT sales FROM users WHERE rowid=?", (primaryKeyUsers, )).fetchone()[0]
            usersoldQty += 1

            # Updating sales quantity
            c.execute("UPDATE users SET sales=? WHERE rowid=?", (usersoldQty, primaryKeyUsers, ))

            conn.commit()
            conn.close()

        else:
            print(f'New userID Added - {user}')

            # Add user and setting sales qty as 1 
            c.execute("INSERT INTO users (rowid, userID, sales) VALUES (?, ?, ?)", (1, user, 1))

            conn.commit()
            conn.close()



        ### UPDATING PRODUCTS TABLE ### 
            
        conn = sqlite3.connect('laced.db')
        c = conn.cursor()

        # Searching if product exists
        productNameSearch = c.execute("""SELECT EXISTS(SELECT 1 FROM productNames WHERE productName=? LIMIT 1)""", (productName, )).fetchone()[0]

        if productNameSearch != 0:
            print('Product Name exists')

            # Finding primary key of the product name
            primaryKeyProductName = c.execute("SELECT rowid FROM productNames WHERE productName=?", (productName, )).fetchone()[0]

            # Finding sales quantity to be updated
            productNamesoldQty = c.execute("SELECT sales FROM productNames WHERE rowid=?", (primaryKeyProductName, )).fetchone()[0]
            productNamesoldQty += 1

            # Updating sales quantity
            c.execute("UPDATE productNames SET sales=? WHERE rowid=?", (productNamesoldQty, primaryKeyProductName, ))

            conn.commit()
            conn.close()

        else:
            print('New Product Name Added')

            # Add product
            c.execute("INSERT INTO productNames (productName, SKU, sales) VALUES (?, ?, ?)", (productName, sku, 1))

            conn.commit()
            conn.close()



        ### UPDATING UNIQUE SALES TABLE ###
            
        # Finding UserID PK to use as FK

        conn = sqlite3.connect('laced.db')
        c = conn.cursor()

        # Searching if user exists
        userSearch = c.execute("""SELECT EXISTS(SELECT 1 FROM users WHERE userID=? LIMIT 1)""", (user, )).fetchone()[0]

        if userSearch != 0:
            print('User exists')

            # Finding PK of user
            primarykeyUserID = c.execute("SELECT rowid FROM users WHERE userID=?", (user, )).fetchone()[0]
        
        else:
            print('Adding new user')

            # Add user and setting sales qty as 1
            c.execute("INSERT INTO users (userID, sales) VALUES (?, ?)", (user, 1))
            conn.commit()

            # Finding PK of user
            primarykeyUserID = c.execute("SELECT rowid FROM users WHERE userID=?", (user, )).fetchone()[0]

        conn.close()

        conn = sqlite3.connect('laced.db')
        c = conn.cursor()

        # Inserting data
        c.execute("INSERT INTO uniqueSales (productName, sku, price, dateSold, userName, userID) VALUES (?, ?, ?, ?, ?, ?)", (
            productName, sku, price, dateObj, user, primarykeyUserID))
        
        conn.commit()
        conn.close()

        return 'Sale uploaded successfully'
    
    @router.post("/checkuser/")
    async def checkUser(self, checkUser: CheckUser):
        userName = checkUser.userName

        if os.path.exists('laced.db'):
            pass
        else:
            self.dbcreate()

        # Finding row ID of user

        conn = sqlite3.connect('laced.db')
        c = conn.cursor()

        # Searching if user exists
        userSearch = c.execute("""SELECT EXISTS(SELECT 1 FROM users WHERE userID=? LIMIT 1)""", (userName, )).fetchone()[0]


        if userSearch != 0:

            # Finding rowid of user
            rowIDUser = c.execute("SELECT rowid FROM users WHERE userID=?", (userName, )).fetchone()[0]
            print(rowIDUser)
            # JOIN uniqueSales and user together for the userName

            c.execute("""   SELECT us.productName, us.sku, us.price, us.date
                    FROM 
                            uniqueSales AS us
                    INNER JOIN
                            users AS u
                    ON
                        us.userID = u.rowid WHERE u.rowid=?""", (rowIDUser, ))

            results = c.fetchall()

            # Create dictionary to store results
            userSales = {}

            for result in range(0, len(results)):
                userItem = results[result][0]
                userSKU = results[result][1]
                userPrice = results[result][2]
                userDate = results[result][3]

                userSales[f'{result}'] = {}
                userSales[f'{result}']['Item Name'] = userItem
                userSales[f'{result}']['SKU'] = userSKU
                userSales[f'{result}']['Sale Price'] = userPrice
                userSales[f'{result}']['Date of Sale'] = userDate

            conn.close()
            return userSales


            
        else:
            conn.close()
            return 'User does not exist'

    @router.post("/checksku")
    async def checkSales(self, checkSKU: CheckSKU):
        sku = checkSKU.sku

        if os.path.exists('laced.db'):
            pass
        else:
            self.dbcreate()

        conn = sqlite3.connect('laced.db')
        c = conn.cursor()

        results = c.execute("SELECT * FROM uniqueSales WHERE SKU=?", (sku, )).fetchall()

        return results
    
    @router.get("/topsellers")
    async def topsellers(self):

        dateNow = self.dateNow()
        date30Delta = self.date30Delta()

        if os.path.exists('laced.db'):
            pass
        else:
            self.dbcreate()

        conn = sqlite3.connect('laced.db')
        c = conn.cursor()

        # Finding all results sold within time periods

        resultsTodayResults = c.execute("SELECT * FROM uniqueSales WHERE dateSold = ?", (dateNow, )).fetchall()
        resultsDelta30Results = c.execute("SELECT * FROM uniqueSales WHERE dateSold >= ?", (date30Delta, )).fetchall()

        todayQty, todayUserName = self.highestSeller(resultsTodayResults)
        delta30Qty, delta30UserName = self.highestSeller(resultsDelta30Results)

        # Finding total number of results within time periods

        totalSalesToday = c.execute("SELECT COUNT(*) FROM uniqueSales WHERE dateSold = ?", (dateNow, )).fetchone()[0]
        totalSalesDelta30 = c.execute("SELECT COUNT(*) FROM uniqueSales WHERE dateSold >= ?", (date30Delta, )).fetchone()[0]

        topsellers = {}

        topsellers['Sales Today'] = totalSalesToday
        topsellers['Sales in 30 Days'] = totalSalesDelta30

        topsellers['Top Seller Today'] = {}
        topsellers['Top Seller Today']['Username'] = todayUserName
        topsellers['Top Seller Today']['Sales Qty'] = todayQty

        topsellers['Top Seller 30 Days'] = {}
        topsellers['Top Seller 30 Days']['Username'] = delta30UserName
        topsellers['Top Seller 30 Days']['Sales Qty'] = delta30Qty

        print(topsellers)
        return topsellers

    @router.get("/latestsales/")
    async def latestsales(self):

        if os.path.exists('laced.db'):
            pass
        else:
            self.dbcreate()

        # Establishing connection to database

        conn = sqlite3.connect('laced.db')
        c = conn.cursor()

        c.execute("SELECT rowid, * FROM uniqueSales")
        results = (c.fetchall())

        # Create dictionary to store results
        returnResults = {}

        for result in results:
            resultNumber = result[0]
            resultName = result[1]
            resultSku = result[2]
            resultPrice = result[4]
            resultDate = result[5]
            resultUser = result[6]

            returnResults[f"{resultNumber}"] = {}
            returnResults[f"{resultNumber}"]['Product Name'] = resultName
            returnResults[f"{resultNumber}"]['Product SKU'] = resultSku
            returnResults[f"{resultNumber}"]['Sold Price'] = resultPrice
            returnResults[f"{resultNumber}"]['Date'] = resultDate
            returnResults[f"{resultNumber}"]['User'] = resultUser
        
        return returnResults

app.include_router(router)


if __name__ == '__main__':

    run = Leaderboard()
