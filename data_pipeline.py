import csv
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError

Base = declarative_base()

class PurchaseV2(Base): 
    __tablename__ = 'purchases_v2' 
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    customer_id = Column(String, nullable=False)
    age = Column(Integer, nullable=False)
    gender = Column(String, nullable=False)
    item_purchased = Column(String, nullable=False)
    purchase_amount = Column(Float, nullable=False)
    age_group = Column(String, nullable=False) 


# Connect to the database
engine = create_engine('sqlite:///shopping.db')
Base.metadata.create_all(engine)  

Session = sessionmaker(bind=engine)
session = Session()


def read_data_from_database():
    """
    Read rows from the 'purchases' table.
    Returns a list of dictionaries.
    """
    query = session.query(PurchaseV2)
    
    purchases = query.limit(5).all() 
     
    return [{
        'id': purchase.id,
        'customer_id': purchase.customer_id,
        'age': purchase.age,
        'gender': purchase.gender,
        'item_purchased': purchase.item_purchased,
        'purchase_amount': purchase.purchase_amount,
        'age_group': purchase.age_group  
    } for purchase in purchases]


def transform_data(row):
    """
    Perform transformations on a row of data.
    """
    row['Gender'] = row['Gender'].title()
    
    row['Item Purchased'] = row['Item Purchased'].upper()
    
    row['Purchase Amount (USD)'] = round(float(row['Purchase Amount (USD)']), 2)
    
    age = int(row['Age'])
    if 18 <= age <= 25:
        row['Age Group'] = 'Young'
    elif 26 <= age <= 40:
        row['Age Group'] = 'Adult'
    else:
        row['Age Group'] = 'Senior'
    
    return row


def read_csv():
    """
    Read data from the CSV file, perform transformations, and save to the database.
    """
    try:
        with open('shopping_trends.csv', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            
            for row in reader:
                transformed_row = transform_data(row)
                
                customer_id = transformed_row['Customer ID']
                age = int(transformed_row['Age'])
                gender = transformed_row['Gender']
                item_purchased = transformed_row['Item Purchased']
                purchase_amount = float(transformed_row['Purchase Amount (USD)'])
                age_group = transformed_row['Age Group']
                
            
                purchase = PurchaseV2(
                    customer_id=customer_id,
                    age=age,
                    gender=gender,
                    item_purchased=item_purchased,
                    purchase_amount=purchase_amount,
                    age_group=age_group 
                )
                
                session.add(purchase)
            
            session.commit()
    except OperationalError as e:
        print(f"Database error: {e}")
        session.rollback() 
    except Exception as e:
        print(f"An error occurred: {e}")


read_csv()

data = read_data_from_database()
for row in data:
    print(row)