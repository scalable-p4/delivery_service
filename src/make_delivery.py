import os
import sys
from dotenv import load_dotenv
from celery import Celery
from sqlalchemy import insert, select
from sqlalchemy import create_engine, insert
from sqlalchemy.orm import sessionmaker
from src.database import token_delivery

BROKER_URL = os.getenv("CELERY_BROKER_URL")
RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND")
celery_app = Celery('update_inventory', broker=BROKER_URL,
                    backend=RESULT_BACKEND)
DATABASE_URL_DELIVERY = os.getenv("DATABASE_URL_DELIVERY")
engine = create_engine(DATABASE_URL_DELIVERY)
Session = sessionmaker(bind=engine)

@celery_app.task(name="make_delivery")
def make_delivery(payload: dict, fn: str):
    print("fn="+str(fn))
    print("payload="+str(payload))
    username: str = payload.get("username")
    quantity: int = payload.get("quantity")
    delivery: bool = payload.get("delivery")
    print("username="+str(username))
    print("quantity="+str(quantity))
    print("delivery="+str(delivery))
    print("db_url="+str(DATABASE_URL_DELIVERY))

    if fn == "make_delivery" and delivery == True:
        session = Session()
        try:
            query = insert(token_delivery).values(
                username=username, quantity=quantity, delivery=delivery
            )
            session.execute(query)
            session.commit()
            print("delivery made")
            celery_app.send_task("create_order",queue="q01",args=[payload,"success_token_transaction"])
            return "SUCCESS"
        except Exception as e:
            print("error making delivery")
            print(e)
            session.rollback()
        finally:
            session.close()
    else:
        print("invalid Function in make delivery")
        print("transaction failed in delivery...sending rollback to inventory")
        celery_app.send_task("update_inventory",queue="q03",args=[payload,"rollback_inventory"])

    
