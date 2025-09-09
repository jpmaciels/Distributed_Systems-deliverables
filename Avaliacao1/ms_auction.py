"""
MS Leilão (publisher)
• (0,1) Mantém internamente uma lista pré-configurada (hardcoded)
de leilões com: ID do leilão, descrição, data e hora de início e fim,
status (ativo, encerrado).
• (0,1) O leilão de um determinado produto deve ser iniciado quando
o tempo definido para esse leilão for atingido. Quando um leilão
começa, ele publica o evento na fila: leilao_iniciado.
• (0,1) O leilão de um determinado produto deve ser finalizado
quando o tempo definido para esse leilão expirar. Quando um leilão
termina, ele publica o evento na fila: leilao_finalizado.
"""

import pika
import json
import time
from datetime import datetime, timedelta
import threading
from loguru import logger

auctions = [
    {
        "id": "leilao1",
        "description": "chocovo",
        "start_time": datetime.now() + timedelta(seconds=10),
        "end_time": datetime.now() + timedelta(seconds=90),
        "status": "active"
    },
    {
        "id": "leilao2",
        "description": "chocopizza nachos",
        "start_time": datetime.now() + timedelta(seconds=25),
        "end_time": datetime.now() + timedelta(seconds=130),
        "status": "active"
    }
]

def connect_to_rabbitmq():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    return connection, channel

def publish_auction_start(auction):
    connection, channel = connect_to_rabbitmq()
    event = {
        "id": auction["id"],
        "description": auction["description"],
        "start_time": auction["start_time"].isoformat(),
        "end_time": auction["end_time"].isoformat(),
        "status": auction["status"]
    }
    channel.basic_publish(
        exchange='auction_fanout_exchange',
        routing_key='',
        body=json.dumps(event)
    )
    logger.info(f" [x] Published start event for auction {auction['id']} - {auction['description']}")
    connection.close()

def publish_auction_end(auction):
    connection, channel = connect_to_rabbitmq()
    event = {
        "id": auction["id"],
        "description": auction["description"],
        "end_time": auction["end_time"].isoformat(),
        "status": "finished"
    }
    channel.basic_publish(
        exchange='direct_exchange',
        routing_key='auction_ended',
        body=json.dumps(event)
    )
    logger.info(f" [x] Published end event for auction {auction['id']} - {auction['description']}")
    connection.close()

def schedule_auction_events():    
    current_time = datetime.now()
    for auction in auctions:
        start_delay = (auction["start_time"] - current_time).total_seconds()
        if start_delay > 0:
            threading.Timer(start_delay, publish_auction_start, args=(auction,)).start()
        else:
            publish_auction_start(auction)
            
        end_delay = (auction["end_time"] - current_time).total_seconds()
        if end_delay > 0:
            threading.Timer(end_delay, publish_auction_end, args=(auction,)).start()
        else:
            publish_auction_end(auction)

def main():
    schedule_auction_events()
    while True:
        time.sleep(1)

if __name__ == "__main__":
    main()